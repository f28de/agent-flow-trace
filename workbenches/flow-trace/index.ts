import type {
  WorkbenchPackage,
  WorkbenchRequest,
  WorkbenchResponse,
} from "@prismshadow/penguin-server/plugin";

/**
 * 智能体流转轨迹工作台的服务端模块。
 *
 * 订阅本 Project 内被关注会话的 AMSP 记录流，把任务（task_begin/task_end）、提问（text）、
 * 子智能体派发（run_subagent 与 origin/subagent 指针）和产物写入（write_file/edit_file
 * 及其结果）折叠成结构化事件，持久化在工作台自己的 state 文档里；页面经 /flow 增量轮询渲染，
 * 经 /export 导出 JSON / JSONL / Markdown 报告。
 *
 * 实现注意：状态启动时拷贝进内存，之后全部在内存里读写，用合并窗口批量落盘
 * （宿主 setState 是整文件原子重写，逐条写会造成 O(n²) 写放大）；
 * 订阅按 25 分钟保鲜、10 分钟无投递自动重订（服务端闲置 30 分钟会回收订阅）；
 * 历史回填（/ingest）来自客户端页面，做形状校验 + origin 白名单 + 每会话节流。
 *
 * 输入规模是这里的主要攻击面：所有来自页面的输入都设了上限（/watch 单次条数、
 * 标识字段长度、arguments 原文长度），关注上限淘汰用「一次排序 + 一次批量删除」
 * 而不是逐个循环删除，避免单请求就把整把串行锁堵住数十秒。
 */

/** 一条流转事件。 */
type FlowEvent = {
  /** 递增序号。 */
  seq: number;
  /** ISO 8601 UTC 时间戳。 */
  ts: string;
  kind:
    | "task_begin"
    | "task_end"
    | "prompt"
    | "delegate"
    | "link"
    | "artifact"
    | "artifact_result";
  /** 产生事件的会话（子会话的记录归到子会话 id）。 */
  sessionId: string;
  /** 会话所属智能体（来自页面提交并经服务端校验的关注清单）。 */
  agentId: string;
  tool?: string;
  /** tool_call_id，用于把产物与其结果配对。 */
  callId?: string;
  /** 产物路径（artifact / artifact_result）。 */
  path?: string;
  /** 工具结果状态（artifact_result）。 */
  status?: string;
  /** 文本摘要（提问、派发任务简述、任务终稿），已截断。 */
  text?: string;
  /** run_subagent 的目标智能体（delegate）。 */
  targetAgentId?: string;
  /** 子会话 id（link）。 */
  childSessionId?: string;
  childAgentId?: string;
};

/** 一条派发链接：父会话 → 直接子会话。seq 是写入时的事件序号，供增量拉取过滤。 */
type FlowLink = { from: string; to: string; ts: string; seq?: number };

/** 一条被关注的会话。 */
type WatchEntry = {
  sessionId: string;
  agentId: string;
  /** 首次关注时间（轮询刷新时不覆盖，仅作淘汰兜底）。 */
  since: string;
  /** 最近一次 /watch 刷新带来的活跃时间（随下次结构性变更落盘）。 */
  lastSeen?: string;
  /** 最近一次（重建）订阅的时刻；页面据此判断订阅重建后需要二次回填。 */
  subAt?: string;
  /** 会话最近活跃时间（页面从会话列表 API 带来）；关注上限淘汰时优先剔除最旧的。 */
  activity?: string;
};

type State = {
  watched: WatchEntry[];
  events: FlowEvent[];
  links: FlowLink[];
  seq: number;
  /** 已折叠记录的持久化指纹（FNV-1a），老状态没有该字段时按空处理。 */
  seen?: string[];
  /** 状态纪元：/clear 时轮换，客户端据此整体重拉（不依赖 seq 大小比较，防 seq 回绕误判）。 */
  epoch?: string;
  /** 未配对产物调用的持久化镜像，重启后仍能配对 tool_call_output。 */
  pending?: { callId: string; sessionId: string; agentId: string; path: string }[];
};

/** 事件缓冲上限（先进先出），避免状态文档无限增长。 */
const MAX_EVENTS = 4000;
/** 派发链接上限（先进先出），与事件缓冲同步淘汰，避免 links 无界增长。 */
const MAX_LINKS = 4000;
/** 未配对产物调用（tool_call 等待 tool_call_output）的上限，超出淘汰最早的。 */
const PENDING_MAX = 512;
/** sanitizeRecord 裁剪超长字符串字段的长度上限（防折叠/指纹的 CPU 尖峰）。 */
const PAYLOAD_FIELD_MAX = 4096;
/** task_end 终稿的累积上限：超出后只保留尾部，避免长会话无限增长。 */
const ANSWER_MAX = 800;
/** 最多关注的会话数；超出时淘汰「不在当前请求中且活跃时间最旧」的项。 */
const MAX_WATCHED = 300;
/** 文本摘要长度上限。 */
const SNIPPET = 240;
/** task_end 携带的终稿摘要稍长一些。 */
const ANSWER_SNIPPET = 400;
/** 记录去重指纹上限（超出时 FIFO 淘汰一半，不整体清空以免双通道重复入库）。 */
const RECORD_KEY_MAX = 8000;
/**
 * 订阅保鲜时长。服务端对闲置 30 分钟无发布的订阅会主动结束（IDLE_RELEASE_MS），
 * 这里 25 分钟后先退订再重订，留 5 分钟余量，避免实时记录永久停摆。
 */
const SUB_MAX_AGE_MS = 25 * 60 * 1000;
/**
 * 订阅活性重订阈值：即使订阅未满 25 分钟，若超过 10 分钟没有任何记录投递，
 * 订阅也可能已被服务端以非闲置原因结束（会话删除、通道重建），下次 follow 时主动重订，
 * 把静默中断窗口从 25 分钟压到 10 分钟。
 */
const SUB_IDLE_RESUB_MS = 10 * 60 * 1000;
/** 内存变更合并落盘的窗口（2 秒：写密集时每秒最多整文件重写 1 次；读路由全走内存，不受影响）。 */
const SAVE_FLUSH_MS = 2000;
/** POST /ingest 单次接收的历史记录上限（超出时保留最新的若干条）。 */
const INGEST_MAX = 500;
/** POST /ingest 的每会话节流窗口：同一会话 60 秒内只接受一次回填。 */
const INGEST_MIN_INTERVAL_MS = 60 * 1000;
/** 刚被关注上限淘汰的会话在冷却期内不自动重新加入，避免每轮轮询反复进出造成抖动。 */
const EVICT_READD_MS = 10 * 60 * 1000;
/**
 * 单次 POST /watch 最多接受的会话条数。
 * 页面提交的是「本 Project 全部会话」，正常规模在数百条；取 1 万是为了只在病态输入时才生效
 * （关注上限只有 300，真实项目远达不到）。不设限时，超大数组会让写入与淘汰路径线性膨胀，
 * 把一次请求变成长时间阻塞——而所有路由共用一把串行锁。
 */
const MAX_WATCH_ITEMS = 10000;
/** 标识类字段（sessionId / agentId / callId）长度上限：防止超长 id 常驻内存并随每条事件复制。 */
const MAX_ID_LEN = 128;
/** 产物路径长度上限（仅展示用，超长无意义）。 */
const MAX_PATH_LEN = 512;
/**
 * tool_call.arguments 原文长度上限。arguments 是「待解析的 JSON 字符串」，
 * 按普通文本截断会破坏结构、丢掉 file_path（曾导致 >4KB 的产物在回填时静默消失），
 * 因此这里放宽到 512KB，超限时只保留折叠所需的关键字段而不是硬截断。
 */
const ARGS_MAX = 512 * 1024;
/**
 * 事件缓冲裁剪余量：满载后每追加一条就 splice(0,1) 会搬移整个数组，
 * 改为「超出上限 SLACK 条后一次性裁回」，把满载期的搬移次数降到 1/SLACK。
 */
const EVENTS_SLACK = 100;
/**
 * /ingest 允许的「未来时间戳」容差。历史记录由页面代取、时间戳来自服务器时钟，
 * 正常不会超前；超出容差即拒收，避免用 "9999-01-01" 绕过「旧记录复活」防护。
 */
const INGEST_FUTURE_SKEW_MS = 5 * 60 * 1000;

/** `host.subscribe` 的签名；老服务器没有它，读取时做防御（见官方 penguin-support 范例）。 */
type AmspRecord = {
  timestamp?: string;
  type?: string;
  payload: { type?: unknown; [key: string]: unknown };
  origin?: unknown;
};
type Subscribe = (
  sessionId: string,
  listener: (record: AmspRecord) => void,
) => () => void;

const nowIso = (): string => new Date().toISOString();

/**
 * FNV-1a 32 位哈希：把整段文本压成定长指纹，用于记录去重。
 * 注意这是概率性去重：32 位空间下 8000 条指纹的碰撞概率约 0.74%，
 * 一旦碰撞会有一条合法记录被静默丢弃（宁可漏记、不可重复记，可接受）。
 */
const fnv1a = (text: string): string => {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
};

/** 把任意值压成单行摘要并截断。 */
const snippet = (value: unknown, max = SNIPPET): string => {
  const text = typeof value === "string" ? value : value == null ? "" : String(value);
  const one = text.replace(/\s+/g, " ").trim();
  return one.length > max ? one.slice(0, max) + "…" : one;
};

/** tool_call 的 arguments 是 JSON 字符串；解析失败时按空对象处理。 */
const parseArgs = (raw: unknown): Record<string, unknown> => {
  if (typeof raw !== "string" || raw.trim() === "") return {};
  try {
    const parsed: unknown = JSON.parse(raw);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
};

const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);

/** 按长度硬截断（不压缩空白，用于路径等不能被改写的值）。 */
const cap = (value: string, max: number): string =>
  value.length > max ? value.slice(0, max) : value;

/**
 * 把时间字符串解析成毫秒数。用于比较与排序——直接比字符串在混入 ±hh:mm 偏移时是错的
 * （"2026-11-06T02:00:00Z" 实际比 "2026-11-06T09:00:00+08:00" 更新，字符串序却相反）。
 */
const tsOf = (value: unknown): number => {
  if (typeof value !== "string" || value === "") return NaN;
  const t = Date.parse(value);
  return Number.isFinite(t) ? t : NaN;
};

/** 日志里转义换行与 ANSI 转义序列：会话 id 来自客户端，直接拼接可伪造日志行。 */
const logSafe = (value: string): string => value.replace(/[\r\n\u001b]/g, " ");

/** 从 arguments 原文里兜底提取 file_path（JSON 解析失败时用）。 */
const FILE_PATH_RE = /"file_path"\s*:\s*"((?:[^"\\]|\\.){0,1024})"/;

/**
 * 取 write_file / edit_file 的 file_path。
 * 优先用已解析结果；解析失败（例如超大或结构损坏的 JSON）时退化为从原文正则提取，
 * 避免整条产物事件被静默丢弃。`parsed` 用于复用调用方已经解析过的对象，省一次解析。
 */
const pathOf = (raw: unknown, parsed?: Record<string, unknown>): string => {
  const direct = (parsed ?? parseArgs(raw)).file_path;
  if (typeof direct === "string" && direct) return direct;
  if (typeof raw !== "string" || raw === "") return "";
  const m = FILE_PATH_RE.exec(raw.slice(0, 32 * 1024));
  if (!m) return "";
  try {
    return String(JSON.parse(`"${m[1]}"`));
  } catch {
    return m[1];
  }
};

/** 超长 arguments 的瘦身：只保留折叠真正用到的字段，其余丢弃。 */
const shrinkArgs = (raw: string): string => {
  const args = parseArgs(raw);
  const out: Record<string, string> = {};
  const path = typeof args.file_path === "string" ? args.file_path : pathOf(raw);
  if (path) out.file_path = cap(path, MAX_PATH_LEN);
  if (typeof args.prompt === "string") out.prompt = snippet(args.prompt);
  if (typeof args.agent_id === "string") out.agent_id = cap(args.agent_id, MAX_ID_LEN);
  return JSON.stringify(out);
};

/** ISO 8601 时间戳（允许 Z 或 ±hh:mm / ±hhmm 偏移）。 */
const ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})$/;

/** 生成状态纪元：/clear 时轮换，客户端检测到变化即整体重拉。 */
const newEpoch = (): string =>
  Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 10);

const emptyState = (): State => ({
  watched: [],
  events: [],
  links: [],
  seq: 0,
  seen: [],
  epoch: newEpoch(),
});

const KINDS: readonly FlowEvent["kind"][] = [
  "task_begin",
  "task_end",
  "prompt",
  "delegate",
  "link",
  "artifact",
  "artifact_result",
];
const isKind = (v: unknown): v is FlowEvent["kind"] =>
  typeof v === "string" && (KINDS as readonly string[]).includes(v);

/**
 * 把宿主读回的任意值归一化成可用的 State。
 *
 * 旧实现只校验「三个字段是数组」，一旦 state.json 被外部改写或磁盘截断
 * （例如 watched 里混入 null），启动时的 `w.sessionId` 就会抛错 → create() 失败 →
 * 工作台整页打不开，而 /clear 也在同一模块里，用户无法自救。
 * 这里改为「逐元素校验 + 丢弃坏元素」，任何输入都能得到一个可用状态，最坏只是丢几条记录。
 */
const normalizeState = (raw: unknown): State => {
  if (!isPlainObject(raw)) return emptyState();
  const st = emptyState();

  if (typeof raw.seq === "number" && Number.isFinite(raw.seq) && raw.seq >= 0)
    st.seq = Math.floor(raw.seq);
  if (typeof raw.epoch === "string" && raw.epoch) st.epoch = raw.epoch;

  if (Array.isArray(raw.watched)) {
    // 同一 sessionId 只保留一条：坏状态里可能有重复项，而 idx / watchedMap 都是按 id 建索引的，
    // 重复项会让淘汰计数与展示对不上。
    const seenIds = new Set<string>();
    for (const w of raw.watched) {
      if (!isPlainObject(w)) continue;
      if (typeof w.sessionId !== "string" || !w.sessionId) continue;
      const sessionId = cap(w.sessionId, MAX_ID_LEN);
      if (seenIds.has(sessionId)) continue;
      seenIds.add(sessionId);
      st.watched.push({
        sessionId,
        agentId: typeof w.agentId === "string" ? cap(w.agentId, MAX_ID_LEN) : "",
        since: typeof w.since === "string" && w.since ? w.since : nowIso(),
        lastSeen: typeof w.lastSeen === "string" ? w.lastSeen : undefined,
        subAt: typeof w.subAt === "string" ? w.subAt : undefined,
        activity: typeof w.activity === "string" ? w.activity : undefined,
      });
    }
  }

  if (Array.isArray(raw.events)) {
    for (const e of raw.events) {
      if (!isPlainObject(e)) continue;
      if (typeof e.seq !== "number" || !Number.isFinite(e.seq)) continue;
      if (typeof e.ts !== "string" || !e.ts) continue;
      if (!isKind(e.kind)) continue;
      if (typeof e.sessionId !== "string") continue;
      st.events.push(e as FlowEvent);
    }
    // 事件必须按 seq 升序：/flow 的 minSeq 直接取 events[0].seq，乱序会让页面剪错本地视图。
    st.events.sort((a, b) => a.seq - b.seq);
    const last = st.events.length > 0 ? st.events[st.events.length - 1].seq : 0;
    if (last > st.seq) st.seq = last;
  }

  if (Array.isArray(raw.links)) {
    for (const l of raw.links) {
      if (!isPlainObject(l)) continue;
      if (typeof l.from !== "string" || typeof l.to !== "string") continue;
      st.links.push({
        from: l.from,
        to: l.to,
        ts: typeof l.ts === "string" ? l.ts : nowIso(),
        seq: typeof l.seq === "number" && Number.isFinite(l.seq) ? l.seq : undefined,
      });
    }
  }

  if (Array.isArray(raw.seen))
    st.seen = raw.seen.filter((k): k is string => typeof k === "string" && k !== "");

  if (Array.isArray(raw.pending)) {
    for (const p of raw.pending) {
      if (!isPlainObject(p)) continue;
      if (typeof p.callId !== "string" || !p.callId) continue;
      st.pending = st.pending ?? [];
      st.pending.push({
        callId: p.callId,
        sessionId: typeof p.sessionId === "string" ? p.sessionId : "",
        agentId: typeof p.agentId === "string" ? p.agentId : "",
        path: typeof p.path === "string" ? p.path : "",
      });
    }
  }

  return st;
};

export default {
  modules: {
    Workbench: {
      create({ use, effect }) {
        const host = use.host;

        // ---- 内存状态 + 批量落盘 -------------------------------------------
        // 启动时拷贝一次（host.getState 返回活引用）；之后全部在内存里改，
        // 用 SAVE_FLUSH_MS 合并窗口批量写回，避免每条记录都整文件重写。
        // normalizeState 是「总函数」：任何形状的旧状态都不会让它抛错（坏文档只会丢坏元素）。
        let state: State;
        try {
          state = normalizeState(host.getState());
        } catch (e) {
          host.log(`flow-trace: 状态读取失败，按空状态重建: ${logSafe(String(e))}`);
          state = emptyState();
        }
        if (!Array.isArray(state.seen)) state.seen = [];
        // 老状态没有纪元时补一个，保证 /flow 始终带 epoch。
        if (typeof state.epoch !== "string" || !state.epoch) state.epoch = newEpoch();
        /** 去重指纹的内存镜像，落盘时写回 state.seen。 */
        const seen = new Set<string>(state.seen);
        let dirty = false;
        let saveTimer: ReturnType<typeof setTimeout> | null = null;
        let saveChain: Promise<void> = Promise.resolve();
        /** 最近一次落盘错误，计入 /status 便于诊断。 */
        let lastSaveError = "";

        /** 把脏状态写回宿主（串行化，失败时保留 dirty 稍后重试并记录日志）。 */
        const flush = (): Promise<void> => {
          const run = async (): Promise<void> => {
            if (!dirty) return;
            dirty = false;
            // 单次 structuredClone 后把 seen / pending 镜像写进快照（省一次整文件拷贝）。
            const snap = structuredClone(state);
            snap.seen = [...seen];
            snap.pending = [...pendingArtifact.entries()].map(([callId, v]) => ({
              callId,
              sessionId: v.sessionId,
              agentId: v.agentId,
              path: v.path,
            }));
            try {
              await host.setState(snap);
              lastSaveError = "";
            } catch (e) {
              dirty = true;
              lastSaveError = String(e);
              host.log(`flow-trace: 状态落盘失败: ${String(e)}`);
            }
          };
          const p = saveChain.then(run, run);
          saveChain = p.catch(() => undefined);
          return p;
        };

        /** 标记脏并启动合并窗口；窗口内多次变更只落盘一次。 */
        const scheduleSave = (): void => {
          dirty = true;
          if (saveTimer !== null) return;
          saveTimer = setTimeout(() => {
            saveTimer = null;
            void flush().catch(() => undefined);
          }, SAVE_FLUSH_MS);
        };

        // 卸载 / 重载工作台时，把内存里尚未落盘的变更尽力写回。
        effect(() => {
          if (saveTimer !== null) clearTimeout(saveTimer);
          saveTimer = null;
          // 卸载时退订全部活订阅，避免泄漏已失效的 AMSP 订阅。
          // （effect 回调只在卸载时执行——与官方 service.ts 的用法一致——
          //   此时 following 已初始化，无 TDZ 问题。）
          for (const [, f] of following) f.stop();
          following.clear();
          void flush().catch(() => undefined);
        });

        // HTTP 请求串行化（与落盘解耦：读请求不排队等写盘）。
        let pending: Promise<unknown> = Promise.resolve();
        const locked = <T>(fn: () => Promise<T>): Promise<T> => {
          const result = pending.then(fn);
          pending = result.catch(() => undefined);
          return result;
        };

        // ---- 实时订阅 -------------------------------------------------------
        // 老服务器的 host 上没有 subscribe：那时只有页面轮询渲染，不做实时记录。
        const subscribe = (host as unknown as { subscribe?: unknown }).subscribe;
        const live =
          typeof subscribe === "function" ? (subscribe as Subscribe).bind(host) : null;

        /** 活订阅：sessionId → { 取消函数, 订阅时刻, 上次投递时刻 }。 */
        type FollowEntry = { stop: () => void; at: number; lastSeenAt: number };
        const following = new Map<string, FollowEntry>();
        /** 产物工具调用：tool_call_id → 产物信息，等 tool_call_output 来配对结果。 */
        const pendingArtifact = new Map<
          string,
          { sessionId: string; agentId: string; path: string }
        >();
        // 重启后把持久化的未配对产物调用加载回来，tool_call_output 仍能配对结果（P1-3）。
        for (const p of state.pending ?? []) {
          if (p && typeof p.callId === "string" && p.callId && !pendingArtifact.has(p.callId))
            pendingArtifact.set(p.callId, {
              sessionId: String(p.sessionId ?? ""),
              agentId: String(p.agentId ?? ""),
              path: String(p.path ?? ""),
            });
        }
        /** 每个会话当前任务累积的助手终稿（task_end 时写入事件）。 */
        const lastAnswer = new Map<string, string>();
        /** 已建立过链接的 父→子，避免重复记录。 */
        const seenLinks = new Set<string>();
        /** sessionId → agentId 缓存，避免每次查询都扫全量 watched。 */
        const watchedMap = new Map<string, string>();
        const rebuildWatchedMap = (): void => {
          watchedMap.clear();
          for (const w of state.watched) watchedMap.set(w.sessionId, w.agentId);
        };
        rebuildWatchedMap();
        const agentOf = (sessionId: string): string => watchedMap.get(sessionId) ?? "";
        /** 每会话上次 /ingest 的时刻（节流用）。 */
        const lastIngestAt = new Map<string, number>();
        /** 刚被关注上限淘汰的会话 → 淘汰时刻（冷却期内不自动重加）。 */
        const evictedAt = new Map<string, number>();

        // ---- 事件写入（同步改内存，经 scheduleSave 批量落盘） ------------------

        /** 追加一条事件（超长时丢弃最旧的）。 */
        const record = (ev: Omit<FlowEvent, "seq" | "ts">): void => {
          state.seq += 1;
          // 统一在这里做字段长度收敛：所有事件都经此入口，是唯一需要守的漏斗。
          // （sessionId 来自客户端，若不限长会随每条事件复制一份，可撑爆内存。）
          state.events.push({
            seq: state.seq,
            ts: nowIso(),
            kind: ev.kind,
            sessionId: cap(ev.sessionId, MAX_ID_LEN),
            agentId: cap(ev.agentId, MAX_ID_LEN),
            tool: ev.tool === undefined ? undefined : cap(ev.tool, 64),
            callId: ev.callId === undefined ? undefined : cap(ev.callId, MAX_ID_LEN),
            path: ev.path === undefined ? undefined : cap(ev.path, MAX_PATH_LEN),
            status: ev.status === undefined ? undefined : cap(ev.status, 64),
            text: ev.text === undefined ? undefined : cap(ev.text, ANSWER_SNIPPET),
            targetAgentId:
              ev.targetAgentId === undefined ? undefined : cap(ev.targetAgentId, MAX_ID_LEN),
            childSessionId:
              ev.childSessionId === undefined ? undefined : cap(ev.childSessionId, MAX_ID_LEN),
            childAgentId:
              ev.childAgentId === undefined ? undefined : cap(ev.childAgentId, MAX_ID_LEN),
          });
          // 批量裁剪：满载后每来一条就 splice(0,1) 会搬移整个数组，
          // 留 EVENTS_SLACK 条余量后一次性裁回，满载期搬移次数降到 1/SLACK。
          if (state.events.length > MAX_EVENTS + EVENTS_SLACK)
            state.events.splice(0, state.events.length - MAX_EVENTS);
          scheduleSave();
        };

        /** 记录一条 父会话→子会话 的派发链接（seenLinks 去重）。 */
        const link = (fromRaw: string, toRaw: string): void => {
          const from = cap(fromRaw, MAX_ID_LEN);
          const to = cap(toRaw, MAX_ID_LEN);
          if (!from || !to) return;
          const key = `${from}->${to}`;
          // seenLinks 是 links 的完整去重镜像（/clear 与 forget 都会同步清理），
          // 因此不必再对 state.links 做一次 O(n) 的 some 扫描。
          if (seenLinks.has(key)) return;
          seenLinks.add(key);
          const ts = nowIso();
          state.seq += 1;
          const seq = state.seq;
          state.links.push({ from, to, ts, seq });
          state.events.push({
            seq,
            ts,
            kind: "link",
            sessionId: from,
            agentId: agentOf(from),
            childSessionId: to,
            childAgentId: agentOf(to),
          });
          if (state.events.length > MAX_EVENTS + EVENTS_SLACK)
            state.events.splice(0, state.events.length - MAX_EVENTS);
          // 链接同样设上限 FIFO：淘汰最旧链接并同步删除其去重标记，避免 links 无界增长。
          if (state.links.length > MAX_LINKS) {
            const dropped = state.links.splice(0, state.links.length - MAX_LINKS);
            for (const l of dropped) seenLinks.delete(l.from + "->" + l.to);
          }
          scheduleSave();
        };

        // ---- 折叠 ------------------------------------------------------------

        /**
         * 取记录的标识性内容（去重指纹用）：text 取 role + 前 240 字，tool_call 取
         * name + tool_call_id（arguments 全量内容不参与指纹，避免对超长 JSON 做
         * 整体 stringify 造成的 CPU 尖峰），subagent 取 session_id，task_end 取终稿摘要
         * （否则同会话多次 task_end 在没有 timestamp 时会退化为同一指纹被误判为重复）。
         */
        const identOf = (
          pType: string,
          p: Record<string, unknown>,
          sessionId: string,
        ): string => {
          switch (pType) {
            case "text":
              return pType + "|" + String(p.role ?? "") + "|" + snippet(p.text, SNIPPET);
            case "tool_call":
              return pType + "|" + String(p.name ?? "") + "|" + String(p.tool_call_id ?? "");
            case "tool_call_output":
              return pType + "|" + String(p.tool_call_id ?? "") + "|" + snippet(p.status, SNIPPET);
            case "subagent":
              return pType + "|" + String(p.session_id ?? "");
            case "task_begin":
              return pType + "|" + snippet(p.task, SNIPPET);
            case "task_end":
              return pType + "|" + snippet(lastAnswer.get(sessionId) ?? "", 64);
            default:
              return pType;
          }
        };

        /** 关心的记录类型；其余类型在碰状态之前直接丢弃。 */
        const FOLD_TYPES = new Set([
          "task_begin",
          "task_end",
          "text",
          "tool_call",
          "tool_call_output",
          "subagent",
        ]);

        /**
         * 把一条 AMSP 记录折叠成零到多条流转事件。
         * 带 origin 的记录来自子会话：归到子会话（origin[0] 是直接子会话 id），
         * 并先建立 父→子 派发链接（link 内部有双重去重，重复调用无副作用），
         * 这样即使子会话自己的订阅先收到该记录、父 origin 副本被去重挡掉，边也不会丢。
         *
         * `trusted` 表示记录来源：实时订阅（host.subscribe）的记录由服务端产生，可信；
         * /ingest 的历史记录由页面代取后提交，属不可信输入——派发端点必须过关注集合白名单。
         * 不区分来源会让「回填」变成伪造派发边的入口，区分后实时路径不受影响。
         */
        const fold = (sessionId: string, rec: AmspRecord, trusted: boolean): void => {
          const p = rec.payload ?? {};
          const pType = typeof p.type === "string" ? p.type : "";
          const origin = Array.isArray(rec.origin) ? rec.origin : [];
          const child =
            origin.length > 0 && typeof origin[0] === "string" ? origin[0] : undefined;
          const eff = child ?? sessionId;

          // 先建链（P0-2 修复：必须在去重 return 之前）。
          if (child && child !== sessionId) link(sessionId, child);

          // 不关心的类型早退：不建指纹、不碰事件缓冲。
          if (!FOLD_TYPES.has(pType)) return;

          // FNV-1a 指纹去重（只哈希标识字段，见 identOf）：同一条记录可能经父会话的
          // origin 流和子会话自己的订阅各来一次。
          const key = fnv1a(
            (rec.timestamp ?? "") + "|" + eff + "|" + identOf(pType, p, eff),
          );
          if (seen.has(key)) return;
          seen.add(key);
          if (seen.size > RECORD_KEY_MAX) {
            // FIFO 淘汰一半（不整体清空，避免双通道记录重复入库）。
            const arr = [...seen];
            seen.clear();
            for (const k of arr.slice(arr.length >> 1)) seen.add(k);
          }

          switch (pType) {
            case "task_begin": {
              lastAnswer.delete(eff);
              record({ kind: "task_begin", sessionId: eff, agentId: agentOf(eff) });
              break;
            }
            case "text": {
              if (p.role === "user") {
                record({
                  kind: "prompt",
                  sessionId: eff,
                  agentId: agentOf(eff),
                  text: snippet(p.text),
                });
              } else if (p.role === "assistant") {
                const prev = lastAnswer.get(eff) ?? "";
                const next = prev + String(p.text ?? "");
                // 累积上限 ANSWER_MAX：超出只保留尾部，避免长会话无限增长。
                lastAnswer.set(
                  eff,
                  next.length > ANSWER_MAX ? next.slice(next.length - ANSWER_MAX) : next,
                );
              }
              break;
            }
            case "task_end": {
              const answer = lastAnswer.get(eff) ?? "";
              lastAnswer.delete(eff);
              record({
                kind: "task_end",
                sessionId: eff,
                agentId: agentOf(eff),
                text: snippet(answer, ANSWER_SNIPPET),
              });
              break;
            }
            case "tool_call": {
              const name = typeof p.name === "string" ? p.name : "";
              const args = parseArgs(p.arguments);
              const callId = typeof p.tool_call_id === "string" ? p.tool_call_id : "";
              const agentId = agentOf(eff);
              if (name === "run_subagent") {
                record({
                  kind: "delegate",
                  sessionId: eff,
                  agentId,
                  tool: name,
                  callId: callId || undefined,
                  text: snippet(args.prompt),
                  targetAgentId:
                    typeof args.agent_id === "string" && args.agent_id
                      ? args.agent_id
                      : agentId,
                });
              } else if (name === "write_file" || name === "edit_file") {
                // pathOf 在 JSON 解析失败时会退化为正则提取：即使 arguments 超大或被
                // 瘦身过，也不会把整条产物事件静默丢掉。
                const path = cap(pathOf(p.arguments, args), MAX_PATH_LEN);
                if (path) {
                  if (callId) {
                    pendingArtifact.set(callId, { sessionId: eff, agentId, path });
                    // 上限 PENDING_MAX：淘汰最早的未配对调用，避免无界增长。
                    if (pendingArtifact.size > PENDING_MAX) {
                      const oldest = pendingArtifact.keys().next();
                      if (!oldest.done) pendingArtifact.delete(oldest.value);
                    }
                  }
                  record({
                    kind: "artifact",
                    sessionId: eff,
                    agentId,
                    tool: name,
                    callId: callId || undefined,
                    path,
                  });
                }
              }
              break;
            }
            case "tool_call_output": {
              const callId = typeof p.tool_call_id === "string" ? p.tool_call_id : "";
              const pendingArt = pendingArtifact.get(callId);
              if (pendingArt) {
                pendingArtifact.delete(callId);
                record({
                  kind: "artifact_result",
                  sessionId: pendingArt.sessionId,
                  agentId: pendingArt.agentId,
                  callId: callId || undefined,
                  path: pendingArt.path,
                  status: typeof p.status === "string" ? p.status : undefined,
                });
              }
              break;
            }
            case "subagent": {
              // 父 Trace 里的子会话指针（落盘为主；流里一般没有，处理一下无妨）。
              if (child) break;
              const to = typeof p.session_id === "string" ? p.session_id : "";
              // 不可信来源（/ingest）的端点必须在已关注会话集合内，否则可凭空注入指向
              // 任意 id 的假派发边，再用 4000 条假边把真实派发关系挤出 MAX_LINKS 的 FIFO
              // 缓冲（定向抹除证据）。实时流的记录由服务端产生，不做此限制，
              // 否则「子会话尚未被关注」时会永久丢掉真实派发边。
              if (to && (trusted || watchedMap.has(to))) link(sessionId, to);
              break;
            }
            default:
              break;
          }
        };

        /**
         * 校验并清洗一条待入账的历史记录（/ingest 的信任边界——记录来自客户端页面，不可直接采信）：
         * 非对象、payload 不是对象、payload.type 不在已知集合、timestamp 不是 ISO 字符串，整条拒绝；
         * origin 只保留「本 Project 已关注」的会话 id，防止伪造指向不存在会话的派生边（幽灵节点）。
         */
        const sanitizeRecord = (rec: unknown): AmspRecord | null => {
          if (!isPlainObject(rec) || !isPlainObject(rec.payload)) return null;
          const p = rec.payload;
          if (typeof p.type !== "string" || !FOLD_TYPES.has(p.type)) return null;
          if (
            rec.timestamp !== undefined &&
            (typeof rec.timestamp !== "string" || !ISO_RE.test(rec.timestamp))
          )
            return null;
          const clean: AmspRecord = { payload: { ...p } };
          // 超长字符串字段裁到 PAYLOAD_FIELD_MAX，防折叠/指纹对超大载荷的 CPU 尖峰。
          // 例外：arguments 是「待解析的 JSON 字符串」，按文本截断会破坏结构，
          // 让 file_path 解析失败、整条产物事件被静默丢弃（>4KB 的写入会消失），
          // 因此跳过它，改由下面的 ARGS_MAX 分支做结构化瘦身。
          for (const k of Object.keys(clean.payload)) {
            if (k === "arguments") continue;
            const v = clean.payload[k];
            if (typeof v === "string" && v.length > PAYLOAD_FIELD_MAX)
              clean.payload[k] = v.slice(0, PAYLOAD_FIELD_MAX);
          }
          // arguments 超长时只保留折叠所需的关键字段（而不是硬截断），
          // 这样 write_file / run_subagent 仍能被正确折叠，同时不让内存随载荷线性膨胀。
          const rawArgs = clean.payload.arguments;
          if (typeof rawArgs === "string" && rawArgs.length > ARGS_MAX)
            clean.payload.arguments = shrinkArgs(rawArgs);
          if (typeof rec.timestamp === "string") clean.timestamp = rec.timestamp;
          if (rec.origin !== undefined) {
            const origin = Array.isArray(rec.origin)
              ? rec.origin
                  .filter(
                    (o): o is string =>
                      typeof o === "string" &&
                      o !== "" &&
                      o.length <= MAX_ID_LEN &&
                      watchedMap.has(o),
                  )
                  .slice(0, 8)
              : [];
            if (origin.length > 0) clean.origin = origin;
          }
          return clean;
        };

        // ---- 订阅与关注管理 ---------------------------------------------------

        /**
         * 订阅一个本 Project 的会话。返回值：
         * - "kept"：已有订阅且未过保鲜期、且近 10 分钟内有记录投递，直接复用；
         * - "new"：新建（或先退订再重订）了订阅；
         * - "failed"：会话不在本 Project（subscribe 抛错）。
         * 超过 SUB_MAX_AGE_MS（25 分钟）或超过 SUB_IDLE_RESUB_MS（10 分钟无投递，
         * 可能已被服务端以非闲置原因结束）时，先退订再重订。
         */
        const follow = (sessionId: string): "kept" | "new" | "failed" => {
          if (!live) return "failed";
          const existing = following.get(sessionId);
          if (existing) {
            const age = Date.now() - existing.at;
            const idle = Date.now() - existing.lastSeenAt;
            if (age < SUB_MAX_AGE_MS && idle <= SUB_IDLE_RESUB_MS) return "kept";
            existing.stop();
          }
          try {
            const at = Date.now();
            const stop = live(sessionId, (rec) => {
              try {
                const f = following.get(sessionId);
                if (f) f.lastSeenAt = Date.now();
                // 实时流由服务端产生，按可信来源折叠。
                fold(sessionId, rec, true);
              } catch (e) {
                host.log(`flow-trace: 折叠 ${logSafe(sessionId)} 的记录失败: ${logSafe(String(e))}`);
              }
            });
            following.set(sessionId, { stop, at, lastSeenAt: at });
            return "new";
          } catch {
            following.delete(sessionId);
            return "failed";
          }
        };

        const unfollow = (sessionId: string): void => {
          following.get(sessionId)?.stop();
          following.delete(sessionId);
        };

        /** 清掉某会话的内存缓存（pendingArtifact / lastAnswer）。 */
        const dropSessionCache = (sessionId: string): void => {
          lastAnswer.delete(sessionId);
          for (const [callId, art] of pendingArtifact)
            if (art.sessionId === sessionId) pendingArtifact.delete(callId);
        };

        /** 取消关注：退订并移出关注列表，但保留其历史事件与链接。 */
        const unwatchEntry = (sessionId: string): void => {
          unfollow(sessionId);
          dropSessionCache(sessionId);
          state.watched = state.watched.filter((w) => w.sessionId !== sessionId);
          watchedMap.delete(sessionId);
        };

        /**
         * 批量取消关注并登记冷却。
         * 逐个调用 unwatchEntry 时，每个受害者都要做一次 O(n) 的 filter 分配，
         * 关注上限淘汰一次可能剔除上千个会话 → 又退化成 O(n²)；
         * 这里只做一次 filter，把淘汰路径压成 O(n)。
         */
        const unwatchMany = (sessionIds: readonly string[]): number => {
          if (sessionIds.length === 0) return 0;
          const victims = new Set(sessionIds);
          const now = Date.now();
          for (const id of victims) {
            unfollow(id);
            dropSessionCache(id);
            watchedMap.delete(id);
            evictedAt.set(id, now);
          }
          const before = state.watched.length;
          state.watched = state.watched.filter((w) => !victims.has(w.sessionId));
          return before - state.watched.length;
        };

        /**
         * 彻底忘记某会话：退订、移出关注，并删除该会话的全部事件与相关派发链接。
         * 同时清掉该会话的链接去重标记（seenLinks），否则「忘记」之后派生边不会重建；
         * 记录指纹 seen 含会话 id，无需清理（重新回填同一批历史仍会被挡住）。
         * 返回删除的事件数。
         */
        const forget = (sessionId: string): number => {
          unwatchEntry(sessionId);
          const before = state.events.length;
          state.events = state.events.filter((e) => e.sessionId !== sessionId);
          state.links = state.links.filter((l) => l.from !== sessionId && l.to !== sessionId);
          for (const k of [...seenLinks]) {
            const i = k.indexOf("->");
            if (i < 0) continue;
            if (k.slice(0, i) === sessionId || k.slice(i + 2) === sessionId)
              seenLinks.delete(k);
          }
          return before - state.events.length;
        };

        // 重载 / 重启后，对状态里仍记录的会话重新订阅；订阅失败（如已不在 Project）则移出关注。
        if (live) {
          let changed = false;
          for (const w of [...state.watched]) {
            const fr = follow(w.sessionId);
            if (fr === "failed") {
              state.watched = state.watched.filter((x) => x.sessionId !== w.sessionId);
              watchedMap.delete(w.sessionId);
              changed = true;
            } else if (fr === "new") {
              const f = following.get(w.sessionId);
              if (f) {
                w.subAt = new Date(f.at).toISOString();
                changed = true;
              }
            }
          }
          if (changed) scheduleSave();
        }

        /** 转义 Markdown 表格单元格里的竖线。 */
        const cell = (value: unknown): string =>
          snippet(value, 300).replace(/\|/g, "\\|").replace(/\n/g, " ");

        const KIND_ZH: Record<FlowEvent["kind"], string> = {
          task_begin: "任务开始",
          task_end: "任务结束",
          prompt: "收到提问",
          delegate: "派发子智能体",
          link: "子会话链接",
          artifact: "产物写入",
          artifact_result: "产物结果",
        };

        /** 生成中文 Markdown 报告。 */
        const toMarkdown = (
          st: State,
          agents: { agentId: string }[],
        ): string => {
          const artifacts = st.events.filter(
            (e) => e.kind === "artifact" || e.kind === "artifact_result",
          );
          const results = new Map(
            st.events
              .filter((e) => e.kind === "artifact_result" && e.callId)
              .map((e) => [e.callId as string, e.status ?? ""]),
          );
          const lines: string[] = [];
          lines.push("# 智能体流转轨迹报告", "");
          lines.push(`- 生成时间：${nowIso()}`);
          lines.push(`- 工作台：flow-trace（agent-flow-trace 插件）`);
          lines.push("");
          lines.push("## 概览", "");
          lines.push(`- 智能体 ${agents.length} 个；关注会话 ${st.watched.length} 个`);
          lines.push(
            `- 流转事件 ${st.events.length} 条；派发链接 ${st.links.length} 条；产物 ${artifacts.length} 件`,
          );
          lines.push("");
          lines.push("## 智能体", "");
          lines.push("| 智能体 | 关注的会话数 |", "| --- | --- |");
          // 先一次聚合出每个智能体的会话数，避免对每个智能体都重扫一遍关注列表（O(A×W)）。
          const watchedPerAgent = new Map<string, number>();
          for (const w of st.watched)
            watchedPerAgent.set(w.agentId, (watchedPerAgent.get(w.agentId) ?? 0) + 1);
          for (const a of agents)
            lines.push(`| ${cell(a.agentId)} | ${watchedPerAgent.get(a.agentId) ?? 0} |`);
          lines.push("");
          lines.push("## 关注的会话", "");
          lines.push("| 会话 | 智能体 | 关注时间 |", "| --- | --- | --- |");
          for (const w of st.watched)
            lines.push(`| ${cell(w.sessionId)} | ${cell(w.agentId)} | ${cell(w.since)} |`);
          lines.push("");
          lines.push("## 派发链接（父会话 → 子会话）", "");
          lines.push("| 时间 | 父会话 | 父智能体 | 子会话 | 子智能体 |", "| --- | --- | --- | --- | --- |");
          for (const l of st.links)
            lines.push(
              `| ${cell(l.ts)} | ${cell(l.from)} | ${cell(agentOf(l.from))} | ${cell(l.to)} | ${cell(agentOf(l.to))} |`,
            );
          lines.push("");
          lines.push("## 产物台账", "");
          lines.push("| 时间 | 路径 | 工具 | 智能体 | 会话 | 结果 |", "| --- | --- | --- | --- | --- | --- |");
          for (const e of st.events.filter((x) => x.kind === "artifact"))
            lines.push(
              `| ${cell(e.ts)} | ${cell(e.path)} | ${cell(e.tool)} | ${cell(e.agentId)} | ${cell(e.sessionId)} | ${cell(results.get(e.callId ?? "") ?? "进行中")} |`,
            );
          lines.push("");
          lines.push("## 事件明细（新的在前，最多 500 条）", "");
          lines.push("| 序号 | 时间 | 类型 | 智能体 | 会话 | 摘要 |", "| --- | --- | --- | --- | --- | --- |");
          for (const e of [...st.events].reverse().slice(0, 500))
            lines.push(
              `| ${e.seq} | ${cell(e.ts)} | ${KIND_ZH[e.kind]} | ${cell(e.agentId)} | ${cell(e.sessionId)} | ${cell(
                [e.path, e.text, e.status, e.targetAgentId, e.childSessionId]
                  .filter((v) => v)
                  .join(" "),
              )} |`,
            );
          lines.push("");
          return lines.join("\n");
        };

        /** 文件名里的时间戳，去掉非法字符。 */
        const fileStamp = (): string => nowIso().replace(/[:.]/g, "-");

        const download = (
          filename: string,
          contentType: string,
          body: string,
        ): WorkbenchResponse => ({
          headers: {
            "content-type": contentType,
            "content-disposition": `attachment; filename="${filename}"`,
          },
          body,
        });

        /** /status、/flow、/watch 等响应里的 watched 一律返回拷贝，避免响应体与内部状态共享引用。 */
        const watchedCopy = (): WatchEntry[] => state.watched.map((w) => ({ ...w }));

        /**
         * 导出：锁内只做一次快照拷贝，序列化放到锁外。
         * 旧实现把 `JSON.stringify(…, null, 2)` 整段放在 locked 里，最多 4000 事件 + 4000 链接
         * 会产出数 MB 字符串、耗时数百毫秒，期间所有轮询请求都被串行锁挡住。
         */
        const handleExport = async (req: WorkbenchRequest): Promise<WorkbenchResponse> => {
          const snap = await locked(async () => ({
            agents: host.listAgents(),
            watched: watchedCopy(),
            links: state.links.map((l) => ({ ...l })),
            events: state.events.map((e) => ({ ...e })),
            seq: state.seq,
          }));
          const format = req.query.format ?? "json";
          host.log(
            `flow-trace: 导出 ${logSafe(format)}（${snap.events.length} 事件 / ${snap.links.length} 链接）`,
          );
          if (format === "json")
            return download(
              `agent-flow-trace-${fileStamp()}.json`,
              "application/json; charset=utf-8",
              // 不再缩进：缩进会让导出体积翻倍，而序列化本身在锁外，缩进收益不抵开销。
              JSON.stringify({
                exportedAt: nowIso(),
                workbench: "flow-trace",
                agents: snap.agents,
                watched: snap.watched,
                links: snap.links,
                events: snap.events,
              }),
            );
          if (format === "jsonl")
            return download(
              `agent-flow-trace-${fileStamp()}.jsonl`,
              "application/x-ndjson; charset=utf-8",
              snap.events.map((e) => JSON.stringify(e)).join("\n") +
                (snap.events.length > 0 ? "\n" : ""),
            );
          if (format === "md") {
            const st: State = {
              watched: snap.watched,
              events: snap.events,
              links: snap.links,
              seq: snap.seq,
            };
            return download(
              `agent-flow-trace-${fileStamp()}.md`,
              "text/markdown; charset=utf-8",
              toMarkdown(st, snap.agents),
            );
          }
          return { status: 400, body: { error: "format 须为 json、jsonl 或 md" } };
        };

        return {
          api: {
            main: {
              async handle(req: WorkbenchRequest): Promise<WorkbenchResponse> {
                // 导出走独立路径：只在锁内取快照，序列化在锁外完成。
                if (req.method === "GET" && req.path === "/export") return handleExport(req);
                return locked(async () => {
                  if (req.method === "GET" && req.path === "/status") {
                    return {
                      body: {
                        ok: true,
                        workbench: "flow-trace",
                        subscribeSupported: live !== null,
                        agents: host.listAgents(),
                        watched: watchedCopy(),
                        liveFollows: following.size,
                        eventCount: state.events.length,
                        lastSaveError: lastSaveError || undefined,
                        limits: {
                          maxEvents: MAX_EVENTS,
                          ingestMax: INGEST_MAX,
                          maxWatched: MAX_WATCHED,
                          recordKeyMax: RECORD_KEY_MAX,
                          maxWatchItems: MAX_WATCH_ITEMS,
                          maxIdLen: MAX_ID_LEN,
                        },
                      },
                    };
                  }
                  if (req.method === "GET" && req.path === "/flow") {
                    // 增量拉取：?since=<seq> 只返回该序号之后的事件与新链接，并带上 maxSeq / minSeq。
                    // minSeq 是缓冲里最旧事件的序号：FIFO 淘汰后页面据此剪掉本地视图里已不存在的事件。
                    const sinceRaw = req.query.since;
                    const since =
                      typeof sinceRaw === "string" && /^\d+$/.test(sinceRaw)
                        ? parseInt(sinceRaw, 10)
                        : 0;
                    const minSeq =
                      state.events.length > 0 ? state.events[0].seq : state.seq + 1;
                    return {
                      body: {
                        agents: host.listAgents(),
                        watched: watchedCopy(),
                        links: (since > 0
                          ? state.links.filter((l) => (l.seq ?? 0) > since)
                          : state.links
                        ).map((l) => ({ ...l })),
                        events: (since > 0
                          ? state.events.filter((e) => e.seq > since)
                          : state.events
                        ).map((e) => ({ ...e })),
                        maxSeq: state.seq,
                        minSeq,
                        epoch: state.epoch,
                      },
                    };
                  }
                  if (req.method === "POST" && req.path === "/watch") {
                    const body = req.body as { sessions?: unknown } | null;
                    const rawList = Array.isArray(body?.sessions) ? body.sessions : [];
                    // 请求体规模上限：不设限时一次请求就能把关注列表堆到 N，
                    // 再叠加淘汰逻辑变成 O(n²) 的长阻塞（而所有路由共用一把串行锁）。
                    const list =
                      rawList.length > MAX_WATCH_ITEMS
                        ? rawList.slice(0, MAX_WATCH_ITEMS)
                        : rawList;
                    const watchCapped = rawList.length > list.length;
                    // 客户端提交的 agentId 只作参考：必须属于本 Project 的智能体集合才采信。
                    const agentIds = new Set(host.listAgents().map((a) => a.agentId));
                    let added = 0;
                    let refreshed = 0;
                    let skipped = 0;
                    let invalid = 0;
                    let evicted = 0;
                    let deferred = 0;
                    let subChanged = false;
                    const requested = new Set<string>();
                    const addedNow = new Set<string>();
                    /**
                     * 排序键：优先用页面带来的会话活跃时间，其次 lastSeen / since。
                     * 一律换算成毫秒再比——字符串比较在混入 ±hh:mm 偏移时会得出相反顺序。
                     */
                    const actMs = (w: WatchEntry): number => {
                      const a = tsOf(w.activity);
                      if (Number.isFinite(a)) return a;
                      const s = tsOf(w.lastSeen ?? w.since);
                      return Number.isFinite(s) ? s : 0;
                    };
                    // sessionId -> WatchEntry 索引：替代循环里每次 O(N) 的 find（N 上限 300）。
                    const idx = new Map<string, WatchEntry>();
                    for (const w of state.watched) idx.set(w.sessionId, w);
                    let metaChanged = false; // agentId / activity 变化也要落盘（此前只 refreshed 不写回）
                    // 惰性清理过期冷却记录，避免 evictedAt 无界增长。
                    {
                      const nowMs = Date.now();
                      for (const [k, t] of evictedAt)
                        if (nowMs - t >= EVICT_READD_MS) evictedAt.delete(k);
                    }
                    for (const item of list) {
                      if (item === null || typeof item !== "object") continue;
                      const { sessionId, agentId, lastActiveAt } = item as Record<
                        string,
                        unknown
                      >;
                      if (
                        typeof sessionId !== "string" ||
                        !sessionId ||
                        sessionId.length > MAX_ID_LEN ||
                        typeof agentId !== "string" ||
                        !agentId ||
                        agentId.length > MAX_ID_LEN ||
                        !agentIds.has(agentId)
                      ) {
                        invalid += 1;
                        continue;
                      }
                      const la =
                        typeof lastActiveAt === "string" &&
                        lastActiveAt.length > 0 &&
                        lastActiveAt.length <= 64
                          ? lastActiveAt
                          : undefined;
                      requested.add(sessionId);
                      const existing = idx.get(sessionId);
                      if (!existing) {
                        // 刚被上限淘汰的会话在冷却期内不自动重新加入，避免每轮轮询反复进出。
                        const et = evictedAt.get(sessionId);
                        if (et !== undefined && Date.now() - et < EVICT_READD_MS) {
                          deferred += 1;
                          continue;
                        }
                      }
                      if (existing) {
                        if (existing.agentId !== agentId) metaChanged = true;
                        existing.agentId = agentId;
                        if (la && existing.activity !== la) metaChanged = true;
                        if (la) existing.activity = la;
                        // 只刷新活跃时间，不覆盖首次关注时间 since：
                        // 否则「按最旧淘汰」会退化成按数组顺序，且每轮都触发整文件落盘。
                        existing.lastSeen = nowIso();
                        refreshed += 1;
                      } else {
                        const w: WatchEntry = {
                          sessionId,
                          agentId,
                          since: nowIso(),
                          lastSeen: nowIso(),
                          activity: la,
                        };
                        state.watched.push(w);
                        idx.set(sessionId, w);
                        added += 1;
                        addedNow.add(sessionId);
                      }
                      watchedMap.set(sessionId, agentId);
                    }
                    /**
                     * 关注上限淘汰：优先保留本次请求中的会话，淘汰不在其中且活跃时间最旧的。
                     * 旧实现是「先 filter + sort 一批，再用 while 循环逐个 unwatch」——循环里
                     * 每轮都要 O(n) 找最旧、再 O(n) 做 filter 分配，总复杂度 O(n²)；
                     * 现在改成「一次排序 + 一次切片 + 一次批量 filter」，整体 O(n log n)。
                     */
                    if (state.watched.length > MAX_WATCHED) {
                      const excess = state.watched.length - MAX_WATCHED;
                      const byAge = (a: WatchEntry, b: WatchEntry): number => {
                        const x = actMs(a);
                        const y = actMs(b);
                        return x < y ? -1 : x > y ? 1 : 0;
                      };
                      const victims = state.watched
                        .filter((w) => !requested.has(w.sessionId))
                        .sort(byAge)
                        .slice(0, excess);
                      // 仍超限（本次请求的会话本身就超过上限）：退化为淘汰其中活跃时间最旧的，
                      // 平局时保留靠后的提交（稳定排序 + slice 取头部）。
                      if (victims.length < excess) {
                        const extra = state.watched
                          .filter((w) => requested.has(w.sessionId))
                          .sort(byAge)
                          .slice(0, excess - victims.length);
                        victims.push(...extra);
                      }
                      evicted = unwatchMany(victims.map((w) => w.sessionId));
                      // 被淘汰的若是本轮新加入的，要从 addedNow 里摘掉（后续订阅循环据此判断）。
                      for (const id of [...addedNow]) if (!watchedMap.has(id)) addedNow.delete(id);
                    }
                    // 淘汰改变了 state.watched，重建索引后再查找。
                    idx.clear();
                    for (const w of state.watched) idx.set(w.sessionId, w);
                    // 对本次请求且仍在关注中的会话确保订阅（25 分钟保鲜 / 10 分钟无投递自动重订）；
                    // 订阅失败不入关注。
                    if (live) {
                      // 订阅失败（会话不在本 Project）的会话先收集、最后一次性 filter，
                      // 避免每次失败都对 watched 做一遍 O(n) 重建。
                      const failedIds: string[] = [];
                      for (const sessionId of requested) {
                        const w = idx.get(sessionId);
                        if (!w) continue; // 本轮刚被淘汰
                        const fr = follow(sessionId);
                        if (fr === "failed") {
                          skipped += 1;
                          failedIds.push(sessionId);
                          watchedMap.delete(sessionId);
                          if (addedNow.has(sessionId)) {
                            added -= 1;
                            addedNow.delete(sessionId);
                          }
                        } else if (fr === "new") {
                          const f = following.get(sessionId);
                          if (f) {
                            w.subAt = new Date(f.at).toISOString();
                            subChanged = true;
                          }
                        }
                      }
                      if (failedIds.length > 0) {
                        const failedSet = new Set(failedIds);
                        state.watched = state.watched.filter(
                          (x) => !failedSet.has(x.sessionId),
                        );
                      }
                    }
                    // 只有真正改变关注结构 / 订阅时刻时才落盘：无变化的轮询不再每 15 秒整文件重写。
                    if (added > 0 || evicted > 0 || skipped > 0 || subChanged || metaChanged)
                      scheduleSave();
                    await flush();
                    return {
                      body: {
                        ok: true,
                        added,
                        refreshed,
                        skipped,
                        invalid,
                        evicted,
                        deferred,
                        capped: watchCapped,
                        watched: watchedCopy(),
                      },
                    };
                  }
                  if (req.method === "POST" && req.path === "/unwatch") {
                    const body = req.body as { sessionId?: unknown } | null;
                    const sessionId = body?.sessionId;
                    if (
                      typeof sessionId === "string" &&
                      sessionId &&
                      sessionId.length <= MAX_ID_LEN
                    )
                      unwatchEntry(sessionId);
                    scheduleSave();
                    await flush();
                    return { body: { ok: true, watched: watchedCopy() } };
                  }
                  if (req.method === "POST" && req.path === "/forget") {
                    const body = req.body as { sessionId?: unknown } | null;
                    const sessionId = body?.sessionId;
                    let removed = 0;
                    if (
                      typeof sessionId === "string" &&
                      sessionId &&
                      sessionId.length <= MAX_ID_LEN
                    ) {
                      removed = forget(sessionId);
                      scheduleSave();
                    }
                    await flush();
                    return { body: { ok: true, removed, watched: watchedCopy() } };
                  }
                  if (req.method === "POST" && req.path === "/ingest") {
                    // 历史回填：页面把 GET /sessions/:id/messages?tailLimit=N 的记录批量提交，
                    // 由 fold 折叠入账（持久化指纹去重，重复回填不会重复计数）。只接受已关注的会话。
                    // 信任边界：记录来自客户端页面，必须先经 sanitizeRecord 形状校验；
                    // origin 只允许指向已关注会话；同一会话 60 秒内只接受一次（节流），
                    // 防止伪造台账或反复冲刷事件缓冲。
                    const body = req.body as { sessionId?: unknown; records?: unknown } | null;
                    const sessionId = body?.sessionId;
                    const records = Array.isArray(body?.records) ? body.records : [];
                    let ingested = 0;
                    let rejected = 0;
                    let stale = 0;
                    let throttled = false;
                    // 惰性清理过期节流记录，避免 lastIngestAt 无界增长。
                    {
                      const nowMs = Date.now();
                      for (const [k, t] of lastIngestAt)
                        if (nowMs - t >= INGEST_MIN_INTERVAL_MS) lastIngestAt.delete(k);
                    }
                    if (
                      typeof sessionId === "string" &&
                      sessionId &&
                      sessionId.length <= MAX_ID_LEN &&
                      watchedMap.has(sessionId) &&
                      records.length > 0
                    ) {
                      const last = lastIngestAt.get(sessionId) ?? 0;
                      if (Date.now() - last < INGEST_MIN_INTERVAL_MS) {
                        throttled = true;
                      } else {
                        // /messages 返回旧→新顺序：slice(-INGEST_MAX) 保留最新的 500 条，而不是最旧的。
                        // 校验循环之后再登记节流时刻：全被拒的请求不消耗节流窗口。
                        const oldestMs = state.events.length > 0 ? tsOf(state.events[0].ts) : NaN;
                        const full = state.events.length >= MAX_EVENTS;
                        const nowMs = Date.now();
                        const accepted: AmspRecord[] = [];
                        for (const rec of records.slice(-INGEST_MAX)) {
                          const clean = sanitizeRecord(rec);
                          if (!clean) {
                            rejected += 1;
                            continue;
                          }
                          // 时间戳一律换算成毫秒再比较：直接比字符串在混入 ±hh:mm 偏移时
                          // 会把「更新」的记录判成更旧（反之亦然）。
                          const recMs = tsOf(clean.timestamp);
                          // 明显超前的未来时间戳直接拒收。否则提交 "9999-01-01T00:00:00Z"
                          // 就能恒大于缓冲最旧事件，绕过下面「旧记录复活」的防护。
                          if (Number.isFinite(recMs) && recMs - nowMs > INGEST_FUTURE_SKEW_MS) {
                            rejected += 1;
                            continue;
                          }
                          // 缓冲已满时拒收早于最旧事件的记录（计入 stale），防止被淘汰的
                          // 旧记录「复活」并把实时事件挤出缓冲；缓冲不满时照常放行，
                          // 保证启动回填 / 缺口补登不受影响。
                          if (full && Number.isFinite(oldestMs) && Number.isFinite(recMs) && recMs < oldestMs) {
                            stale += 1;
                            continue;
                          }
                          accepted.push(clean);
                        }
                        lastIngestAt.set(sessionId, Date.now());
                        for (const clean of accepted) {
                          try {
                            // 回填记录来自页面，按不可信来源折叠（派发端点需过关注集合白名单）。
                            fold(sessionId, clean, false);
                            ingested += 1;
                          } catch (e) {
                            host.log(`flow-trace: 回填记录折叠失败: ${logSafe(String(e))}`);
                          }
                        }
                      }
                    }
                    await flush();
                    return {
                      body: {
                        ok: true,
                        ingested,
                        rejected,
                        stale,
                        throttled,
                        capped: records.length > INGEST_MAX,
                      },
                    };
                  }
                  if (req.method === "POST" && req.path === "/clear") {
                    state.events = [];
                    state.links = [];
                    state.seq = 0;
                    // 纪元轮换：客户端检测到 epoch 变化即整体重拉，不依赖 seq 大小比较。
                    state.epoch = newEpoch();
                    seen.clear();
                    seenLinks.clear();
                    pendingArtifact.clear();
                    lastAnswer.clear();
                    lastIngestAt.clear();
                    // 淘汰冷却也一并清掉：「清空记录」应被视为一次彻底重置，
                    // 否则刚被上限淘汰的会话在 10 分钟内既不能自动重加、也不会重新回填。
                    evictedAt.clear();
                    scheduleSave();
                    await flush();
                    host.log("flow-trace: 已清空全部事件与派发链接");
                    return { body: { ok: true } };
                  }
                  // /export 不在这里处理：它在锁内只取快照，序列化在锁外进行（见 handleExport）。
                  return {
                    status: 404,
                    body: { error: `未知路由: ${req.method} ${req.path}` },
                  };
                });
              },
            },
          },
        };
      },
    },
  },
} satisfies WorkbenchPackage;
