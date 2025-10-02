<script setup lang="ts">
import axios, { AxiosError } from "axios";
import { ElMessage } from "element-plus";
import { onBeforeUnmount, onMounted, reactive, watch } from "vue";

type Dict = Record<string, any>;

type ChatStartRequest = {
  initial_state?: Dict | null;
  use_world_state?: boolean;
};

type ChatStartResponse = {
  session_id: string;
  branch_id: string;
  state_snapshot: Dict;
};

type ChatSendRequest = {
  session_id: string;
  branch_id?: string | null;
  user_input: string;
  ref: string;
  extras?: Dict | null;
  resources?: Dict | null;
};

type ChatSendResponse = {
  round_no: number;
  snapshot_id: string;
  llm_reply: string;
  items: Dict[];
  logs: string[];
  metrics: Dict;
  state_snapshot: Dict;
  round_status: string;
};

type RoundStatusResponse = {
  round_no: number;
  status: string;
  blockers: string[];
};

type RerollRequest = {
  session_id: string;
  branch_id: string;
  round_no: number;
  ref: string;
  extras?: Dict | null;
  resources?: Dict | null;
};

type RerollResponse = {
  round_no: number;
  llm_reply: string;
  items: Dict[];
  logs: string[];
  metrics: Dict;
  state_snapshot: Dict;
  round_status: string;
};

type BranchRequest = {
  session_id: string;
  from_round?: number | null;
  parent_branch_id?: string | null;
  set_active?: boolean;
};

type BranchResponse = {
  branch_id: string;
};

type RunFlowRequest = {
  ref: string;
  items: Dict[];
  session_id?: string | null;
  use_world_state?: boolean;
  initial_state?: Dict | null;
  resources?: Dict | null;
};

type RunFlowResponse = {
  items: Dict[];
  logs: string[];
  metrics: Dict;
  state_snapshot: Dict;
};

type ReloadRequest = { dirs?: string[] | null };
type ReloadResponse = { flows: string[]; node_types: string[] };

type ValidateDocRequest = { doc: Dict };

// Debug traffic types
type TrafficEvent = {
  id: string;
  ts: string;
  type: string;
  service: string;
  method?: string;
  url?: string;
  req_headers?: Dict;
  req_body?: string;
  status?: number;
  elapsed_ms?: number;
  resp_headers?: Dict;
  resp_body?: string;
  error?: string;
  pair_id?: string;
};
type TrafficListResponse = { count: number; events: TrafficEvent[] };

const LS_KEYS = {
  session: "debugconsole.sessionId",
  branch: "debugconsole.branchId",
  ref: "debugconsole.ref",
  round: "debugconsole.lastRoundNo",
  extras: "debugconsole.extras",
  userInput: "debugconsole.userInput",
} as const;

// Global state
const state = reactive({
  sessionId: "" as string,
  branchId: "" as string,
  ref: (localStorage.getItem(LS_KEYS.ref) ?? "main@1") as string,
  userInput: (localStorage.getItem(LS_KEYS.userInput) ?? "") as string,
  extrasText: (localStorage.getItem(LS_KEYS.extras) ?? "{\n  \n}") as string,
  sending: false,
  polling: true,
  pollIntervalMs: 2500,
  lastRoundNo: Number(localStorage.getItem(LS_KEYS.round) ?? "0"),
  lastSnapshotId: "" as string,
  lastRoundStatus: "" as string,
  lastBlockers: [] as string[],

  // Results (Chat)
  reply: "" as string,
  items: [] as Dict[],
  logs: [] as string[],
  metrics: {} as Dict,
  stateSnapshot: {} as Dict,

  // Admin info
  reloadInfo: null as ReloadResponse | null,

  // Validate IR
  validateJson: "{\n  \n}",
  validateResult: null as null | { valid: boolean; error?: string },

  // Run Flow
  runFlowRef: "main@1",
  runFlowItemsText: '[\n  {\n    "user_input": "我走进酒馆，环顾四周。"\n  }\n]',
  runFlowResp: null as RunFlowResponse | null,

  // Error last
  lastHttpStatus: null as number | null,
  lastHttpError: "" as string,

  // Traffic (LLM outbound)
  trafficCount: 0,
  trafficEvents: [] as TrafficEvent[],
});

let pollTimer: any = null;

function saveLocal() {
  if (state.sessionId) localStorage.setItem(LS_KEYS.session, state.sessionId);
  if (state.branchId) localStorage.setItem(LS_KEYS.branch, state.branchId);
  if (state.ref) localStorage.setItem(LS_KEYS.ref, state.ref);
  localStorage.setItem(LS_KEYS.userInput, state.userInput ?? "");
  localStorage.setItem(LS_KEYS.extras, state.extrasText ?? "");
  if (state.lastRoundNo) localStorage.setItem(LS_KEYS.round, String(state.lastRoundNo));
}

function loadLocal() {
  state.sessionId = localStorage.getItem(LS_KEYS.session) ?? "";
  state.branchId = localStorage.getItem(LS_KEYS.branch) ?? "";
  state.ref = localStorage.getItem(LS_KEYS.ref) ?? state.ref;
  state.lastRoundNo = Number(localStorage.getItem(LS_KEYS.round) ?? "0");
}

function pretty(obj: any) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

function safeParseJson(
  text: string
): { ok: true; value: any } | { ok: false; error: string } {
  if (!text.trim()) return { ok: true, value: null };
  try {
    const v = JSON.parse(text);
    return { ok: true, value: v };
  } catch (e: any) {
    return { ok: false, error: e?.message ?? "JSON 解析失败" };
  }
}

function setError(err: unknown) {
  if (axios.isAxiosError(err)) {
    const e = err as AxiosError<any>;
    state.lastHttpStatus = e.response?.status ?? null;
    const detail = (e.response?.data as any)?.detail ?? e.message;
    state.lastHttpError = typeof detail === "string" ? detail : JSON.stringify(detail);
    ElMessage.error(`HTTP ${state.lastHttpStatus ?? ""}: ${state.lastHttpError}`);
  } else {
    state.lastHttpStatus = null;
    state.lastHttpError = String(err);
    ElMessage.error(state.lastHttpError);
  }
}

async function startSession() {
  try {
    const payload: ChatStartRequest = { use_world_state: true };
    const { data } = await axios.post<ChatStartResponse>(
      "/api/chat/session/start",
      payload
    );
    state.sessionId = data.session_id;
    state.branchId = data.branch_id;
    state.stateSnapshot = data.state_snapshot ?? {};
    saveLocal();
    ElMessage.success("会话已创建");
  } catch (err) {
    setError(err);
  }
}

async function sendOnce() {
  if (!state.sessionId || !state.branchId) {
    ElMessage.warning("请先创建会话");
    return;
  }
  if (!state.userInput.trim()) {
    ElMessage.warning("请输入 user_input");
    return;
  }
  const extras = safeParseJson(state.extrasText);
  if (!extras.ok) {
    ElMessage.error("extras JSON 解析失败: " + extras.error);
    return;
  }

  state.sending = true;
  try {
    const payload: ChatSendRequest = {
      session_id: state.sessionId,
      branch_id: state.branchId,
      user_input: state.userInput,
      ref: state.ref || "main@1",
      extras: extras.value ?? undefined,
    };
    const { data } = await axios.post<ChatSendResponse>("/api/chat/send", payload);
    state.reply = data.llm_reply;
    state.items = data.items ?? [];
    state.logs = data.logs ?? [];
    state.metrics = data.metrics ?? {};
    state.stateSnapshot = data.state_snapshot ?? {};
    state.lastRoundStatus = data.round_status ?? "";
    state.lastRoundNo = data.round_no ?? state.lastRoundNo;
    state.lastSnapshotId = data.snapshot_id ?? "";
    saveLocal();
    // Refresh outbound traffic list after a successful send
    loadTraffic();
  } catch (err) {
    setError(err);
  } finally {
    state.sending = false;
  }
}

async function pollStatusOnce() {
  if (!state.sessionId || !state.branchId || !state.lastRoundNo) return;
  try {
    const url = `/api/chat/round/${encodeURIComponent(
      state.sessionId
    )}/${encodeURIComponent(state.branchId)}/${state.lastRoundNo}/status`;
    const { data } = await axios.get<RoundStatusResponse>(url);
    state.lastRoundStatus = data.status ?? state.lastRoundStatus;
    state.lastBlockers = data.blockers ?? [];
  } catch (err) {
    // 不中断，让 UI 继续
  }
}

async function rerollLast() {
  if (!state.sessionId || !state.branchId || !state.lastRoundNo) {
    ElMessage.warning("缺少 session/branch/round 信息");
    return;
  }
  try {
    const payload: RerollRequest = {
      session_id: state.sessionId,
      branch_id: state.branchId,
      round_no: state.lastRoundNo,
      ref: state.ref || "main@1",
    };
    const { data } = await axios.post<RerollResponse>("/api/chat/round/reroll", payload);
    state.reply = data.llm_reply;
    state.items = data.items ?? [];
    state.logs = data.logs ?? [];
    state.metrics = data.metrics ?? {};
    state.stateSnapshot = data.state_snapshot ?? {};
    state.lastRoundStatus = data.round_status ?? state.lastRoundStatus;
    ElMessage.success("重roll完成");
  } catch (err) {
    setError(err);
  }
}

async function branchFromLast() {
  if (!state.sessionId) {
    ElMessage.warning("请先创建会话");
    return;
  }
  try {
    const payload: BranchRequest = {
      session_id: state.sessionId,
      from_round: state.lastRoundNo || undefined,
      parent_branch_id: state.branchId || undefined,
      set_active: true,
    };
    const { data } = await axios.post<BranchResponse>("/api/chat/branch", payload);
    state.branchId = data.branch_id;
    saveLocal();
    ElMessage.success("已从最近回合创建新分支并设为活动分支");
  } catch (err) {
    setError(err);
  }
}

async function reloadFlows() {
  try {
    const payload: ReloadRequest = {};
    const { data } = await axios.post<ReloadResponse>("/api/flow/reload", payload);
    state.reloadInfo = data;
    ElMessage.success("Flows 重新加载完成");
  } catch (err) {
    setError(err);
  }
}

async function validateIR() {
  const parsed = safeParseJson(state.validateJson);
  if (!parsed.ok) {
    ElMessage.error("校验 JSON 解析失败: " + parsed.error);
    return;
  }
  try {
    const payload: ValidateDocRequest = { doc: parsed.value ?? {} };
    const { data } = await axios.post("/api/flow/validate", payload);
    state.validateResult = data as any;
    if ((data as any).valid) ElMessage.success("IR Schema 校验通过");
    else ElMessage.error("IR 校验未通过：" + ((data as any).error ?? "未知错误"));
  } catch (err) {
    setError(err);
  }
}

async function runFlow() {
  const items = safeParseJson(state.runFlowItemsText);
  if (!items.ok || !Array.isArray(items.value)) {
    ElMessage.error("items 必须是 JSON 数组: " + (!items.ok ? items.error : ""));
    return;
  }
  const payload: RunFlowRequest = {
    ref: state.runFlowRef || "main@1",
    items: items.value,
    use_world_state: true,
  };
  try {
    const { data } = await axios.post<RunFlowResponse>("/api/flow/run", payload);
    state.runFlowResp = data;
    ElMessage.success("Flow 执行完成");
  } catch (err) {
    setError(err);
  }
}

// Debug traffic
async function loadTraffic() {
  try {
    const { data } = await axios.get<TrafficListResponse>("/api/debug/traffic?limit=200");
    state.trafficCount = (data as any).count ?? 0;
    state.trafficEvents = (data as any).events ?? [];
  } catch (err) {
    // ignore
  }
}

async function clearTraffic() {
  try {
    await axios.post("/api/debug/traffic/clear");
    await loadTraffic();
  } catch (err) {
    // ignore
  }
}

function copy(text: string) {
  navigator.clipboard?.writeText(text).then(() => {
    ElMessage.success("已复制到剪贴板");
  });
}

// Poller
watch(
  () => state.polling,
  (on) => {
    if (on) {
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = setInterval(pollStatusOnce, state.pollIntervalMs);
    } else {
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = null;
    }
  },
  { immediate: true }
);

onMounted(() => {
  loadLocal();
  // first poll
  if (state.polling) {
    pollTimer = setInterval(pollStatusOnce, state.pollIntervalMs);
  }
  // initial debug traffic fetch
  loadTraffic();
});

onBeforeUnmount(() => {
  if (pollTimer) clearInterval(pollTimer);
});
</script>

<template>
  <el-container class="layout">
    <el-header class="header">
      <div class="title">SmartTavern Debug Console</div>
      <div class="spacer" />
      <el-switch
        v-model="state.polling"
        active-text="轮询状态"
        inactive-text="停止轮询"
      />
    </el-header>

    <el-container>
      <el-aside width="360px" class="aside">
        <el-card shadow="never" class="block">
          <template #header><b>会话</b></template>
          <div class="kv">
            <span class="k">session_id</span
            ><span class="v mono">{{ state.sessionId || "-" }}</span>
          </div>
          <div class="kv">
            <span class="k">branch_id</span
            ><span class="v mono">{{ state.branchId || "-" }}</span>
          </div>
          <el-button type="primary" @click="startSession" class="mt8">创建会话</el-button>
        </el-card>

        <el-card shadow="never" class="block">
          <template #header><b>运维工具</b></template>
          <div class="tool-row">
            <el-button size="small" @click="reloadFlows">Reload Flows</el-button>
          </div>
          <div v-if="state.reloadInfo" class="mt8 small">
            <div>
              <b>flows:</b>
              <span class="mono">{{ state.reloadInfo.flows.join(", ") || "-" }}</span>
            </div>
            <div>
              <b>node_types:</b>
              <span class="mono">{{
                state.reloadInfo.node_types.join(", ") || "-"
              }}</span>
            </div>
          </div>

          <el-divider />

          <div class="tool-row"><b>Validate IR</b></div>
          <el-input
            v-model="state.validateJson"
            type="textarea"
            :rows="6"
            placeholder="粘贴 IR JSON 进行校验"
          />
          <el-button class="mt8" type="primary" size="small" @click="validateIR"
            >校验</el-button
          >
          <div v-if="state.validateResult" class="mt8 small">
            <div>
              <b>valid:</b> {{ (state.validateResult as any).valid ? 'true' : 'false' }}
            </div>
            <div v-if="(state.validateResult as any).error">
              <b>error:</b>
              <span class="mono">{{ (state.validateResult as any).error }}</span>
            </div>
          </div>

          <el-divider />

          <div class="tool-row"><b>Run Flow</b></div>
          <el-form label-width="72">
            <el-form-item label="ref">
              <el-input v-model="state.runFlowRef" placeholder="main@1" />
            </el-form-item>
            <el-form-item label="items">
              <el-input
                v-model="state.runFlowItemsText"
                type="textarea"
                :rows="6"
                placeholder='[{"user_input":"hi"}]'
              />
            </el-form-item>
          </el-form>
          <el-button type="primary" size="small" @click="runFlow">执行</el-button>
          <div v-if="state.runFlowResp" class="mt8 small">
            <div class="mono">{{ pretty(state.runFlowResp) }}</div>
          </div>
        </el-card>
      </el-aside>

      <el-main class="main">
        <el-card shadow="never" class="block">
          <template #header><b>发送</b></template>
          <el-form label-width="72">
            <el-form-item label="ref">
              <el-input v-model="state.ref" placeholder="main@1" @change="saveLocal" />
            </el-form-item>
            <el-form-item label="user_input">
              <el-input
                v-model="state.userInput"
                type="textarea"
                :rows="3"
                placeholder="我走进酒馆，环顾四周。"
                @change="saveLocal"
              />
            </el-form-item>
            <el-form-item label="extras">
              <el-input
                v-model="state.extrasText"
                type="textarea"
                :rows="6"
                placeholder='可选：{"key":"value"}'
                @change="saveLocal"
              />
            </el-form-item>
          </el-form>

          <div class="btns">
            <el-button type="primary" :loading="state.sending" @click="sendOnce"
              >发送</el-button
            >
            <el-button @click="rerollLast" :disabled="!state.lastRoundNo"
              >重roll最近回合</el-button
            >
            <el-button @click="branchFromLast" :disabled="!state.lastRoundNo"
              >从最近回合分支</el-button
            >
          </div>

          <el-alert
            v-if="state.lastHttpStatus !== null || state.lastHttpError"
            :title="`HTTP: ${state.lastHttpStatus ?? ''} ${state.lastHttpError ?? ''}`"
            type="error"
            show-icon
            class="mt8"
            :closable="false"
          />
        </el-card>

        <el-card shadow="never" class="block">
          <template #header><b>结果</b></template>
          <el-tabs type="border-card">
            <el-tab-pane label="Reply">
              <div class="reply">{{ state.reply || "-" }}</div>
              <div class="small mt8 mono">
                snapshot_id: {{ state.lastSnapshotId || "-" }} | round_no:
                {{ state.lastRoundNo || "-" }} | round_status:
                {{ state.lastRoundStatus || "-" }}
              </div>
            </el-tab-pane>
            <el-tab-pane label="Items">
              <div class="mono">{{ pretty(state.items) }}</div>
              <el-button size="small" class="mt8" @click="copy(pretty(state.items))"
                >复制</el-button
              >
            </el-tab-pane>
            <el-tab-pane label="Logs">
              <div class="mono small">
                <div v-for="(l, i) in state.logs" :key="i">{{ l }}</div>
              </div>
              <el-button size="small" class="mt8" @click="copy(state.logs.join('\n'))"
                >复制</el-button
              >
            </el-tab-pane>
            <el-tab-pane label="Metrics">
              <div class="mono small">{{ pretty(state.metrics) }}</div>
            </el-tab-pane>
            <el-tab-pane label="State">
              <div class="mono small">{{ pretty(state.stateSnapshot) }}</div>
            </el-tab-pane>
            <el-tab-pane label="Status">
              <div class="small">
                <div><b>status:</b> {{ state.lastRoundStatus || "-" }}</div>
                <div>
                  <b>blockers:</b>
                  <span class="mono">{{
                    (state.lastBlockers || []).join(", ") || "-"
                  }}</span>
                </div>
              </div>
              <el-button size="small" class="mt8" @click="pollStatusOnce">立即查询</el-button>
            </el-tab-pane>
            <el-tab-pane label="Traffic">
              <div class="small">
                <div><b>count:</b> {{ state.trafficCount }}</div>
                <div class="btns mt8">
                  <el-button size="small" @click="loadTraffic">刷新</el-button>
                  <el-button size="small" @click="clearTraffic">清空</el-button>
                </div>
              </div>
              <div class="mono small mt8">
                <div v-for="ev in state.trafficEvents" :key="ev.id">
                  [{{ ev.ts }}] {{ ev.type }} {{ ev.method || '' }} {{ ev.url || '' }} status={{ ev.status || '' }} elapsed={{ ev.elapsed_ms || '' }}ms
                </div>
              </div>
            </el-tab-pane>
          </el-tabs>
        </el-card>
      </el-main>
    </el-container>
  </el-container>
</template>

<style scoped>
.layout {
  width: 100%;
  height: 100vh;
  background: var(--el-bg-color-page);
}
.header {
  display: flex;
  align-items: center;
  padding: 8px 16px;
  border-bottom: 1px solid var(--el-border-color-light);
  background: var(--el-bg-color);
}
.title {
  font-weight: 700;
  font-size: 16px;
}
.spacer {
  flex: 1;
}
.aside {
  padding: 12px;
  border-right: 1px solid var(--el-border-color-light);
  background: var(--el-bg-color);
}
.main {
  padding: 12px;
}
.block + .block {
  margin-top: 12px;
}
.kv {
  display: flex;
  gap: 8px;
  font-size: 12px;
  margin: 2px 0;
}
.k {
  width: 88px;
  color: var(--el-text-color-secondary);
}
.v {
  flex: 1;
  word-break: break-all;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
    "Courier New", monospace;
}
.small {
  font-size: 12px;
  line-height: 1.5;
}
.mt8 {
  margin-top: 8px;
}
.btns {
  display: flex;
  gap: 8px;
}
.reply {
  white-space: pre-wrap;
  min-height: 24px;
}
.tool-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}
</style>
