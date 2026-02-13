<template>
  <main class="page">
    <h1 class="page-title">Kingbase -> Doris 轻量同步控制台</h1>

    <!-- 5-Step Wizard Card -->
    <section class="card">
      <!-- Stepper Navigation -->
      <div class="stepper">
        <div
          v-for="item in stepItems"
          :key="item.step"
          class="step"
          :class="{ active: currentStep === item.step, done: currentStep > item.step }"
          @click="currentStep = item.step"
        >
          <span class="step-dot">{{ item.step }}</span>
          <span>{{ item.label }}</span>
        </div>
      </div>

      <!-- Step 1: Source/Target Setup -->
      <div v-if="currentStep === 1">
        <TaskSelector
          :form="taskForm"
          :source-data-sources="sourceDataSources"
          :target-data-sources="targetDataSources"
        />

        <div class="card" style="margin-top: 8px;">
          <DataSourceManager
            :form="dataSourceForm"
            :data-sources="dataSources"
            :data-source-types="metaOptions.dataSourceTypes"
            :message="dataSourceMessage"
            @save="saveDataSource"
            @reload="reloadDataSources"
            @test="testDataSource"
            @checkSlot="checkSlot"
            @delete="deleteDataSource"
            @select="selectDataSource"
          />
        </div>
      </div>

      <!-- Step 2: Function Configuration -->
      <FunctionConfig
        v-if="currentStep === 2"
        :form="taskForm"
        :run-modes="metaOptions.runModes"
      />

      <!-- Step 3: Table Filter & Routing -->
      <TableFilterRouting
        v-if="currentStep === 3"
        :form="taskForm"
        :route-modes="metaOptions.routeModes"
      />

      <!-- Step 4: Data Processing -->
      <DataProcessing
        v-if="currentStep === 4"
        :form="taskForm"
        :delete-sync-modes="metaOptions.deleteSyncModes"
        :output-modes="metaOptions.outputModes"
        :delta-null-strategies="metaOptions.deltaNullStrategies"
      />

      <!-- Step 5: Task Confirmation -->
      <TaskConfirmation
        v-if="currentStep === 5"
        :preview="taskPreview"
        :message="taskMessage"
        @save="saveTask"
        @reset="resetTask"
      />

      <!-- Wizard Navigation -->
      <div class="wizard-nav">
        <button :disabled="currentStep <= 1" @click="currentStep--">上一步</button>
        <button class="primary" :disabled="currentStep >= 5" @click="currentStep++">下一步</button>
      </div>
    </section>

    <!-- Task List & Log Viewer -->
    <section class="card split">
      <TaskList
        :tasks="tasks"
        :runtime-map="runtimeMap"
        @reloadTasks="reloadTasks"
        @reloadRuntime="reloadRuntime"
        @selectTask="selectTask"
        @runTask="runTask"
        @stopTask="stopTask"
        @loadLogs="loadLogs"
        @deleteTask="deleteTask"
      />

      <LogViewer
        :tasks="tasks"
        :logs-text="logsText"
        :task-id="logTaskId"
        @loadLogs="loadLogs"
      />
    </section>
  </main>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue';
import { request } from './utils/api.js';
import TaskSelector from './components/TaskSelector.vue';
import DataSourceManager from './components/DataSourceManager.vue';
import FunctionConfig from './components/FunctionConfig.vue';
import TableFilterRouting from './components/TableFilterRouting.vue';
import DataProcessing from './components/DataProcessing.vue';
import TaskConfirmation from './components/TaskConfirmation.vue';
import TaskList from './components/TaskList.vue';
import LogViewer from './components/LogViewer.vue';

// ============================================================================
// 状态管理
// ============================================================================

const stepItems = [
  { step: 1, label: '源目标设置' },
  { step: 2, label: '功能配置' },
  { step: 3, label: '表过滤与路由' },
  { step: 4, label: '数据处理' },
  { step: 5, label: '创建确认' }
];

const currentStep = ref(1);

const metaOptions = reactive({
  dataSourceTypes: [],
  runModes: [],
  routeModes: [],
  outputModes: [],
  deleteSyncModes: [],
  deltaNullStrategies: []
});

const dataSources = ref([]);
const tasks = ref([]);
const runtimeMap = reactive({});

const dataSourceMessage = reactive({ text: '', type: '' });
const taskMessage = reactive({ text: '', type: '' });

const logs = ref([]);
const logTaskId = ref('');

// ============================================================================
// 表单默认值
// ============================================================================

function createDefaultDataSourceForm() {
  return {
    id: '',
    name: '',
    type: 'KINGBASE',
    host: '',
    port: null,
    databaseName: '',
    username: '',
    password: '',
    params: ''
  };
}

function createDefaultTaskForm() {
  return {
    id: '',
    name: '',
    sourceDataSourceId: '',
    targetDataSourceId: '',
    includeTables: '',
    includeSchemas: '',
    serverName: 'kingbase_server',
    serverId: '54001',
    slotName: 'dbz_kingbase_slot',
    pluginName: 'decoderbufs',
    initSlot: true,
    recreateSlot: false,
    slotDropOnStop: false,
    replicaIdentityFull: false,
    replicaIdentityFullFailFast: false,
    replicaIdentityFullTables: '',
    routeMode: 'schema_table',
    dorisDatabase: 'cdc',
    dorisDatabasePrefix: 'cdc_',
    dorisTablePrefix: '',
    dorisTableSuffix: '',
    dorisSchemaTableSeparator: '__',
    autoCreateTargetDatabase: true,
    autoCreateTargetTable: true,
    autoAddTargetColumns: true,
    skipDeleteWithoutPk: true,
    dorisBuckets: 10,
    dorisReplicationNum: 1,
    deleteSyncMode: 'PHYSICAL_DELETE',
    logicalDeleteColumn: '__DORIS_DELETE_SIGN__',
    offsetFlushMs: 10000,
    outputMode: 'JDBC_DML',
    enhancedBatchSize: 1000,
    enhancedOutputFile: '',
    deltaNullStrategy: 'SKIP',
    changedFieldsEnabled: true,
    deltasEnabled: true,
    tombstoneAsDelete: false,
    scheduleEnabled: false,
    scheduleCron: '',
    scheduleRunMode: 'RESUME_CDC',
    truncateBeforeFull: true,
    dropBeforeFull: false,
    forceRecreateSlotOnForceRun: false,
    workDir: ''
  };
}

const dataSourceForm = reactive(createDefaultDataSourceForm());
const taskForm = reactive(createDefaultTaskForm());

// ============================================================================
// 计算属性
// ============================================================================

const sourceDataSources = computed(() => dataSources.value.filter((d) => d.type === 'KINGBASE'));
const targetDataSources = computed(() => dataSources.value.filter((d) => d.type === 'DORIS'));
const taskPreview = computed(() => JSON.stringify(taskForm, null, 2));
const logsText = computed(() => (logs.value.length ? logs.value.join('\n') : '暂无日志'));

// ============================================================================
// 数据加载函数
// ============================================================================

async function loadMetaOptions() {
  const data = await request('/api/meta/options');
  metaOptions.dataSourceTypes = data.dataSourceTypes || [];
  metaOptions.runModes = data.runModes || [];
  metaOptions.routeModes = data.routeModes || [];
  metaOptions.outputModes = data.outputModes || [];
  metaOptions.deleteSyncModes = data.deleteSyncModes || [];
  metaOptions.deltaNullStrategies = data.deltaNullStrategies || [];
}

async function reloadDataSources() {
  dataSources.value = await request('/api/datasources');
}

async function reloadTasks() {
  tasks.value = await request('/api/tasks');
  if (!logTaskId.value && tasks.value.length > 0) {
    logTaskId.value = tasks.value[0].id;
  }
}

async function reloadRuntime() {
  const list = await request('/api/tasks/runtime/all');
  Object.keys(runtimeMap).forEach((key) => delete runtimeMap[key]);
  (list || []).forEach((item) => {
    runtimeMap[item.taskId] = item;
  });
}

// ============================================================================
// 数据源操作
// ============================================================================

function resetDataSourceForm() {
  Object.assign(dataSourceForm, createDefaultDataSourceForm());
}

async function saveDataSource() {
  try {
    const payload = {
      name: dataSourceForm.name,
      type: dataSourceForm.type,
      host: dataSourceForm.host,
      port: dataSourceForm.port,
      databaseName: dataSourceForm.databaseName,
      username: dataSourceForm.username,
      password: dataSourceForm.password,
      params: dataSourceForm.params
    };

    let data;
    if (dataSourceForm.id) {
      data = await request(`/api/datasources/${dataSourceForm.id}`, { method: 'PUT', body: payload });
      dataSourceMessage.text = `数据源更新成功: ${data.name}`;
    } else {
      data = await request('/api/datasources', { method: 'POST', body: payload });
      dataSourceMessage.text = `数据源创建成功: ${data.name}`;
    }

    dataSourceMessage.type = 'ok';
    dataSourceForm.id = data.id;
    await reloadDataSources();
  } catch (error) {
    dataSourceMessage.text = error.message;
    dataSourceMessage.type = 'err';
  }
}

async function deleteDataSource() {
  if (!dataSourceForm.id) {
    dataSourceMessage.text = '请先选择要删除的数据源';
    dataSourceMessage.type = 'err';
    return;
  }

  if (!window.confirm(`确认删除数据源 ${dataSourceForm.id} 吗？`)) {
    return;
  }

  try {
    await request(`/api/datasources/${dataSourceForm.id}`, { method: 'DELETE' });
    dataSourceMessage.text = '数据源删除成功';
    dataSourceMessage.type = 'ok';
    resetDataSourceForm();
    await reloadDataSources();
  } catch (error) {
    dataSourceMessage.text = error.message;
    dataSourceMessage.type = 'err';
  }
}

async function testDataSource() {
  if (!dataSourceForm.id) {
    dataSourceMessage.text = '请先保存数据源，再执行连接测试';
    dataSourceMessage.type = 'err';
    return;
  }

  try {
    const result = await request(`/api/datasources/${dataSourceForm.id}/test`, { method: 'POST' });
    dataSourceMessage.text = result.ok
      ? `连接成功，耗时 ${result.latencyMs}ms`
      : `连接失败: ${result.message}`;
    dataSourceMessage.type = result.ok ? 'ok' : 'err';
  } catch (error) {
    dataSourceMessage.text = error.message;
    dataSourceMessage.type = 'err';
  }
}

async function checkSlot() {
  if (!dataSourceForm.id) {
    dataSourceMessage.text = '请先保存并选择一个 KINGBASE 数据源';
    dataSourceMessage.type = 'err';
    return;
  }

  const slotName = taskForm.slotName || 'dbz_kingbase_slot';
  try {
    const result = await request(`/api/datasources/${dataSourceForm.id}/slot-check?slotName=${encodeURIComponent(slotName)}`, { method: 'POST' });
    if (result.ok && result.exists) {
      dataSourceMessage.text = `Slot 存在: ${result.slotName}, plugin=${result.plugin}, active=${result.active}`;
      dataSourceMessage.type = 'ok';
      return;
    }
    if (result.ok && !result.exists) {
      dataSourceMessage.text = `Slot 不存在: ${slotName}`;
      dataSourceMessage.type = 'err';
      return;
    }

    dataSourceMessage.text = result.message || 'Slot 检查失败';
    dataSourceMessage.type = 'err';
  } catch (error) {
    dataSourceMessage.text = error.message;
    dataSourceMessage.type = 'err';
  }
}

function selectDataSource(row) {
  Object.assign(dataSourceForm, createDefaultDataSourceForm(), JSON.parse(JSON.stringify(row)));
  dataSourceMessage.text = `已载入数据源 ${row.name}`;
  dataSourceMessage.type = 'ok';
}

// ============================================================================
// 任务操作
// ============================================================================

function resetTask() {
  Object.assign(taskForm, createDefaultTaskForm());
  taskMessage.text = '任务表单已重置';
  taskMessage.type = 'ok';
}

async function saveTask() {
  try {
    const payload = JSON.parse(JSON.stringify(taskForm));
    payload.routeMode = String(payload.routeMode || '').toLowerCase();

    let data;
    if (taskForm.id) {
      data = await request(`/api/tasks/${taskForm.id}`, { method: 'PUT', body: payload });
      taskMessage.text = `任务更新成功: ${data.name}`;
    } else {
      data = await request('/api/tasks', { method: 'POST', body: payload });
      taskMessage.text = `任务创建成功: ${data.name}`;
    }
    taskMessage.type = 'ok';

    Object.assign(taskForm, createDefaultTaskForm(), data);
    await reloadTasks();
    await reloadRuntime();
  } catch (error) {
    taskMessage.text = error.message;
    taskMessage.type = 'err';
  }
}

function selectTask(task) {
  Object.assign(taskForm, createDefaultTaskForm(), JSON.parse(JSON.stringify(task)));
  currentStep.value = 1;
  taskMessage.text = `已载入任务 ${task.name}`;
  taskMessage.type = 'ok';
}

async function deleteTask(taskId) {
  if (!window.confirm(`确认删除任务 ${taskId} 吗？`)) {
    return;
  }
  try {
    await request(`/api/tasks/${taskId}`, { method: 'DELETE' });
    taskMessage.text = '任务删除成功';
    taskMessage.type = 'ok';
    if (taskForm.id === taskId) {
      resetTask();
    }
    await reloadTasks();
    await reloadRuntime();
  } catch (error) {
    taskMessage.text = error.message;
    taskMessage.type = 'err';
  }
}

async function runTask(taskId, mode) {
  try {
    await request(`/api/tasks/${taskId}/run?mode=${encodeURIComponent(mode)}`, { method: 'POST' });
    taskMessage.text = `任务已触发运行: ${taskId} (${mode})`;
    taskMessage.type = 'ok';
    await reloadRuntime();
  } catch (error) {
    taskMessage.text = error.message;
    taskMessage.type = 'err';
  }
}

async function stopTask(taskId) {
  try {
    await request(`/api/tasks/${taskId}/stop`, { method: 'POST' });
    taskMessage.text = `任务已请求停止: ${taskId}`;
    taskMessage.type = 'ok';
    await reloadRuntime();
  } catch (error) {
    taskMessage.text = error.message;
    taskMessage.type = 'err';
  }
}

async function loadLogs(taskId) {
  if (!taskId) {
    return;
  }
  try {
    logTaskId.value = taskId;
    logs.value = await request(`/api/tasks/${taskId}/logs?tail=300`);
  } catch (error) {
    logs.value = [`加载日志失败: ${error.message}`];
  }
}

// ============================================================================
// 生命周期
// ============================================================================

onMounted(async () => {
  try {
    await loadMetaOptions();
    await reloadDataSources();
    await reloadTasks();
    await reloadRuntime();
  } catch (error) {
    taskMessage.text = error.message;
    taskMessage.type = 'err';
  }
});
</script>
