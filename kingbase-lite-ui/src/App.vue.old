<template>
  <main class="page">
    <h1 class="page-title">Kingbase -> Doris 轻量同步控制台</h1>

    <section class="card">
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

      <div v-if="currentStep === 1">
        <h3 class="pane-title">Step 1 - 源目标设置</h3>
        <p class="hint">先配置并测试数据源，再选择任务的源/目标数据源。</p>

        <div class="grid">
          <label>
            任务 ID（编辑时自动带出）
            <input v-model="taskForm.id" placeholder="创建任务可留空" />
          </label>
          <label>
            任务名称
            <input v-model="taskForm.name" placeholder="kb_to_doris_demo" />
          </label>
          <label>
            源数据源
            <select v-model="taskForm.sourceDataSourceId">
              <option value="">请选择</option>
              <option v-for="item in sourceDataSources" :key="item.id" :value="item.id">
                {{ item.name }} ({{ item.id }})
              </option>
            </select>
          </label>
          <label>
            目标数据源
            <select v-model="taskForm.targetDataSourceId">
              <option value="">请选择</option>
              <option v-for="item in targetDataSources" :key="item.id" :value="item.id">
                {{ item.name }} ({{ item.id }})
              </option>
            </select>
          </label>
        </div>

        <div class="card" style="margin-top: 8px;">
          <p class="hint">数据源管理（增删改查 / 测试连接 / Slot 检查）</p>
          <div class="grid">
            <label>
              数据源 ID（编辑时自动带出）
              <input v-model="dataSourceForm.id" placeholder="创建可留空" />
            </label>
            <label>
              名称
              <input v-model="dataSourceForm.name" placeholder="kb-dev" />
            </label>
            <label>
              类型
              <select v-model="dataSourceForm.type">
                <option value="">请选择</option>
                <option v-for="option in metaOptions.dataSourceTypes" :key="option.code" :value="option.code">
                  {{ option.label }}
                </option>
              </select>
            </label>
            <label>
              主机
              <input v-model="dataSourceForm.host" placeholder="127.0.0.1" />
            </label>
          </div>
          <div class="grid">
            <label>
              端口
              <input v-model.number="dataSourceForm.port" type="number" placeholder="54321 / 9030" />
            </label>
            <label>
              数据库
              <input v-model="dataSourceForm.databaseName" placeholder="test / cdc" />
            </label>
            <label>
              用户名
              <input v-model="dataSourceForm.username" placeholder="system / root" />
            </label>
            <label>
              密码
              <input v-model="dataSourceForm.password" type="password" placeholder="可为空" />
            </label>
          </div>
          <div class="grid">
            <label style="grid-column: span 4;">
              附加参数 (params)
              <input v-model="dataSourceForm.params" placeholder="connectTimeout=5000&socketTimeout=30000" />
            </label>
          </div>

          <div class="actions">
            <button class="primary" @click="saveDataSource">保存数据源</button>
            <button @click="reloadDataSources">刷新列表</button>
            <button @click="testDataSource">测试连接</button>
            <button @click="checkSlot">检查 Slot</button>
            <button class="danger" @click="deleteDataSource">删除数据源</button>
          </div>
          <p class="msg" :class="messageClass(dataSourceMessage.type)">{{ dataSourceMessage.text }}</p>

          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>名称</th>
                <th>类型</th>
                <th>地址</th>
                <th>数据库</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in dataSources" :key="row.id">
                <td>{{ row.id }}</td>
                <td>{{ row.name }}</td>
                <td>{{ typeLabel(row.type) }}</td>
                <td>{{ row.host }}:{{ row.port }}</td>
                <td>{{ row.databaseName }}</td>
                <td>
                  <button @click="selectDataSource(row)">编辑</button>
                </td>
              </tr>
              <tr v-if="!dataSources.length">
                <td colspan="6" class="small">暂无数据源</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div v-if="currentStep === 2">
        <h3 class="pane-title">Step 2 - 功能配置</h3>
        <p class="hint">配置复制槽、运行模式和定时策略。</p>

        <div class="grid">
          <label>
            复制槽名
            <input v-model="taskForm.slotName" />
          </label>
          <label>
            解码插件
            <input v-model="taskForm.pluginName" />
          </label>
          <label>
            serverName
            <input v-model="taskForm.serverName" />
          </label>
          <label>
            serverId
            <input v-model="taskForm.serverId" />
          </label>
        </div>

        <div class="grid">
          <label>
            初始化 Slot
            <select v-model="taskForm.initSlot">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
          <label>
            启动前重建 Slot
            <select v-model="taskForm.recreateSlot">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            进程退出后删除 Slot
            <select v-model="taskForm.slotDropOnStop">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            手动运行默认模式
            <select v-model="taskForm.scheduleRunMode">
              <option v-for="option in metaOptions.runModes" :key="option.code" :value="option.code">
                {{ option.label }}
              </option>
            </select>
          </label>
        </div>

        <div class="grid">
          <label>
            启用定时调度
            <select v-model="taskForm.scheduleEnabled">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            Cron 表达式
            <input v-model="taskForm.scheduleCron" placeholder="0 0 2 * * ?" />
          </label>
          <label>
            offset 刷新间隔(ms)
            <input v-model.number="taskForm.offsetFlushMs" type="number" />
          </label>
          <label>
            工作目录（可选）
            <input v-model="taskForm.workDir" placeholder="留空用默认目录" />
          </label>
        </div>
      </div>

      <div v-if="currentStep === 3">
        <h3 class="pane-title">Step 3 - 表过滤与路由</h3>
        <p class="hint">过滤范围至少填写一个：表清单或 schema 清单。</p>

        <div class="grid">
          <label style="grid-column: span 2;">
            includeTables（逗号分隔）
            <input v-model="taskForm.includeTables" placeholder="form.t_order,form.t_user" />
          </label>
          <label style="grid-column: span 2;">
            includeSchemas（逗号分隔）
            <input v-model="taskForm.includeSchemas" placeholder="form,public" />
          </label>
        </div>

        <div class="grid">
          <label>
            路由模式
            <select v-model="taskForm.routeMode">
              <option v-for="option in metaOptions.routeModes" :key="option.code" :value="option.code">
                {{ option.label }}
              </option>
            </select>
          </label>
          <label>
            Doris 目标库
            <input v-model="taskForm.dorisDatabase" placeholder="cdc" />
          </label>
          <label>
            Doris 库名前缀
            <input v-model="taskForm.dorisDatabasePrefix" placeholder="cdc_" />
          </label>
          <label>
            Schema 与表连接符
            <input v-model="taskForm.dorisSchemaTableSeparator" placeholder="__" />
          </label>
        </div>

        <div class="grid">
          <label>
            表名前缀
            <input v-model="taskForm.dorisTablePrefix" />
          </label>
          <label>
            表名后缀
            <input v-model="taskForm.dorisTableSuffix" />
          </label>
          <label>
            REPLICA IDENTITY FULL
            <select v-model="taskForm.replicaIdentityFull">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            FULL 设置失败立即终止
            <select v-model="taskForm.replicaIdentityFullFailFast">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
        </div>

        <label>
          REPLICA IDENTITY FULL 指定表（逗号分隔，schema.table）
          <textarea v-model="taskForm.replicaIdentityFullTables" placeholder="form.t_order,form.t_user"></textarea>
        </label>
      </div>

      <div v-if="currentStep === 4">
        <h3 class="pane-title">Step 4 - 数据处理策略</h3>
        <p class="hint">控制 Doris 自动建模、删除策略、增强 JSON 输出与增量字段差异计算。</p>

        <div class="grid">
          <label>
            自动建库
            <select v-model="taskForm.autoCreateTargetDatabase">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
          <label>
            自动建表
            <select v-model="taskForm.autoCreateTargetTable">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
          <label>
            自动补列
            <select v-model="taskForm.autoAddTargetColumns">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
          <label>
            无主键删除事件跳过
            <select v-model="taskForm.skipDeleteWithoutPk">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
        </div>

        <div class="grid">
          <label>
            删除同步模式
            <select v-model="taskForm.deleteSyncMode">
              <option v-for="option in metaOptions.deleteSyncModes" :key="option.code" :value="option.code">
                {{ option.label }}
              </option>
            </select>
          </label>
          <label>
            输出模式
            <select v-model="taskForm.outputMode">
              <option v-for="option in metaOptions.outputModes" :key="option.code" :value="option.code">
                {{ option.label }}
              </option>
            </select>
          </label>
          <label>
            Delta 空值策略
            <select v-model="taskForm.deltaNullStrategy">
              <option v-for="option in metaOptions.deltaNullStrategies" :key="option.code" :value="option.code">
                {{ option.label }}
              </option>
            </select>
          </label>
          <label>
            tombstone 视作删除
            <select v-model="taskForm.tombstoneAsDelete">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
        </div>

        <div class="grid">
          <label>
            changed_fields 输出
            <select v-model="taskForm.changedFieldsEnabled">
              <option :value="true">开启</option>
              <option :value="false">关闭</option>
            </select>
          </label>
          <label>
            deltas 输出
            <select v-model="taskForm.deltasEnabled">
              <option :value="true">开启</option>
              <option :value="false">关闭</option>
            </select>
          </label>
          <label>
            增强 JSON 批大小
            <input v-model.number="taskForm.enhancedBatchSize" type="number" min="1" />
          </label>
          <label>
            逻辑删除标记列
            <input
              v-model="taskForm.logicalDeleteColumn"
              :disabled="taskForm.deleteSyncMode !== 'LOGICAL_DELETE_SIGN'"
              placeholder="__DORIS_DELETE_SIGN__"
            />
          </label>
        </div>

        <div class="grid-2">
          <label>
            增强 JSON 输出文件（可选）
            <input v-model="taskForm.enhancedOutputFile" placeholder="/tmp/enhanced-cdc.jsonl" />
          </label>
        </div>

        <div class="grid">
          <label>
            全量前 truncate
            <select v-model="taskForm.truncateBeforeFull">
              <option :value="true">是</option>
              <option :value="false">否</option>
            </select>
          </label>
          <label>
            全量前 drop
            <select v-model="taskForm.dropBeforeFull">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            强制全量时重建 Slot
            <select v-model="taskForm.forceRecreateSlotOnForceRun">
              <option :value="false">否</option>
              <option :value="true">是</option>
            </select>
          </label>
          <label>
            Doris Buckets
            <input v-model.number="taskForm.dorisBuckets" type="number" min="1" />
          </label>
        </div>

        <div class="grid-2">
          <label>
            Doris Replication Num
            <input v-model.number="taskForm.dorisReplicationNum" type="number" min="1" />
          </label>
        </div>
      </div>

      <div v-if="currentStep === 5">
        <h3 class="pane-title">Step 5 - 创建确认</h3>
        <p class="hint">确认配置后保存任务，任务会自动出现在下方任务列表。</p>
        <div class="json-preview">{{ taskPreview }}</div>
        <div class="actions" style="margin-top: 10px;">
          <button class="primary" @click="saveTask">保存任务</button>
          <button @click="resetTask">重置表单</button>
        </div>
        <p class="msg" :class="messageClass(taskMessage.type)">{{ taskMessage.text }}</p>
      </div>

      <div class="wizard-nav">
        <button :disabled="currentStep <= 1" @click="currentStep--">上一步</button>
        <button class="primary" :disabled="currentStep >= 5" @click="currentStep++">下一步</button>
      </div>
    </section>

    <section class="card split">
      <div>
        <h3 class="pane-title">任务列表与运行控制</h3>
        <div class="actions" style="margin-bottom: 8px;">
          <button @click="reloadTasks">刷新任务</button>
          <button @click="reloadRuntime">刷新运行态</button>
        </div>

        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>名称</th>
              <th>源->目标</th>
              <th>调度</th>
              <th>状态</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="task in tasks" :key="task.id">
              <td>{{ task.id }}</td>
              <td>{{ task.name }}</td>
              <td>{{ task.sourceDataSourceId }} -> {{ task.targetDataSourceId }}</td>
              <td>{{ task.scheduleEnabled ? ('开启: ' + (task.scheduleCron || '-')) : '关闭' }}</td>
              <td>
                <div>{{ runtimeText(task.id) }}</div>
                <div class="small">{{ runtimeMessage(task.id) }}</div>
              </td>
              <td>
                <div class="actions">
                  <button @click="selectTask(task)">编辑</button>
                  <button class="primary" @click="runTask(task.id, 'RESUME_CDC')">运行</button>
                  <button class="warn" @click="runTask(task.id, 'FULL_THEN_CDC')">全量+CDC</button>
                  <button class="warn" @click="runTask(task.id, 'FORCE_FULL_THEN_CDC')">强制全量+CDC</button>
                  <button class="danger" @click="stopTask(task.id)">停止</button>
                  <button @click="loadLogs(task.id)">日志</button>
                  <button class="danger" @click="deleteTask(task.id)">删除</button>
                </div>
              </td>
            </tr>
            <tr v-if="!tasks.length">
              <td colspan="6" class="small">暂无任务</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div>
        <h3 class="pane-title">运行日志</h3>
        <div class="actions" style="margin-bottom: 8px;">
          <label style="max-width: 160px;">
            日志任务 ID
            <select v-model="logTaskId">
              <option value="">请选择</option>
              <option v-for="task in tasks" :key="task.id" :value="task.id">{{ task.name }} ({{ task.id }})</option>
            </select>
          </label>
          <button @click="loadLogs(logTaskId)" :disabled="!logTaskId">刷新日志</button>
        </div>
        <div class="logbox">{{ logsText }}</div>
      </div>
    </section>
  </main>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue';

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

const sourceDataSources = computed(() => dataSources.value.filter((d) => d.type === 'KINGBASE'));
const targetDataSources = computed(() => dataSources.value.filter((d) => d.type === 'DORIS'));
const taskPreview = computed(() => JSON.stringify(taskForm, null, 2));
const logsText = computed(() => (logs.value.length ? logs.value.join('\n') : '暂无日志'));

function messageClass(type) {
  if (type === 'ok') {
    return 'ok';
  }
  if (type === 'err') {
    return 'err';
  }
  return '';
}

function resetDataSourceForm() {
  Object.assign(dataSourceForm, createDefaultDataSourceForm());
}

function resetTask() {
  Object.assign(taskForm, createDefaultTaskForm());
  taskMessage.text = '任务表单已重置';
  taskMessage.type = 'ok';
}

function typeLabel(code) {
  return metaOptions.dataSourceTypes.find((item) => item.code === code)?.label || code || '';
}

function runtimeText(taskId) {
  const runtime = runtimeMap[taskId];
  if (!runtime) {
    return '未启动';
  }
  return runtime.running ? '运行中' : '已停止';
}

function runtimeMessage(taskId) {
  const runtime = runtimeMap[taskId];
  return runtime?.message || '-';
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    throw new Error(`接口返回无法解析: ${response.status}`);
  }

  if (!response.ok || !payload.ok) {
    throw new Error(payload.message || `请求失败: ${response.status}`);
  }
  return payload.data;
}

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
