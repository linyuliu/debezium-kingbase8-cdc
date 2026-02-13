<template>
  <div class="task-list">
    <h3 class="pane-title">任务列表与运行控制</h3>
    
    <div class="actions" style="margin-bottom: 8px;">
      <button @click="$emit('reloadTasks')">刷新任务</button>
      <button @click="$emit('reloadRuntime')">刷新运行态</button>
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
            <div>{{ getRuntimeText(task.id) }}</div>
            <div class="small">{{ getRuntimeMessage(task.id) }}</div>
          </td>
          <td>
            <div class="actions">
              <button @click="$emit('selectTask', task)">编辑</button>
              <button class="primary" @click="$emit('runTask', task.id, 'RESUME_CDC')">运行</button>
              <button class="warn" @click="$emit('runTask', task.id, 'FULL_THEN_CDC')">全量+CDC</button>
              <button class="warn" @click="$emit('runTask', task.id, 'FORCE_FULL_THEN_CDC')">强制全量+CDC</button>
              <button class="danger" @click="$emit('stopTask', task.id)">停止</button>
              <button @click="$emit('loadLogs', task.id)">日志</button>
              <button class="danger" @click="$emit('deleteTask', task.id)">删除</button>
            </div>
          </td>
        </tr>
        <tr v-if="!tasks.length">
          <td colspan="6" class="small">暂无任务</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
const props = defineProps({
  tasks: {
    type: Array,
    default: () => []
  },
  runtimeMap: {
    type: Object,
    default: () => ({})
  }
});

defineEmits(['reloadTasks', 'reloadRuntime', 'selectTask', 'runTask', 'stopTask', 'loadLogs', 'deleteTask']);

function getRuntimeText(taskId) {
  const runtime = props.runtimeMap[taskId];
  if (!runtime) {
    return '未启动';
  }
  return runtime.running ? '运行中' : '已停止';
}

function getRuntimeMessage(taskId) {
  const runtime = props.runtimeMap[taskId];
  return runtime?.message || '-';
}
</script>
