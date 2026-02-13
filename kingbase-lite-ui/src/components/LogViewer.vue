<template>
  <div class="log-viewer">
    <h3 class="pane-title">运行日志</h3>
    
    <div class="actions" style="margin-bottom: 8px;">
      <label style="max-width: 160px;">
        日志任务 ID
        <select v-model="selectedTaskId">
          <option value="">请选择</option>
          <option v-for="task in tasks" :key="task.id" :value="task.id">
            {{ task.name }} ({{ task.id }})
          </option>
        </select>
      </label>
      <button @click="$emit('loadLogs', selectedTaskId)" :disabled="!selectedTaskId">
        刷新日志
      </button>
    </div>
    
    <div class="logbox">{{ logsText }}</div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue';

const props = defineProps({
  tasks: {
    type: Array,
    default: () => []
  },
  logsText: {
    type: String,
    default: '暂无日志'
  },
  taskId: {
    type: String,
    default: ''
  }
});

defineEmits(['loadLogs']);

const selectedTaskId = ref(props.taskId);

watch(() => props.taskId, (newVal) => {
  selectedTaskId.value = newVal;
});
</script>
