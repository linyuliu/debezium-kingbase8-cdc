<template>
  <div class="task-confirmation">
    <h3 class="pane-title">Step 5 - 创建确认</h3>
    <p class="hint">确认配置后保存任务，任务会自动出现在下方任务列表。</p>
    
    <div class="json-preview">{{ preview }}</div>
    
    <div class="actions" style="margin-top: 10px;">
      <button class="primary" @click="$emit('save')">保存任务</button>
      <button @click="$emit('reset')">重置表单</button>
    </div>
    
    <p v-if="message.text" class="msg" :class="messageClass(message.type)">
      {{ message.text }}
    </p>
  </div>
</template>

<script setup>
defineProps({
  preview: {
    type: String,
    required: true
  },
  message: {
    type: Object,
    default: () => ({ text: '', type: '' })
  }
});

defineEmits(['save', 'reset']);

function messageClass(type) {
  return type === 'ok' ? 'ok' : type === 'err' ? 'err' : '';
}
</script>
