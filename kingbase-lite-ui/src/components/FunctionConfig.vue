<template>
  <div class="function-config">
    <h3 class="pane-title">Step 2 - 功能配置</h3>
    <p class="hint">配置复制槽、运行模式和定时策略。</p>

    <!-- Replication Slot Configuration -->
    <div class="grid">
      <label>
        复制槽名
        <input v-model="form.slotName" />
      </label>
      <label>
        解码插件
        <input v-model="form.pluginName" />
      </label>
      <label>
        serverName
        <input v-model="form.serverName" />
      </label>
      <label>
        serverId
        <input v-model="form.serverId" />
      </label>
    </div>

    <!-- Slot Options -->
    <div class="grid">
      <label>
        初始化 Slot
        <select v-model="form.initSlot">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
      <label>
        启动前重建 Slot
        <select v-model="form.recreateSlot">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        进程退出后删除 Slot
        <select v-model="form.slotDropOnStop">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        手动运行默认模式
        <select v-model="form.scheduleRunMode">
          <option v-for="option in runModes" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
    </div>

    <!-- Schedule Configuration -->
    <div class="grid">
      <label>
        启用定时调度
        <select v-model="form.scheduleEnabled">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        Cron 表达式
        <input v-model="form.scheduleCron" placeholder="0 0 2 * * ?" />
      </label>
      <label>
        offset 刷新间隔(ms)
        <input v-model.number="form.offsetFlushMs" type="number" />
      </label>
      <label>
        工作目录（可选）
        <input v-model="form.workDir" placeholder="留空用默认目录" />
      </label>
    </div>
  </div>
</template>

<script setup>
defineProps({
  form: {
    type: Object,
    required: true
  },
  runModes: {
    type: Array,
    default: () => []
  }
});
</script>
