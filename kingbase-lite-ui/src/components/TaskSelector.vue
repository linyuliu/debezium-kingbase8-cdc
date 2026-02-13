<template>
  <div class="task-selector">
    <h3 class="pane-title">Step 1 - 源目标设置</h3>
    <p class="hint">先配置并测试数据源，再选择任务的源/目标数据源。</p>

    <div class="grid">
      <label>
        任务 ID（编辑时自动带出）
        <input v-model="form.id" placeholder="创建任务可留空" />
      </label>
      <label>
        任务名称
        <input v-model="form.name" placeholder="kb_to_doris_demo" />
      </label>
      <label>
        源数据源
        <select v-model="form.sourceDataSourceId">
          <option value="">请选择</option>
          <option v-for="item in sourceDataSources" :key="item.id" :value="item.id">
            {{ item.name }} ({{ item.id }})
          </option>
        </select>
      </label>
      <label>
        目标数据源
        <select v-model="form.targetDataSourceId">
          <option value="">请选择</option>
          <option v-for="item in targetDataSources" :key="item.id" :value="item.id">
            {{ item.name }} ({{ item.id }})
          </option>
        </select>
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
  sourceDataSources: {
    type: Array,
    default: () => []
  },
  targetDataSources: {
    type: Array,
    default: () => []
  }
});
</script>
