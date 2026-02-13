<template>
  <div class="data-processing">
    <h3 class="pane-title">Step 4 - 数据处理策略</h3>
    <p class="hint">控制 Doris 自动建模、删除策略、增强 JSON 输出与增量字段差异计算。</p>

    <!-- Auto-Create Options -->
    <div class="grid">
      <label>
        自动建库
        <select v-model="form.autoCreateTargetDatabase">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
      <label>
        自动建表
        <select v-model="form.autoCreateTargetTable">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
      <label>
        自动补列
        <select v-model="form.autoAddTargetColumns">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
      <label>
        无主键删除事件跳过
        <select v-model="form.skipDeleteWithoutPk">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
    </div>

    <!-- Delete and Output Modes -->
    <div class="grid">
      <label>
        删除同步模式
        <select v-model="form.deleteSyncMode">
          <option v-for="option in deleteSyncModes" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
      <label>
        输出模式
        <select v-model="form.outputMode">
          <option v-for="option in outputModes" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
      <label>
        Delta 空值策略
        <select v-model="form.deltaNullStrategy">
          <option v-for="option in deltaNullStrategies" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
      <label>
        tombstone 视作删除
        <select v-model="form.tombstoneAsDelete">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
    </div>

    <!-- Enhanced Output Options -->
    <div class="grid">
      <label>
        changed_fields 输出
        <select v-model="form.changedFieldsEnabled">
          <option :value="true">开启</option>
          <option :value="false">关闭</option>
        </select>
      </label>
      <label>
        deltas 输出
        <select v-model="form.deltasEnabled">
          <option :value="true">开启</option>
          <option :value="false">关闭</option>
        </select>
      </label>
      <label>
        增强 JSON 批大小
        <input v-model.number="form.enhancedBatchSize" type="number" min="1" />
      </label>
      <label>
        逻辑删除标记列
        <input
          v-model="form.logicalDeleteColumn"
          :disabled="form.deleteSyncMode !== 'LOGICAL_DELETE_SIGN'"
          placeholder="__DORIS_DELETE_SIGN__"
        />
      </label>
    </div>

    <div class="grid-2">
      <label>
        增强 JSON 输出文件（可选）
        <input v-model="form.enhancedOutputFile" placeholder="/tmp/enhanced-cdc.jsonl" />
      </label>
    </div>

    <!-- Full Sync Options -->
    <div class="grid">
      <label>
        全量前 truncate
        <select v-model="form.truncateBeforeFull">
          <option :value="true">是</option>
          <option :value="false">否</option>
        </select>
      </label>
      <label>
        全量前 drop
        <select v-model="form.dropBeforeFull">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        强制全量时重建 Slot
        <select v-model="form.forceRecreateSlotOnForceRun">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        Doris Buckets
        <input v-model.number="form.dorisBuckets" type="number" min="1" />
      </label>
    </div>

    <div class="grid-2">
      <label>
        Doris Replication Num
        <input v-model.number="form.dorisReplicationNum" type="number" min="1" />
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
  deleteSyncModes: {
    type: Array,
    default: () => []
  },
  outputModes: {
    type: Array,
    default: () => []
  },
  deltaNullStrategies: {
    type: Array,
    default: () => []
  }
});
</script>
