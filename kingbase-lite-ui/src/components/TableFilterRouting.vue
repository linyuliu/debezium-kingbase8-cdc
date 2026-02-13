<template>
  <div class="table-filter-routing">
    <h3 class="pane-title">Step 3 - 表过滤与路由</h3>
    <p class="hint">过滤范围至少填写一个：表清单或 schema 清单。</p>

    <!-- Table/Schema Filter -->
    <div class="grid">
      <label style="grid-column: span 2;">
        includeTables（逗号分隔）
        <input v-model="form.includeTables" placeholder="form.t_order,form.t_user" />
      </label>
      <label style="grid-column: span 2;">
        includeSchemas（逗号分隔）
        <input v-model="form.includeSchemas" placeholder="form,public" />
      </label>
    </div>

    <!-- Routing Configuration -->
    <div class="grid">
      <label>
        路由模式
        <select v-model="form.routeMode">
          <option v-for="option in routeModes" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
      <label>
        Doris 目标库
        <input v-model="form.dorisDatabase" placeholder="cdc" />
      </label>
      <label>
        Doris 库名前缀
        <input v-model="form.dorisDatabasePrefix" placeholder="cdc_" />
      </label>
      <label>
        Schema 与表连接符
        <input v-model="form.dorisSchemaTableSeparator" placeholder="__" />
      </label>
    </div>

    <!-- Additional Options -->
    <div class="grid">
      <label>
        表名前缀
        <input v-model="form.dorisTablePrefix" />
      </label>
      <label>
        表名后缀
        <input v-model="form.dorisTableSuffix" />
      </label>
      <label>
        REPLICA IDENTITY FULL
        <select v-model="form.replicaIdentityFull">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
      <label>
        FULL 设置失败立即终止
        <select v-model="form.replicaIdentityFullFailFast">
          <option :value="false">否</option>
          <option :value="true">是</option>
        </select>
      </label>
    </div>

    <label>
      REPLICA IDENTITY FULL 指定表（逗号分隔，schema.table）
      <textarea v-model="form.replicaIdentityFullTables" placeholder="form.t_order,form.t_user"></textarea>
    </label>
  </div>
</template>

<script setup>
defineProps({
  form: {
    type: Object,
    required: true
  },
  routeModes: {
    type: Array,
    default: () => []
  }
});
</script>
