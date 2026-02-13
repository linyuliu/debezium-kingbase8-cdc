<template>
  <div class="data-source-manager">
    <p class="hint">数据源管理（增删改查 / 测试连接 / Slot 检查）</p>
    
    <!-- Data Source Form -->
    <div class="grid">
      <label>
        数据源 ID（编辑时自动带出）
        <input v-model="form.id" placeholder="创建可留空" />
      </label>
      <label>
        名称
        <input v-model="form.name" placeholder="kb-dev" />
      </label>
      <label>
        类型
        <select v-model="form.type">
          <option value="">请选择</option>
          <option v-for="option in dataSourceTypes" :key="option.code" :value="option.code">
            {{ option.label }}
          </option>
        </select>
      </label>
      <label>
        主机
        <input v-model="form.host" placeholder="127.0.0.1" />
      </label>
    </div>
    
    <div class="grid">
      <label>
        端口
        <input v-model.number="form.port" type="number" placeholder="54321 / 9030" />
      </label>
      <label>
        数据库
        <input v-model="form.databaseName" placeholder="test / cdc" />
      </label>
      <label>
        用户名
        <input v-model="form.username" placeholder="system / root" />
      </label>
      <label>
        密码
        <input v-model="form.password" type="password" placeholder="可为空" />
      </label>
    </div>
    
    <div class="grid">
      <label style="grid-column: span 4;">
        附加参数 (params)
        <input v-model="form.params" placeholder="connectTimeout=5000&socketTimeout=30000" />
      </label>
    </div>

    <!-- Actions -->
    <div class="actions">
      <button class="primary" @click="$emit('save')">保存数据源</button>
      <button @click="$emit('reload')">刷新列表</button>
      <button @click="$emit('test')">测试连接</button>
      <button @click="$emit('checkSlot')">检查 Slot</button>
      <button class="danger" @click="$emit('delete')">删除数据源</button>
    </div>
    
    <!-- Message -->
    <p v-if="message.text" class="msg" :class="messageClass(message.type)">
      {{ message.text }}
    </p>

    <!-- Data Sources Table -->
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
          <td>{{ getTypeLabel(row.type) }}</td>
          <td>{{ row.host }}:{{ row.port }}</td>
          <td>{{ row.databaseName }}</td>
          <td>
            <button @click="$emit('select', row)">编辑</button>
          </td>
        </tr>
        <tr v-if="!dataSources.length">
          <td colspan="6" class="small">暂无数据源</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  form: {
    type: Object,
    required: true
  },
  dataSources: {
    type: Array,
    default: () => []
  },
  dataSourceTypes: {
    type: Array,
    default: () => []
  },
  message: {
    type: Object,
    default: () => ({ text: '', type: '' })
  }
});

defineEmits(['save', 'reload', 'test', 'checkSlot', 'delete', 'select']);

function messageClass(type) {
  return type === 'ok' ? 'ok' : type === 'err' ? 'err' : '';
}

function getTypeLabel(code) {
  return props.dataSourceTypes.find((item) => item.code === code)?.label || code || '';
}
</script>
