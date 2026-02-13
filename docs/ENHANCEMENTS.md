# Debezium Kingbase CDC 增强说明

本文档记录了代码库的关键增强和优化内容。

## 1. Proto 协议增强 ✅

### 新增功能
- **原始 WAL 包日志**：完整记录接收到的 proto 包原始信息
  - 启用方式：`KB_DEBUG_RAW_WAL=true` 或系统属性 `-Dkb.debug.rawwal=true`
  - 输出前 96 字节预览和完整 16 进制转储
  
- **详细解析日志**：逐帧输出 proto 解析详情
  - 启用方式：`KB_DEBUG_PROTO_PARSE=true` 或 `-Dkb.debug.proto.parse=true`
  - 包含：帧类型、事务ID、操作类型、表名、列数量等
  
- **列级详情输出**：输出每列的名称和类型信息
  - 便于逆向分析不同 Kingbase 版本的差异
  - 帮助诊断版本兼容性问题

- **严格解析模式**：让解析失败直接抛错而非跳过
  - 启用方式：`KB_STRICT_PROTO_PARSE=true` 或 `-Dkb.strict.proto.parse=true`

### 文件修改
- `PgProtoMessageDecoder.java`
  - 新增 `logColumnDetails()` 方法输出列详情
  - 增强 `logRawMessage()` 提供完整转储选项
  - 改进异常日志格式

## 2. OID 验证增强 ✅

### 新增功能
- **OID 来源文档化**
  - 明确标注：PostgreSQL 官方定义 (pg_type.h)
  - Kingbase8 JDBC 驱动 8.6.1 版本
  - decoderbufs 插件逆向工程

- **OID 验证机制**
  - 启用方式：`KB_VALIDATE_OID=true` 或 `-Dkb.validate.oid=true`
  - 自动检查扩展 OID 与基类定义的冲突
  - 反射验证基类中的 OID 定义

- **未知 OID 检测**
  - `getKnownTypeName(int oid)` - 查询已知类型名称
  - `logUnknownOid(int oid, String columnName)` - 记录未知 OID

### 文件修改
- `PgOid.java`
  - 新增 OID 映射表 (KNOWN_OIDS)
  - 新增 OID 验证方法 `validateOids()` 和 `checkOidConflict()`
  - 添加详细 Javadoc 文档

## 3. 依赖清理 ✅

### 移除的依赖
- `mysql-connector-j` - 未在代码库中使用
  - 从 `kingbase-connector-core/pom.xml` 移除

## 4. UI 重构 ✅

### 组件化架构
原 `App.vue` (907 行单体文件) 已重构为：

#### 新组件
1. **TaskSelector.vue** - Step 1 任务源目标选择
2. **DataSourceManager.vue** - 数据源管理 (增删改查/测试/Slot检查)
3. **FunctionConfig.vue** - Step 2 复制槽功能配置
4. **TableFilterRouting.vue** - Step 3 表过滤与路由规则
5. **DataProcessing.vue** - Step 4 数据处理策略
6. **TaskConfirmation.vue** - Step 5 JSON 预览确认
7. **TaskList.vue** - 任务列表与运行控制
8. **LogViewer.vue** - 日志查看器

#### 工具函数
- `utils/api.js` - 统一 API 请求函数

### 改进点
- **关注点分离**：每个组件专注单一职责
- **可维护性**：代码组织清晰，易于修改和测试
- **可复用性**：组件和工具函数可在其他地方复用
- **代码导航**：清晰的分区注释便于快速定位

## 5. 日志优化 ✅

### Proto 协议日志
- 统一使用 `[Proto解析]` 前缀标签
- 分级输出：TRACE < DEBUG < INFO < WARN
- 环境变量控制详细程度

### OID 验证日志
- 使用 `[OID验证]` 前缀标签
- 警告级别标记不匹配和未知 OID
- 启动时输出验证状态

## 6. 中文注释优化 ✅

### 现状
- **Java 核心代码**：注释以英文为主（符合国际化最佳实践）
- **日志消息**：使用中文便于中国用户理解（有意为之）
- **UI 标签**：使用中文面向中国市场（产品定位）

## 使用示例

### 启用所有调试日志
```bash
export KB_DEBUG_RAW_WAL=true
export KB_DEBUG_PROTO_PARSE=true
export KB_VALIDATE_OID=true

# 或使用 Java 系统属性
java -Dkb.debug.rawwal=true \
     -Dkb.debug.proto.parse=true \
     -Dkb.validate.oid=true \
     -jar kingbase-connector-core.jar
```

### 严格模式（解析失败抛错）
```bash
export KB_STRICT_PROTO_PARSE=true
# 或
java -Dkb.strict.proto.parse=true -jar kingbase-connector-core.jar
```

## 技术栈

- **后端**: Java 8, Debezium 1.5.4.final, Kingbase8 JDBC 8.6.1
- **前端**: Vue 3, Vite
- **构建**: Maven 3, Protobuf 3.21.12

## 兼容性说明

### Kingbase 版本
- 主要测试版本：Kingbase8 8.6.x
- OID 定义基于 8.6.1 JDBC 驱动
- 不同版本 OID 可能存在差异，建议启用验证日志

### 数据库插件
- 支持 decoderbufs 插件
- 兼容官方和当前两种 proto 协议格式

## 未来改进建议

1. **国际化**：考虑为日志消息添加多语言支持
2. **性能监控**：添加 proto 解析性能指标
3. **自动化测试**：为新增组件添加单元测试
4. **文档完善**：为每个组件添加使用示例

## 参考资料

- [PostgreSQL 逻辑复制文档](https://www.postgresql.org/docs/current/logical-replication.html)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/connectors/postgresql.html)
- [Kingbase8 官方文档](https://help.kingbase.com.cn/)
