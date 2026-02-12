# KingbaseES CDC 配置与运维说明（本项目）

本文档面向当前仓库（`debezium-connector-kingbase`），默认使用 `plugin.name=decoderbufs`。  
常用运维 SQL 已整理到：`docs/sql/kingbase_cdc_ops.sql`。

## 1. 数据库前置配置

修改 `kingbase.conf` 并重启数据库：

```conf
wal_level=logical
max_replication_slots=10
max_wal_senders=10
```

建议：
- `max_replication_slots` >= 连接器实例数。
- `max_wal_senders` >= `max_replication_slots`。

## 2. Slot（复制槽）生命周期

创建和查看：

```sql
SELECT * FROM sys_create_logical_replication_slot('dbz_kingbase_slot', 'decoderbufs');
SELECT * FROM sys_catalog.sys_replication_slots WHERE slot_name = 'dbz_kingbase_slot';
```

删除（仅在重置链路时）：

```sql
SELECT sys_drop_replication_slot('dbz_kingbase_slot');
```

说明：
- 默认不建议删除 slot，避免丢增量位点。
- 本项目已支持 `slot.drop.on.stop`，默认 `false`（建议保持默认）。
- 多个 connector 不能共用同一个 slot；每个任务必须唯一 `slot.name`。

## 3. 多表、多 schema 同步

`table.include.list` 支持逗号分隔；可跨 schema：

```properties
table.include.list=form.t_debug,form.sys_user,public.order_info
schema.include.list=form,public
```

在当前 `KingbaseTest` 中可直接用环境变量覆盖：

```bash
export KB_TABLES='form.t_debug,form.sys_user,public.order_info'
export KB_SCHEMAS='form,public'
```

推荐启动方式（多模块项目）：

```bash
mvn -q -pl kingbase-connector-core \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=io.debezium.connector.kingbasees.KingbaseTest \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

兼容旧入口（默认包）也可用：

```bash
mvn -q -pl kingbase-connector-core \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=KingbaseTest \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

## 4. Publication 是否必须

对当前分支默认的 `decoderbufs`：
- 不依赖 `publication` 才能拿到行变更。
- `publication.autocreate.mode` 的描述是为 `pgoutput` 设计（配置注释里也写明了 only pgoutput）。

如果你沿用 Issue #408 的数据库侧习惯，保留 publication 不影响运行；但这条链路的关键是 `slot + decoderbufs`。

## 5. REPLICA IDENTITY FULL（单表和批量）

单表：

```sql
ALTER TABLE form.t_debug REPLICA IDENTITY FULL;
```

批量生成全库 SQL（先生成再执行）：

```sql
SELECT format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', schemaname, tablename)
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
```

查看当前状态：

```sql
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       CASE c.relreplident
         WHEN 'd' THEN 'DEFAULT'
         WHEN 'n' THEN 'NOTHING'
         WHEN 'f' THEN 'FULL'
         WHEN 'i' THEN 'INDEX'
       END AS replica_identity
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1, 2;
```

## 6. DDL 与字段变更行为

- DDL 本身不会像 DML 一样直接产出行事件。
- 当表结构变化后，通常在后续 DML 到来时触发 schema refresh。
- 看到 `Different column count ... refreshing table schema` 属于预期刷新日志。

## 7. 日志配置（你的 `log-spring.xml` 为什么不生效）

当前 `KingbaseTest` 是普通 Java Main + Debezium Embedded，不是 Spring Boot 应用。

同时 classpath 中实际生效的是 `slf4j-log4j12`，所以：
- `logback-spring.xml` / `log-spring.xml` 不会被这条启动链路加载。
- 应使用 `src/test/resources/log4j.properties`。

现在 `KingbaseTest` 启动时会自动加载 classpath 下的 `log4j.properties`，并打印已加载路径。

## 8. 常见告警解释

- `Unknown type named int requested` / `Unknown OID -1 requested`  
  类型归一化或元数据未命中导致，当前分支已补 `int/int2/int4/int8` 归一化与容错。

- `Unable to parse decoderbufs row message`  
  常见于混合 framing 或协议差异；当前分支支持 kingbase 变体和官方 decoderbufs 双协议尝试，并默认跳过不可解析帧（可通过 `KB_STRICT_PROTO_PARSE=true` 改回严格失败）。

## 参考

- https://github.com/datavane/tis/issues/408
- https://help.kingbase.com.cn/v8/development/sql-plsql/sql-quick/datatype.html
