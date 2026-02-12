# Kingbase Rich Types CDC 测试步骤

本文档配套 SQL：`docs/sql/kingbase_cdc_rich_types_demo.sql`  
固定样例（非批量生成）可用：`docs/sql/kingbase_cdc_fixed_rows.sql`

## 1. 数据库前置配置

1. 修改 `kingbase.conf`：

```conf
wal_level=logical
max_replication_slots=10
max_wal_senders=10
```

2. 重启 Kingbase 服务。

## 2. 创建 CDC 所需对象

在 `test` 库执行：

```sql
-- 创建逻辑复制槽（若已存在先删除）
SELECT * FROM sys_create_logical_replication_slot('dbz_kingbase_slot', 'decoderbufs');

-- 查看槽
SELECT * FROM sys_catalog.sys_replication_slots WHERE slot_name='dbz_kingbase_slot';
```

## 3. 执行多类型测试表脚本

```bash
# 示例（按你的环境调整）
ksql -h 127.0.0.1 -p 54321 -U system -d test -f docs/sql/kingbase_cdc_rich_types_demo.sql
```

脚本会完成：
- 建表 `form.t_cdc_rich_types`
- 写入 100 行 rich type 数据
- 执行 10 行 `UPDATE`
- 执行 10 行 `DELETE`

## 4. Connector 启动参数

`KingbaseTest` 建议环境变量：

```bash
export KB_HOST=127.0.0.1
export KB_PORT=54321
export KB_USER=system
export KB_PASSWORD=your_password
export KB_DB=test
export KB_TABLES=form.t_cdc_rich_types
export KB_SCHEMAS=form
export KB_PLUGIN_NAME=decoderbufs
export KB_SNAPSHOT_MODE=never
export KB_SLOT_NAME=dbz_kingbase_slot
export KB_SLOT_DROP_ON_STOP=false
```

调试开关（可选）：

```bash
export KB_DEBUG_DATUM=true
export KB_DEBUG_RAW_WAL=true
```

## 5. 结果验证

1. 启动 `KingbaseTest`：

```bash
mvn -q -pl kingbase-connector-core \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=io.debezium.connector.kingbasees.KingbaseTest \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

（兼容旧入口也可：`-Dexec.mainClass=KingbaseTest`）
2. 观察控制台事件输出：
- `INSERT`：100 条（若 `snapshot.mode=never`，只会看到启动后新写入）
- `UPDATE`：10 条
- `DELETE`：10 条
3. 重点确认字段：`tinyint/mediumint/int3/number/datetime/datetimetz/jsonb/bytea` 是否出现在事件中。

## 6. 清理（可选）

```sql
DROP TABLE IF EXISTS form.t_cdc_rich_types;
-- 仅在需要重置链路时删除 slot
SELECT sys_drop_replication_slot('dbz_kingbase_slot');
```
