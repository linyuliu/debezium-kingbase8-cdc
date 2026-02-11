-- Kingbase CDC 运维 SQL（slot / replica identity / 检查）

-- 1) 查看所有复制槽
SELECT * FROM sys_catalog.sys_replication_slots;

-- 2) 查看指定槽
SELECT * FROM sys_catalog.sys_replication_slots WHERE slot_name = 'dbz_kingbase_slot';

-- 3) 创建槽（首次）
SELECT * FROM sys_create_logical_replication_slot('dbz_kingbase_slot', 'decoderbufs');

-- 4) 删除槽（重置链路时）
-- 注意：删除前先停掉 connector，避免 slot 处于 active 状态
SELECT sys_drop_replication_slot('dbz_kingbase_slot');

-- 5) 单表设置 REPLICA IDENTITY FULL
ALTER TABLE form.t_debug REPLICA IDENTITY FULL;

-- 6) 批量生成全表 FULL 语句（先执行查询，再复制结果执行）
SELECT format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', schemaname, tablename)
FROM pg_catalog.pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema');

-- 7) 查看每张表当前 REPLICA IDENTITY
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
