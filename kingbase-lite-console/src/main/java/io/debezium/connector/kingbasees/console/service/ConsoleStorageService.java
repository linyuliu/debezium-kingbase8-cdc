package io.debezium.connector.kingbasees.console.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.alibaba.fastjson2.JSONWriter;
import io.debezium.connector.kingbasees.console.config.ConsoleProperties;
import io.debezium.connector.kingbasees.console.model.DataSourceConfig;
import io.debezium.connector.kingbasees.console.model.SyncTaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

/**
 * 控制台存储初始化服务：
 * 1) 初始化 SQLite 表结构
 * 2) 自动迁移历史 JSON 文件到数据库
 */
@Slf4j
@Service
public class ConsoleStorageService {

    private final JdbcTemplate jdbcTemplate;
    private final ConsoleProperties properties;

    public ConsoleStorageService(JdbcTemplate jdbcTemplate, ConsoleProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    @PostConstruct
    public synchronized void init() {
        createSchema();
        migrateLegacyJsonIfNeeded();
    }

    private void createSchema() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS lite_datasource (" +
                "id VARCHAR(64) PRIMARY KEY, " +
                "name VARCHAR(128) NOT NULL, " +
                "type VARCHAR(32) NOT NULL, " +
                "config_json TEXT NOT NULL, " +
                "created_at BIGINT NOT NULL, " +
                "updated_at BIGINT NOT NULL" +
                ")");
        jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS uk_lite_datasource_name_nocase " +
                "ON lite_datasource(name COLLATE NOCASE)");

        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS lite_task (" +
                "id VARCHAR(64) PRIMARY KEY, " +
                "name VARCHAR(128) NOT NULL, " +
                "source_data_source_id VARCHAR(64) NOT NULL, " +
                "target_data_source_id VARCHAR(64) NOT NULL, " +
                "schedule_enabled INTEGER NOT NULL, " +
                "schedule_cron VARCHAR(128), " +
                "schedule_run_mode VARCHAR(64), " +
                "config_json TEXT NOT NULL, " +
                "created_at BIGINT NOT NULL, " +
                "updated_at BIGINT NOT NULL" +
                ")");
        jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS uk_lite_task_name_nocase " +
                "ON lite_task(name COLLATE NOCASE)");

        log.info("[存储] SQLite 表结构初始化完成");
    }

    private void migrateLegacyJsonIfNeeded() {
        migrateDatasourceJsonIfNeeded();
        migrateTaskJsonIfNeeded();
    }

    private void migrateDatasourceJsonIfNeeded() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_datasource", Integer.class);
        if (count != null && count > 0) {
            return;
        }

        Path jsonFile = Paths.get(properties.getDataDir(), "datasources.json");
        if (!Files.exists(jsonFile)) {
            return;
        }

        List<DataSourceConfig> list = readList(jsonFile, new TypeReference<List<DataSourceConfig>>() {
        });
        if (list.isEmpty()) {
            return;
        }

        int migrated = 0;
        for (DataSourceConfig config : list) {
            if (config == null || isBlank(config.getId()) || isBlank(config.getName()) || config.getType() == null) {
                continue;
            }
            long createdAt = config.getCreatedAt() == null ? System.currentTimeMillis() : config.getCreatedAt();
            long updatedAt = config.getUpdatedAt() == null ? createdAt : config.getUpdatedAt();
            String json = JSON.toJSONString(config, JSONWriter.Feature.WriteNulls);
            jdbcTemplate.update("INSERT OR REPLACE INTO lite_datasource(id,name,type,config_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                    config.getId(), config.getName(), config.getType().name(), json, createdAt, updatedAt);
            migrated++;
        }

        log.info("[迁移] 数据源历史 JSON 已迁移，条数={}", migrated);
    }

    private void migrateTaskJsonIfNeeded() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_task", Integer.class);
        if (count != null && count > 0) {
            return;
        }

        Path jsonFile = Paths.get(properties.getDataDir(), "tasks.json");
        if (!Files.exists(jsonFile)) {
            return;
        }

        List<SyncTaskConfig> list = readList(jsonFile, new TypeReference<List<SyncTaskConfig>>() {
        });
        if (list.isEmpty()) {
            return;
        }

        int migrated = 0;
        for (SyncTaskConfig task : list) {
            if (task == null || isBlank(task.getId()) || isBlank(task.getName()) ||
                    isBlank(task.getSourceDataSourceId()) || isBlank(task.getTargetDataSourceId())) {
                continue;
            }
            long createdAt = task.getCreatedAt() == null ? System.currentTimeMillis() : task.getCreatedAt();
            long updatedAt = task.getUpdatedAt() == null ? createdAt : task.getUpdatedAt();
            String json = JSON.toJSONString(task, JSONWriter.Feature.WriteNulls);

            jdbcTemplate.update("INSERT OR REPLACE INTO lite_task(" +
                            "id,name,source_data_source_id,target_data_source_id,schedule_enabled,schedule_cron,schedule_run_mode,config_json,created_at,updated_at" +
                            ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                    task.getId(), task.getName(), task.getSourceDataSourceId(), task.getTargetDataSourceId(),
                    task.isScheduleEnabled() ? 1 : 0,
                    task.getScheduleCron(), task.getScheduleRunMode(),
                    json, createdAt, updatedAt);
            migrated++;
        }

        log.info("[迁移] 任务历史 JSON 已迁移，条数={}", migrated);
    }

    private <T> List<T> readList(Path path, TypeReference<List<T>> typeReference) {
        try {
            byte[] bytes = Files.readAllBytes(path);
            String json = new String(bytes, StandardCharsets.UTF_8);
            List<T> list = JSON.parseObject(json, typeReference);
            return list == null ? Collections.<T>emptyList() : list;
        }
        catch (IOException e) {
            log.warn("[迁移] 读取历史 JSON 失败: {}", path, e);
            return Collections.emptyList();
        }
        catch (Exception e) {
            log.warn("[迁移] 解析历史 JSON 失败: {}", path, e);
            return Collections.emptyList();
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
