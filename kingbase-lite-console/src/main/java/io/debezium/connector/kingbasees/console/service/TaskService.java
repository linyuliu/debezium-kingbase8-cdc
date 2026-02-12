package io.debezium.connector.kingbasees.console.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import io.debezium.connector.kingbasees.console.model.RouteMode;
import io.debezium.connector.kingbasees.console.model.RunMode;
import io.debezium.connector.kingbasees.console.model.SyncTaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * 任务服务：
 * 1) 任务 CRUD（持久化到 SQLite）
 * 2) 任务参数归一化与校验
 * 3) 任务调度注册刷新
 */
@Slf4j
@Service
public class TaskService {

    private final JdbcTemplate jdbcTemplate;
    @SuppressWarnings("unused")
    private final ConsoleStorageService storageService;
    private final DataSourceService dataSourceService;
    private final TaskRunnerService taskRunnerService;

    public TaskService(JdbcTemplate jdbcTemplate,
                       ConsoleStorageService storageService,
                       DataSourceService dataSourceService,
                       TaskRunnerService taskRunnerService) {
        this.jdbcTemplate = jdbcTemplate;
        this.storageService = storageService;
        this.dataSourceService = dataSourceService;
        this.taskRunnerService = taskRunnerService;
    }

    @PostConstruct
    public synchronized void init() {
        List<SyncTaskConfig> tasks = list();
        taskRunnerService.reloadSchedules(tasks);
        log.info("[任务] 初始化完成，当前任务数量={}", tasks.size());
    }

    public List<SyncTaskConfig> list() {
        String sql = "SELECT id,name,source_data_source_id,target_data_source_id,schedule_enabled,schedule_cron,schedule_run_mode,config_json,created_at,updated_at " +
                "FROM lite_task ORDER BY created_at ASC, name ASC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> parseRow(rs));
    }

    public SyncTaskConfig getRequired(String id) {
        List<SyncTaskConfig> list = jdbcTemplate.query(
                "SELECT id,name,source_data_source_id,target_data_source_id,schedule_enabled,schedule_cron,schedule_run_mode,config_json,created_at,updated_at " +
                        "FROM lite_task WHERE id = ?",
                (rs, rowNum) -> parseRow(rs),
                id);
        if (list.isEmpty()) {
            throw new IllegalArgumentException("任务不存在: " + id);
        }
        return list.get(0);
    }

    public synchronized SyncTaskConfig create(SyncTaskConfig input) {
        SyncTaskConfig task = normalizeAndValidate(input, true, null);
        String id = "task_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long now = System.currentTimeMillis();
        task.setId(id);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);

        jdbcTemplate.update("INSERT INTO lite_task(" +
                        "id,name,source_data_source_id,target_data_source_id,schedule_enabled,schedule_cron,schedule_run_mode,config_json,created_at,updated_at" +
                        ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                task.getId(),
                task.getName(),
                task.getSourceDataSourceId(),
                task.getTargetDataSourceId(),
                task.isScheduleEnabled() ? 1 : 0,
                task.getScheduleCron(),
                task.getScheduleRunMode(),
                toJson(task),
                task.getCreatedAt(),
                task.getUpdatedAt());

        taskRunnerService.refreshSchedule(task);
        log.info("[任务] 创建成功 id={}, name={}", task.getId(), task.getName());
        return task;
    }

    public synchronized SyncTaskConfig update(String id, SyncTaskConfig input) {
        SyncTaskConfig existing = getRequired(id);
        SyncTaskConfig task = normalizeAndValidate(input, false, id);

        task.setId(id);
        task.setCreatedAt(existing.getCreatedAt());
        task.setUpdatedAt(System.currentTimeMillis());

        jdbcTemplate.update("UPDATE lite_task SET " +
                        "name=?,source_data_source_id=?,target_data_source_id=?,schedule_enabled=?,schedule_cron=?,schedule_run_mode=?,config_json=?,updated_at=? " +
                        "WHERE id=?",
                task.getName(),
                task.getSourceDataSourceId(),
                task.getTargetDataSourceId(),
                task.isScheduleEnabled() ? 1 : 0,
                task.getScheduleCron(),
                task.getScheduleRunMode(),
                toJson(task),
                task.getUpdatedAt(),
                task.getId());

        taskRunnerService.refreshSchedule(task);
        log.info("[任务] 更新成功 id={}, name={}", task.getId(), task.getName());
        return task;
    }

    public synchronized void delete(String id) {
        getRequired(id);
        jdbcTemplate.update("DELETE FROM lite_task WHERE id = ?", id);
        taskRunnerService.removeTask(id);
        log.info("[任务] 删除成功 id={}", id);
    }

    private SyncTaskConfig normalizeAndValidate(SyncTaskConfig input, boolean creating, String currentId) {
        if (input == null) {
            throw new IllegalArgumentException("任务请求体不能为空");
        }

        SyncTaskConfig task = new SyncTaskConfig();
        task.setName(trimToNull(input.getName()));
        task.setSourceDataSourceId(trimToNull(input.getSourceDataSourceId()));
        task.setTargetDataSourceId(trimToNull(input.getTargetDataSourceId()));
        task.setIncludeTables(normalizeCsv(input.getIncludeTables()));
        task.setIncludeSchemas(normalizeCsv(input.getIncludeSchemas()));
        task.setServerName(defaultString(input.getServerName(), "kingbase_server"));
        task.setServerId(defaultString(input.getServerId(), "54001"));
        task.setSlotName(defaultString(input.getSlotName(), "dbz_kingbase_slot"));
        task.setPluginName(defaultString(input.getPluginName(), "decoderbufs"));
        task.setInitSlot(input.isInitSlot());
        task.setRecreateSlot(input.isRecreateSlot());
        task.setSlotDropOnStop(input.isSlotDropOnStop());
        task.setReplicaIdentityFull(input.isReplicaIdentityFull());
        task.setReplicaIdentityFullFailFast(input.isReplicaIdentityFullFailFast());
        task.setReplicaIdentityFullTables(normalizeCsv(input.getReplicaIdentityFullTables()));

        task.setRouteMode(RouteMode.fromCode(defaultString(input.getRouteMode(), "schema_table")).getCode());
        task.setDorisDatabase(defaultString(input.getDorisDatabase(), "cdc"));
        task.setDorisDatabasePrefix(defaultString(input.getDorisDatabasePrefix(), "cdc_"));
        task.setDorisTablePrefix(defaultString(input.getDorisTablePrefix(), ""));
        task.setDorisTableSuffix(defaultString(input.getDorisTableSuffix(), ""));
        task.setDorisSchemaTableSeparator(defaultString(input.getDorisSchemaTableSeparator(), "__"));
        task.setAutoCreateTargetDatabase(input.isAutoCreateTargetDatabase());
        task.setAutoCreateTargetTable(input.isAutoCreateTargetTable());
        task.setAutoAddTargetColumns(input.isAutoAddTargetColumns());
        task.setSkipDeleteWithoutPk(input.isSkipDeleteWithoutPk());
        task.setDorisBuckets(input.getDorisBuckets() == null ? 10 : input.getDorisBuckets());
        task.setDorisReplicationNum(input.getDorisReplicationNum() == null ? 1 : input.getDorisReplicationNum());
        task.setDeleteSyncMode(defaultString(input.getDeleteSyncMode(), "PHYSICAL_DELETE").toUpperCase());
        task.setLogicalDeleteColumn(defaultString(input.getLogicalDeleteColumn(), "__DORIS_DELETE_SIGN__"));
        task.setOffsetFlushMs(input.getOffsetFlushMs() == null ? 10000L : input.getOffsetFlushMs());
        task.setOutputMode(defaultString(input.getOutputMode(), "JDBC_DML").toUpperCase());
        task.setEnhancedBatchSize(input.getEnhancedBatchSize() == null ? 1000 : input.getEnhancedBatchSize());
        task.setEnhancedOutputFile(trimToNull(input.getEnhancedOutputFile()));
        task.setDeltaNullStrategy(defaultString(input.getDeltaNullStrategy(), "SKIP").toUpperCase());
        task.setChangedFieldsEnabled(input.isChangedFieldsEnabled());
        task.setDeltasEnabled(input.isDeltasEnabled());
        task.setTombstoneAsDelete(input.isTombstoneAsDelete());

        task.setScheduleEnabled(input.isScheduleEnabled());
        task.setScheduleCron(trimToNull(input.getScheduleCron()));
        task.setScheduleRunMode(defaultString(input.getScheduleRunMode(), RunMode.RESUME_CDC.name()));

        task.setTruncateBeforeFull(input.isTruncateBeforeFull());
        task.setDropBeforeFull(input.isDropBeforeFull());
        task.setForceRecreateSlotOnForceRun(input.isForceRecreateSlotOnForceRun());

        task.setWorkDir(trimToNull(input.getWorkDir()));

        if (isBlank(task.getName())) {
            throw new IllegalArgumentException("任务名称不能为空");
        }
        if (isBlank(task.getSourceDataSourceId()) || isBlank(task.getTargetDataSourceId())) {
            throw new IllegalArgumentException("源数据源和目标数据源不能为空");
        }

        dataSourceService.getRequired(task.getSourceDataSourceId());
        dataSourceService.getRequired(task.getTargetDataSourceId());

        if (isBlank(task.getIncludeTables()) && isBlank(task.getIncludeSchemas())) {
            throw new IllegalArgumentException("includeTables 与 includeSchemas 至少填写一个");
        }

        if (task.isScheduleEnabled()) {
            if (isBlank(task.getScheduleCron())) {
                throw new IllegalArgumentException("开启调度后必须填写 Cron");
            }
            if (!CronSequenceGenerator.isValidExpression(task.getScheduleCron())) {
                throw new IllegalArgumentException("Cron 表达式不合法: " + task.getScheduleCron());
            }
        }

        RunMode.fromText(task.getScheduleRunMode());

        if (task.getDorisBuckets() < 1) {
            throw new IllegalArgumentException("dorisBuckets 必须 >= 1");
        }
        if (task.getDorisReplicationNum() < 1) {
            throw new IllegalArgumentException("dorisReplicationNum 必须 >= 1");
        }
        if (task.getEnhancedBatchSize() < 1) {
            throw new IllegalArgumentException("enhancedBatchSize 必须 >= 1");
        }

        if (existsByName(task.getName(), currentId)) {
            throw new IllegalArgumentException("任务名称已存在: " + task.getName());
        }

        if (!creating && currentId != null) {
            task.setId(currentId);
        }
        return task;
    }

    private boolean existsByName(String name, String currentId) {
        Integer count;
        if (currentId == null) {
            count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_task WHERE lower(name) = lower(?)", Integer.class, name);
        }
        else {
            count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_task WHERE lower(name) = lower(?) AND id <> ?",
                    Integer.class,
                    name,
                    currentId);
        }
        return count != null && count > 0;
    }

    private SyncTaskConfig parseRow(ResultSet rs) throws SQLException {
        String id = rs.getString("id");
        String name = rs.getString("name");
        String sourceDataSourceId = rs.getString("source_data_source_id");
        String targetDataSourceId = rs.getString("target_data_source_id");
        boolean scheduleEnabled = rs.getInt("schedule_enabled") == 1;
        String scheduleCron = rs.getString("schedule_cron");
        String scheduleRunMode = rs.getString("schedule_run_mode");
        String json = rs.getString("config_json");
        Long createdAt = rs.getLong("created_at");
        Long updatedAt = rs.getLong("updated_at");

        SyncTaskConfig task;
        try {
            task = isBlank(json) ? new SyncTaskConfig() : JSON.parseObject(json, SyncTaskConfig.class);
        }
        catch (Exception e) {
            log.warn("[任务] 解析 config_json 失败，按基础字段回填 id={}", id, e);
            task = new SyncTaskConfig();
        }
        if (task == null) {
            task = new SyncTaskConfig();
        }

        task.setId(id);
        task.setName(name);
        task.setSourceDataSourceId(sourceDataSourceId);
        task.setTargetDataSourceId(targetDataSourceId);
        task.setScheduleEnabled(scheduleEnabled);
        task.setScheduleCron(scheduleCron);
        task.setScheduleRunMode(defaultString(scheduleRunMode, RunMode.RESUME_CDC.name()));
        task.setCreatedAt(createdAt);
        task.setUpdatedAt(updatedAt);
        return task;
    }

    private String toJson(SyncTaskConfig task) {
        return JSON.toJSONString(task, JSONWriter.Feature.WriteNulls);
    }

    private String normalizeCsv(String csv) {
        if (isBlank(csv)) {
            return "";
        }
        String[] parts = csv.split(",");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            String v = part.trim();
            if (v.isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(v);
        }
        return sb.toString();
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String defaultString(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value.trim();
    }
}
