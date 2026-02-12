package io.debezium.connector.kingbasees.console.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import io.debezium.connector.kingbasees.console.model.DataSourceConfig;
import io.debezium.connector.kingbasees.console.model.DataSourceTestResult;
import io.debezium.connector.kingbasees.console.model.DataSourceType;
import io.debezium.connector.kingbasees.console.model.SlotCheckResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * 数据源服务：
 * 1) 数据源 CRUD（持久化到 SQLite）
 * 2) 连接测试
 * 3) Kingbase 复制槽检查
 */
@Slf4j
@Service
public class DataSourceService {

    private final JdbcTemplate jdbcTemplate;
    @SuppressWarnings("unused")
    private final ConsoleStorageService storageService;

    public DataSourceService(JdbcTemplate jdbcTemplate, ConsoleStorageService storageService) {
        this.jdbcTemplate = jdbcTemplate;
        this.storageService = storageService;
    }

    @PostConstruct
    public void init() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_datasource", Integer.class);
        log.info("[数据源] 初始化完成，当前数据源数量={}", count == null ? 0 : count);
    }

    public List<DataSourceConfig> list() {
        String sql = "SELECT id,name,type,config_json,created_at,updated_at FROM lite_datasource ORDER BY created_at ASC, name ASC";
        return jdbcTemplate.query(sql, (rs, rowNum) -> parseRow(rs));
    }

    public DataSourceConfig getRequired(String id) {
        List<DataSourceConfig> list = jdbcTemplate.query(
                "SELECT id,name,type,config_json,created_at,updated_at FROM lite_datasource WHERE id = ?",
                (rs, rowNum) -> parseRow(rs),
                id
        );
        if (list.isEmpty()) {
            throw new IllegalArgumentException("数据源不存在: " + id);
        }
        return list.get(0);
    }

    public synchronized DataSourceConfig create(DataSourceConfig input) {
        DataSourceConfig config = normalizeAndValidate(input, null);
        String id = "ds_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long now = System.currentTimeMillis();
        config.setId(id);
        config.setCreatedAt(now);
        config.setUpdatedAt(now);

        jdbcTemplate.update("INSERT INTO lite_datasource(id,name,type,config_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                config.getId(),
                config.getName(),
                config.getType().name(),
                toJson(config),
                config.getCreatedAt(),
                config.getUpdatedAt());

        log.info("[数据源] 创建成功 id={}, name={}, type={}", config.getId(), config.getName(), config.getType());
        return config;
    }

    public synchronized DataSourceConfig update(String id, DataSourceConfig input) {
        DataSourceConfig existing = getRequired(id);
        DataSourceConfig config = normalizeAndValidate(input, id);
        config.setId(id);
        config.setCreatedAt(existing.getCreatedAt());
        config.setUpdatedAt(System.currentTimeMillis());

        jdbcTemplate.update("UPDATE lite_datasource SET name=?, type=?, config_json=?, updated_at=? WHERE id=?",
                config.getName(),
                config.getType().name(),
                toJson(config),
                config.getUpdatedAt(),
                id);

        log.info("[数据源] 更新成功 id={}, name={}", id, config.getName());
        return config;
    }

    public synchronized void delete(String id) {
        getRequired(id);

        Integer referenced = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM lite_task WHERE source_data_source_id = ? OR target_data_source_id = ?",
                Integer.class,
                id,
                id);
        if (referenced != null && referenced > 0) {
            throw new IllegalArgumentException("数据源被任务引用，不能删除: " + id);
        }

        jdbcTemplate.update("DELETE FROM lite_datasource WHERE id = ?", id);
        log.info("[数据源] 删除成功 id={}", id);
    }

    public DataSourceTestResult testConnectionById(String id) {
        return testConnection(getRequired(id));
    }

    public DataSourceTestResult testConnection(DataSourceConfig config) {
        long start = System.currentTimeMillis();
        String url = buildJdbcUrl(config);
        try {
            loadDriver(config.getType());
            try (Connection connection = DriverManager.getConnection(url, config.getUsername(), defaultString(config.getPassword()));
                 Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(5);
                try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                    if (rs.next()) {
                        DataSourceTestResult ok = new DataSourceTestResult();
                        ok.setOk(true);
                        ok.setMessage("连接成功");
                        ok.setLatencyMs(System.currentTimeMillis() - start);
                        ok.setJdbcUrl(hidePassword(url));
                        return ok;
                    }
                }
            }
            return failResult("SELECT 1 未返回结果", start, url);
        }
        catch (Exception e) {
            return failResult(e.getMessage(), start, url);
        }
    }

    public SlotCheckResult checkSlot(String id, String slotName) {
        DataSourceConfig config = getRequired(id);
        if (config.getType() != DataSourceType.KINGBASE) {
            throw new IllegalArgumentException("仅支持 KINGBASE 数据源做 Slot 检查");
        }
        if (isBlank(slotName)) {
            throw new IllegalArgumentException("slotName 不能为空");
        }

        String url = buildJdbcUrl(config);
        try {
            loadDriver(config.getType());
            try (Connection connection = DriverManager.getConnection(url, config.getUsername(), defaultString(config.getPassword()))) {
                return querySlot(connection, slotName);
            }
        }
        catch (Exception e) {
            SlotCheckResult result = new SlotCheckResult();
            result.setOk(false);
            result.setSlotName(slotName);
            result.setMessage(e.getMessage());
            return result;
        }
    }

    private SlotCheckResult querySlot(Connection connection, String slotName) throws SQLException {
        String[] sqls = new String[]{
                "SELECT slot_name, plugin, active FROM sys_catalog.sys_replication_slots WHERE slot_name = ?",
                "SELECT slot_name, plugin, active FROM pg_catalog.pg_replication_slots WHERE slot_name = ?"
        };

        for (String sql : sqls) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, slotName);
                try (ResultSet rs = ps.executeQuery()) {
                    SlotCheckResult result = new SlotCheckResult();
                    if (rs.next()) {
                        result.setOk(true);
                        result.setExists(true);
                        result.setSlotName(rs.getString("slot_name"));
                        result.setPlugin(rs.getString("plugin"));
                        result.setActive(rs.getBoolean("active"));
                        return result;
                    }
                    result.setOk(true);
                    result.setExists(false);
                    result.setSlotName(slotName);
                    return result;
                }
            }
            catch (SQLException ignored) {
            }
        }

        throw new SQLException("无法从 sys_catalog 或 pg_catalog 查询复制槽");
    }

    private DataSourceConfig normalizeAndValidate(DataSourceConfig input, String currentId) {
        if (input == null) {
            throw new IllegalArgumentException("数据源请求体不能为空");
        }

        DataSourceConfig config = new DataSourceConfig();
        config.setName(trimToNull(input.getName()));
        config.setType(input.getType());
        config.setHost(trimToNull(input.getHost()));
        config.setPort(input.getPort());
        config.setDatabaseName(trimToNull(input.getDatabaseName()));
        config.setUsername(trimToNull(input.getUsername()));
        config.setPassword(input.getPassword() == null ? "" : input.getPassword());
        config.setParams(trimToNull(input.getParams()));

        if (isBlank(config.getName())) {
            throw new IllegalArgumentException("数据源名称不能为空");
        }
        if (config.getType() == null) {
            throw new IllegalArgumentException("数据源类型不能为空");
        }
        if (isBlank(config.getHost())) {
            throw new IllegalArgumentException("数据源地址不能为空");
        }
        if (config.getPort() == null || config.getPort() < 1 || config.getPort() > 65535) {
            throw new IllegalArgumentException("端口必须在 1..65535 范围内");
        }
        if (isBlank(config.getUsername())) {
            throw new IllegalArgumentException("用户名不能为空");
        }

        if (config.getType() == DataSourceType.KINGBASE && isBlank(config.getDatabaseName())) {
            throw new IllegalArgumentException("KINGBASE 数据源必须填写 databaseName");
        }
        if (config.getType() == DataSourceType.DORIS && isBlank(config.getDatabaseName())) {
            config.setDatabaseName("cdc");
        }

        if (existsByName(config.getName(), currentId)) {
            throw new IllegalArgumentException("数据源名称已存在: " + config.getName());
        }

        return config;
    }

    private boolean existsByName(String name, String currentId) {
        Integer count;
        if (currentId == null) {
            count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM lite_datasource WHERE lower(name) = lower(?)", Integer.class, name);
        }
        else {
            count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(1) FROM lite_datasource WHERE lower(name) = lower(?) AND id <> ?",
                    Integer.class,
                    name,
                    currentId);
        }
        return count != null && count > 0;
    }

    private DataSourceConfig parseRow(ResultSet rs) throws SQLException {
        String id = rs.getString("id");
        String name = rs.getString("name");
        String type = rs.getString("type");
        String json = rs.getString("config_json");
        Long createdAt = rs.getLong("created_at");
        Long updatedAt = rs.getLong("updated_at");

        DataSourceConfig config;
        try {
            config = isBlank(json) ? new DataSourceConfig() : JSON.parseObject(json, DataSourceConfig.class);
        }
        catch (Exception e) {
            log.warn("[数据源] 解析 config_json 失败，按基础字段回填 id={}", id, e);
            config = new DataSourceConfig();
        }
        if (config == null) {
            config = new DataSourceConfig();
        }

        config.setId(id);
        config.setName(name);
        config.setType(DataSourceType.valueOf(type));
        config.setCreatedAt(createdAt);
        config.setUpdatedAt(updatedAt);
        return config;
    }

    private String toJson(DataSourceConfig config) {
        return JSON.toJSONString(config, JSONWriter.Feature.WriteNulls);
    }

    private String buildJdbcUrl(DataSourceConfig config) {
        String params = normalizeParams(config.getParams());
        if (config.getType() == DataSourceType.KINGBASE) {
            return "jdbc:kingbase8://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabaseName() + params;
        }
        return "jdbc:mysql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabaseName() +
                "?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true" + appendParams(params);
    }

    private void loadDriver(DataSourceType type) throws ClassNotFoundException {
        if (type == DataSourceType.KINGBASE) {
            Class.forName("com.kingbase8.Driver");
        }
        else {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
    }

    private String normalizeParams(String params) {
        if (isBlank(params)) {
            return "";
        }
        String value = params.trim();
        if (value.startsWith("?")) {
            return value;
        }
        return "?" + value;
    }

    private String appendParams(String extra) {
        if (isBlank(extra)) {
            return "";
        }
        if (extra.startsWith("?")) {
            return "&" + extra.substring(1);
        }
        return "&" + extra;
    }

    private DataSourceTestResult failResult(String message, long start, String url) {
        DataSourceTestResult fail = new DataSourceTestResult();
        fail.setOk(false);
        fail.setMessage(message == null ? "连接失败" : message);
        fail.setLatencyMs(System.currentTimeMillis() - start);
        fail.setJdbcUrl(hidePassword(url));
        return fail;
    }

    private String hidePassword(String url) {
        if (url == null) {
            return "";
        }
        String lower = url.toLowerCase(Locale.ROOT);
        int idx = lower.indexOf("password=");
        if (idx < 0) {
            return url;
        }
        int end = url.indexOf('&', idx);
        if (end < 0) {
            return url.substring(0, idx) + "password=***";
        }
        return url.substring(0, idx) + "password=***" + url.substring(end);
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
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
}
