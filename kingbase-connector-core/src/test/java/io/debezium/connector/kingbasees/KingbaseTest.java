package io.debezium.connector.kingbasees;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Kingbase CDC 本地冒烟入口。
 * 支持 IDE 直接运行，也支持通过 Maven 指定该类运行。
 */
@Slf4j
public class KingbaseTest {

    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    public static void main(String[] args) throws Exception {
        initLoggingConfig();
        ensureWorkDir();
        log.info("[KingbaseTest] 启动本地 CDC 冒烟测试");

        Properties props = new Properties();
        props.setProperty("name", "kingbase");
        props.setProperty("connector.class", PostgresConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        props.setProperty("offset.storage.file.filename", "/tmp/debezium/kingbase/offset.txt");
        props.setProperty("offset.flush.interval.ms", "60000");

        props.setProperty("plugin.name", getSetting("kb.plugin", "KB_PLUGIN_NAME", "decoderbufs"));
        props.setProperty("database.hostname", getSetting("kb.host", "KB_HOST", "127.0.0.1"));
        props.setProperty("database.port", getSetting("kb.port", "KB_PORT", "5322"));
        props.setProperty("database.user", getSetting("kb.user", "KB_USER", "kingbase"));
        props.setProperty("database.password", getSetting("kb.password", "KB_PASSWORD", "123456"));
        props.setProperty("database.server.id", getSetting("kb.server.id", "KB_SERVER_ID", "40981"));
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", "/tmp/debezium/kingbase/history.txt");
        props.setProperty("database.server.name", getSetting("kb.server.name", "KB_SERVER_NAME", "kingbase-server"));
        props.setProperty("database.dbname", getSetting("kb.db", "KB_DB", "test"));
        props.setProperty("snapshot.mode", getSetting("kb.snapshot.mode", "KB_SNAPSHOT_MODE", "never"));
        props.setProperty("table.include.list", normalizeCsvList(getSetting("kb.tables", "KB_TABLES", "form.t_cdc_rich_types,form.t_debug")));
        props.setProperty("slot.name", getSetting("kb.slot.name", "KB_SLOT_NAME", "dbz_kingbase_slot"));
        props.setProperty("slot.drop.on.stop", getSetting("kb.slot.drop.on.stop", "KB_SLOT_DROP_ON_STOP", "false"));

        String schemaIncludeList = normalizeCsvList(getSetting("kb.schemas", "KB_SCHEMAS", ""));
        if (!schemaIncludeList.isEmpty()) {
            props.setProperty("schema.include.list", schemaIncludeList);
        }

        log.info("[KingbaseTest] 准备启动连接器 host={} port={} db={} tables={} schemas={} slot={} snapshot={}",
                props.getProperty("database.hostname"),
                props.getProperty("database.port"),
                props.getProperty("database.dbname"),
                props.getProperty("table.include.list"),
                props.getProperty("schema.include.list", "<all>"),
                props.getProperty("slot.name"),
                props.getProperty("snapshot.mode"));

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(KingbaseTest::printEvent)
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        log.error("[KingbaseTest] 连接器运行失败：{}", message, error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        log.info("[KingbaseTest] 引擎线程已启动");

        addShutdownHook(engine);
        awaitTermination(executor);
        log.info("[KingbaseTest] 运行结束");
    }

    private static void ensureWorkDir() {
        File dir = new File("/tmp/debezium/kingbase");
        if (!dir.exists() && !dir.mkdirs()) {
            log.warn("[KingbaseTest] 创建工作目录失败：{}", dir.getAbsolutePath());
        }
    }

    private static void initLoggingConfig() {
        if (System.getProperty("log4j.configuration") != null) {
            return;
        }

        URL config = KingbaseTest.class.getClassLoader().getResource("log4j.properties");
        if (config != null) {
            System.setProperty("log4j.configuration", config.toString());
            log.info("[KingbaseTest] 使用 log4j 配置：{}", config);
        }
        else {
            log.info("[KingbaseTest] classpath 中未找到 log4j.properties");
        }
    }

    private static String getSetting(String sysKey, String envKey, String defaultValue) {
        String sysValue = System.getProperty(sysKey);
        if (sysValue != null && !sysValue.trim().isEmpty()) {
            return sysValue.trim();
        }

        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue.trim();
        }

        return defaultValue;
    }

    private static String normalizeCsvList(String raw) {
        if (raw == null) {
            return "";
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));
    }

    private static void printEvent(ChangeEvent<String, String> event) {
        log.info("----- Debezium 事件 -----");
        log.info("目标主题：{}", event.destination());
        log.info("操作类型：{}", extractOperation(event.value()));
        log.info("主键内容：{}", event.key());
        log.info("事件内容：{}", event.value());
    }

    private static String extractOperation(String valueJson) {
        if (valueJson == null) {
            return "空";
        }
        int marker = valueJson.indexOf("\"op\":\"");
        if (marker < 0) {
            return "未知";
        }
        int start = marker + 6;
        int end = valueJson.indexOf('"', start);
        if (end < 0) {
            return "未知";
        }
        return valueJson.substring(start, end);
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> debeziumEngine) {
        if (debeziumEngine == null) {
            return;
        }
        try {
            debeziumEngine.close();
        }
        catch (IOException ignored) {
        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> debeziumEngine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(debeziumEngine)));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor == null) {
            return;
        }

        try {
            int loops = 0;
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                loops++;
                if (loops % 6 == 0) {
                    log.info("[KingbaseTest] 运行中，等待 CDC 事件...");
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
