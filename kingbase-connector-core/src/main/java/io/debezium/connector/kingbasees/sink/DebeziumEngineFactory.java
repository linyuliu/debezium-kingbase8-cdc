package io.debezium.connector.kingbasees.sink;

import io.debezium.connector.kingbasees.PostgresConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Debezium 引擎构建工厂。
 */
final class DebeziumEngineFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumEngineFactory.class);

    private DebeziumEngineFactory() {
    }

    static DebeziumEngine<ChangeEvent<String, String>> build(SyncConfig config, SyncWriter writer) {
        Properties props = new Properties();
        props.setProperty("name", config.connectorName);
        props.setProperty("connector.class", PostgresConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        props.setProperty("offset.storage.file.filename", config.offsetFile);
        props.setProperty("offset.flush.interval.ms", String.valueOf(config.offsetFlushMs));
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", config.historyFile);

        props.setProperty("plugin.name", config.kbPlugin);
        props.setProperty("database.hostname", config.kbHost);
        props.setProperty("database.port", config.kbPort);
        props.setProperty("database.user", config.kbUser);
        props.setProperty("database.password", config.kbPassword);
        props.setProperty("database.server.id", config.kbServerId);
        props.setProperty("database.server.name", config.kbServerName);
        props.setProperty("database.dbname", config.kbDb);
        props.setProperty("snapshot.mode", config.kbSnapshotMode);
        props.setProperty("slot.name", config.kbSlotName);
        props.setProperty("slot.drop.on.stop", String.valueOf(config.kbSlotDropOnStop));
        props.setProperty("tombstones.on.delete", String.valueOf(config.tombstoneAsDelete || config.outputMode.hasEnhancedJsonOutput()));
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("binary.handling.mode", "base64");
        props.setProperty("include.schema.changes", "false");

        if (!config.tableIncludeListRaw.isEmpty()) {
            props.setProperty("table.include.list", config.tableIncludeListRaw);
        }
        if (!config.schemaIncludeListRaw.isEmpty()) {
            props.setProperty("schema.include.list", config.schemaIncludeListRaw);
        }

        return DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(writer::handle)
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        LOGGER.error("[同步引擎] Debezium 异常退出：{}", message, error);
                    }
                    else if (!success) {
                        LOGGER.warn("[同步引擎] Debezium 非正常退出：{}", message);
                    }
                    else {
                        LOGGER.info("[同步引擎] Debezium 正常退出：{}", message);
                    }
                })
                .build();
    }
}
