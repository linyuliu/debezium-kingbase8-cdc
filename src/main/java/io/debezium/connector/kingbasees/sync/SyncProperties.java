package io.debezium.connector.kingbasees.sync;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "sync")
public class SyncProperties {

    private boolean enabled = true;
    private boolean recreateConnectors = true;
    private Source source = new Source();
    private Doris doris = new Doris();
    private KafkaConnect kafkaConnect = new KafkaConnect();

    @Data
    public static class Source {
        private String host = "127.0.0.1";
        private int port = 54321;
        private String database = "test";
        private String user = "kingbase";
        private String password = "123456";

        // Debezium Source 基础配置
        private String pluginName = "decoderbufs";
        private String snapshotMode = "initial";
        private String slotName = "dbz_kingbase_slot";
        private boolean slotDropOnStop = false;
        private boolean initSlot = true;
        private boolean recreateSlot = false;

        // 源端增强配置（可选）
        private boolean applyReplicaIdentityFull = true;
        private boolean replicaIdentityFullFailFast = false;

        // Debezium Topic 前缀（最终 topic: serverName.schema.table）
        private String serverName = "kingbase_server";
        private String sourceConnectorName = "kb-source";

        // 两者至少配置一个：优先 includeTables
        private String includeTables = "";
        private String includeSchemas = "";
    }

    @Data
    public static class Doris {
        private String jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true";
        private String user = "root";
        private String password = "";

        // 本地重跑策略（自动建表、是否先删表）
        private boolean dropTablesBeforeSync = true;
        private boolean autoCreateTables = true;

        // 路由模式：
        // schema_table => cdc.schema__table
        // schema_as_db => cdc_schema.table
        private String routeMode = "schema_table";
        private String database = "cdc";
        private String databasePrefix = "cdc_";
        private String tablePrefix = "";
        private String tableSuffix = "";
        private String schemaTableSeparator = "__";

        private int buckets = 10;
        private int replicationNum = 1;
    }

    @Data
    public static class KafkaConnect {
        private String connectUrl = "http://127.0.0.1:8083";
        private String sinkConnectorName = "doris-sink";
        private int sinkTasksMax = 1;

        // Doris Connector 对接 FE 的地址与端口
        private String dorisFeNodes = "127.0.0.1";
        private int dorisHttpPort = 8030;
        private int dorisQueryPort = 9030;

        private int bufferCountRecords = 10000;
        private int bufferFlushTimeMs = 1000;
        private int maxRetries = 3;
    }
}
