package io.debezium.connector.kingbasees.console.model;

import lombok.Data;

@Data
public class SyncTaskConfig {

    private String id;
    private String name;

    private String sourceDataSourceId;
    private String targetDataSourceId;

    private String includeTables;
    private String includeSchemas;

    private String serverName = "kingbase_server";
    private String serverId = "54001";

    private String slotName = "dbz_kingbase_slot";
    private String pluginName = "decoderbufs";

    private boolean initSlot = true;
    private boolean recreateSlot = false;
    private boolean slotDropOnStop = false;

    private boolean replicaIdentityFull = false;
    private boolean replicaIdentityFullFailFast = false;
    private String replicaIdentityFullTables;

    private String routeMode = "schema_table";
    private String dorisDatabase = "cdc";
    private String dorisDatabasePrefix = "cdc_";
    private String dorisTablePrefix = "";
    private String dorisTableSuffix = "";
    private String dorisSchemaTableSeparator = "__";

    private boolean autoCreateTargetDatabase = true;
    private boolean autoCreateTargetTable = true;
    private boolean autoAddTargetColumns = true;
    private boolean skipDeleteWithoutPk = true;

    private Integer dorisBuckets = 10;
    private Integer dorisReplicationNum = 1;
    private String deleteSyncMode = "PHYSICAL_DELETE";
    private String logicalDeleteColumn = "__DORIS_DELETE_SIGN__";

    private Long offsetFlushMs = 10000L;
    private String outputMode = "JDBC_DML";
    private Integer enhancedBatchSize = 1000;
    private String enhancedOutputFile;
    private String deltaNullStrategy = "SKIP";
    private boolean changedFieldsEnabled = true;
    private boolean deltasEnabled = true;
    private boolean tombstoneAsDelete = false;

    private boolean scheduleEnabled = false;
    private String scheduleCron;
    private String scheduleRunMode = RunMode.RESUME_CDC.name();

    private boolean truncateBeforeFull = true;
    private boolean dropBeforeFull = false;
    private boolean forceRecreateSlotOnForceRun = false;

    private String workDir;

    private Long createdAt;
    private Long updatedAt;
}
