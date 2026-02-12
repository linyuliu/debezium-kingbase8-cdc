package io.debezium.connector.kingbasees.console.dto.request;

import io.debezium.connector.kingbasees.console.model.SyncTaskConfig;
import lombok.Data;

/**
 * 同步任务创建/更新请求。
 * 使用包装类型是为了区分“未传值”和“显式传值”。
 */
@Data
public class TaskUpsertRequest {

    private String name;
    private String sourceDataSourceId;
    private String targetDataSourceId;

    private String includeTables;
    private String includeSchemas;

    private String serverName;
    private String serverId;

    private String slotName;
    private String pluginName;

    private Boolean initSlot;
    private Boolean recreateSlot;
    private Boolean slotDropOnStop;

    private Boolean replicaIdentityFull;
    private Boolean replicaIdentityFullFailFast;
    private String replicaIdentityFullTables;

    private String routeMode;
    private String dorisDatabase;
    private String dorisDatabasePrefix;
    private String dorisTablePrefix;
    private String dorisTableSuffix;
    private String dorisSchemaTableSeparator;

    private Boolean autoCreateTargetDatabase;
    private Boolean autoCreateTargetTable;
    private Boolean autoAddTargetColumns;
    private Boolean skipDeleteWithoutPk;

    private Integer dorisBuckets;
    private Integer dorisReplicationNum;
    private String deleteSyncMode;
    private String logicalDeleteColumn;

    private Long offsetFlushMs;
    private String outputMode;
    private Integer enhancedBatchSize;
    private String enhancedOutputFile;
    private String deltaNullStrategy;
    private Boolean changedFieldsEnabled;
    private Boolean deltasEnabled;
    private Boolean tombstoneAsDelete;

    private Boolean scheduleEnabled;
    private String scheduleCron;
    private String scheduleRunMode;

    private Boolean truncateBeforeFull;
    private Boolean dropBeforeFull;
    private Boolean forceRecreateSlotOnForceRun;

    private String workDir;

    public SyncTaskConfig toModel() {
        SyncTaskConfig task = new SyncTaskConfig();

        task.setName(name);
        task.setSourceDataSourceId(sourceDataSourceId);
        task.setTargetDataSourceId(targetDataSourceId);

        task.setIncludeTables(includeTables);
        task.setIncludeSchemas(includeSchemas);

        task.setServerName(serverName);
        task.setServerId(serverId);
        task.setSlotName(slotName);
        task.setPluginName(pluginName);

        if (initSlot != null) {
            task.setInitSlot(initSlot.booleanValue());
        }
        if (recreateSlot != null) {
            task.setRecreateSlot(recreateSlot.booleanValue());
        }
        if (slotDropOnStop != null) {
            task.setSlotDropOnStop(slotDropOnStop.booleanValue());
        }

        if (replicaIdentityFull != null) {
            task.setReplicaIdentityFull(replicaIdentityFull.booleanValue());
        }
        if (replicaIdentityFullFailFast != null) {
            task.setReplicaIdentityFullFailFast(replicaIdentityFullFailFast.booleanValue());
        }
        task.setReplicaIdentityFullTables(replicaIdentityFullTables);

        task.setRouteMode(routeMode);
        task.setDorisDatabase(dorisDatabase);
        task.setDorisDatabasePrefix(dorisDatabasePrefix);
        task.setDorisTablePrefix(dorisTablePrefix);
        task.setDorisTableSuffix(dorisTableSuffix);
        task.setDorisSchemaTableSeparator(dorisSchemaTableSeparator);

        if (autoCreateTargetDatabase != null) {
            task.setAutoCreateTargetDatabase(autoCreateTargetDatabase.booleanValue());
        }
        if (autoCreateTargetTable != null) {
            task.setAutoCreateTargetTable(autoCreateTargetTable.booleanValue());
        }
        if (autoAddTargetColumns != null) {
            task.setAutoAddTargetColumns(autoAddTargetColumns.booleanValue());
        }
        if (skipDeleteWithoutPk != null) {
            task.setSkipDeleteWithoutPk(skipDeleteWithoutPk.booleanValue());
        }

        if (dorisBuckets != null) {
            task.setDorisBuckets(dorisBuckets);
        }
        if (dorisReplicationNum != null) {
            task.setDorisReplicationNum(dorisReplicationNum);
        }
        task.setDeleteSyncMode(deleteSyncMode);
        task.setLogicalDeleteColumn(logicalDeleteColumn);
        if (offsetFlushMs != null) {
            task.setOffsetFlushMs(offsetFlushMs);
        }
        task.setOutputMode(outputMode);
        if (enhancedBatchSize != null) {
            task.setEnhancedBatchSize(enhancedBatchSize);
        }
        task.setEnhancedOutputFile(enhancedOutputFile);
        task.setDeltaNullStrategy(deltaNullStrategy);
        if (changedFieldsEnabled != null) {
            task.setChangedFieldsEnabled(changedFieldsEnabled.booleanValue());
        }
        if (deltasEnabled != null) {
            task.setDeltasEnabled(deltasEnabled.booleanValue());
        }
        if (tombstoneAsDelete != null) {
            task.setTombstoneAsDelete(tombstoneAsDelete.booleanValue());
        }

        if (scheduleEnabled != null) {
            task.setScheduleEnabled(scheduleEnabled.booleanValue());
        }
        task.setScheduleCron(scheduleCron);
        task.setScheduleRunMode(scheduleRunMode);

        if (truncateBeforeFull != null) {
            task.setTruncateBeforeFull(truncateBeforeFull.booleanValue());
        }
        if (dropBeforeFull != null) {
            task.setDropBeforeFull(dropBeforeFull.booleanValue());
        }
        if (forceRecreateSlotOnForceRun != null) {
            task.setForceRecreateSlotOnForceRun(forceRecreateSlotOnForceRun.booleanValue());
        }

        task.setWorkDir(workDir);
        return task;
    }
}
