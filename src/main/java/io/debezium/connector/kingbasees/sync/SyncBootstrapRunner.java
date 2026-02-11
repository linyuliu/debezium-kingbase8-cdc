package io.debezium.connector.kingbasees.sync;

import io.debezium.connector.kingbasees.sync.model.TableId;
import io.debezium.connector.kingbasees.sync.model.TableMeta;
import io.debezium.connector.kingbasees.sync.service.DorisSchemaService;
import io.debezium.connector.kingbasees.sync.service.KafkaConnectService;
import io.debezium.connector.kingbasees.sync.service.SourceMetadataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SyncBootstrapRunner implements CommandLineRunner {

    private final SyncProperties properties;
    private final SourceMetadataService sourceMetadataService;
    private final DorisSchemaService dorisSchemaService;
    private final KafkaConnectService kafkaConnectService;

    @Override
    public void run(String... args) throws Exception {
        if (!properties.isEnabled()) {
            log.info("[Boot] sync.enabled=false, bootstrap skipped");
            return;
        }

        Class.forName("cn.com.kingbase.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        log.info("==================================================");
        log.info("[Boot] Kingbase -> Doris bootstrap start");
        log.info("==================================================");

        try (Connection sourceConn = sourceMetadataService.openSourceConnection();
             Connection dorisConn = dorisSchemaService.openDorisConnection()) {

            // 阶段 1：解析本次同步的源表范围（支持 include-tables / include-schemas）
            List<TableId> includedTables = sourceMetadataService.resolveIncludedTables(sourceConn);
            if (includedTables.isEmpty()) {
                throw new IllegalStateException("No source tables resolved. Configure sync.source.include-tables or sync.source.include-schemas.");
            }
            log.info("[1/5] source tables: {}", includedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));

            // 阶段 2：源端准备（slot 初始化 + 可选 REPLICA IDENTITY FULL）
            sourceMetadataService.initSlotIfNeeded(sourceConn);
            if (properties.getSource().isApplyReplicaIdentityFull()) {
                sourceMetadataService.applyReplicaIdentityFull(sourceConn, includedTables);
            }
            log.info("[2/5] source preparation done");

            // 阶段 3：提取源表元数据，并在 Doris 侧自动建表/补列
            List<TableMeta> tableMetas = sourceMetadataService.loadTableMetas(sourceConn, includedTables);
            dorisSchemaService.bootstrapTables(dorisConn, tableMetas);
            log.info("[3/5] Doris schema bootstrap done");

            // 阶段 4：可选重建 Connect 任务（便于重跑）
            kafkaConnectService.recreateConnectorsIfNeeded();
            log.info("[4/5] connector cleanup done");

            // 阶段 5：提交 Source/Sink 配置并输出状态
            kafkaConnectService.upsertSourceConnector(includedTables);
            kafkaConnectService.upsertSinkConnector(tableMetas);
            kafkaConnectService.printConnectorStatus();
            log.info("[5/5] connector upsert done");
        }

        log.info("==================================================");
        log.info("[Boot] bootstrap completed");
        log.info("==================================================");
    }
}
