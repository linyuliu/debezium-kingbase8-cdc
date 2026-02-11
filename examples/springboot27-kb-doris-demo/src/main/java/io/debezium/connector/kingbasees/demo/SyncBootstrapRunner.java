package io.debezium.connector.kingbasees.demo;

import io.debezium.connector.kingbasees.demo.model.TableId;
import io.debezium.connector.kingbasees.demo.model.TableMeta;
import io.debezium.connector.kingbasees.demo.service.DorisSchemaService;
import io.debezium.connector.kingbasees.demo.service.KafkaConnectService;
import io.debezium.connector.kingbasees.demo.service.SourceMetadataService;
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
            log.info("[Boot] demo.enabled=false, bootstrap skipped");
            return;
        }

        Class.forName("cn.com.kingbase.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        log.info("==================================================");
        log.info("[Boot] Kingbase -> Doris bootstrap start");
        log.info("==================================================");

        try (Connection sourceConn = sourceMetadataService.openSourceConnection();
             Connection dorisConn = dorisSchemaService.openDorisConnection()) {

            // Stage 1: resolve source tables
            List<TableId> includedTables = sourceMetadataService.resolveIncludedTables(sourceConn);
            if (includedTables.isEmpty()) {
                throw new IllegalStateException("No source tables resolved. Configure demo.source.include-tables or demo.source.include-schemas.");
            }
            log.info("[1/5] source tables: {}", includedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));

            // Stage 2: source-side preparation
            sourceMetadataService.initSlotIfNeeded(sourceConn);
            if (properties.getSource().isApplyReplicaIdentityFull()) {
                sourceMetadataService.applyReplicaIdentityFull(sourceConn, includedTables);
            }
            log.info("[2/5] source preparation done");

            // Stage 3: extract source metadata and bootstrap Doris
            List<TableMeta> tableMetas = sourceMetadataService.loadTableMetas(sourceConn, includedTables);
            dorisSchemaService.bootstrapTables(dorisConn, tableMetas);
            log.info("[3/5] Doris schema bootstrap done");

            // Stage 4: (optional) recreate connectors
            kafkaConnectService.recreateConnectorsIfNeeded();
            log.info("[4/5] connector cleanup done");

            // Stage 5: upsert source/sink connectors and check status
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

