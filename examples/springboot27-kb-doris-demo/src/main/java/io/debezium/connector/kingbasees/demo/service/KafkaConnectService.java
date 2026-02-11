package io.debezium.connector.kingbasees.demo.service;

import io.debezium.connector.kingbasees.demo.SyncProperties;
import io.debezium.connector.kingbasees.demo.model.DorisTable;
import io.debezium.connector.kingbasees.demo.model.TableId;
import io.debezium.connector.kingbasees.demo.model.TableMeta;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.debezium.connector.kingbasees.demo.util.DemoUtil.lower;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.parseStringList;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.trimRightSlash;
import static io.debezium.connector.kingbasees.demo.util.DemoUtil.urlEncode;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConnectService {

    private final SyncProperties properties;
    private final DorisSchemaService dorisSchemaService;
    private final RestTemplateBuilder restTemplateBuilder;

    private RestTemplate restTemplate;

    private RestTemplate client() {
        if (restTemplate == null) {
            restTemplate = restTemplateBuilder
                    .setConnectTimeout(Duration.ofSeconds(10))
                    .setReadTimeout(Duration.ofSeconds(30))
                    .build();
        }
        return restTemplate;
    }

    public void recreateConnectorsIfNeeded() {
        if (!properties.isRecreateConnectors()) {
            return;
        }
        deleteConnector(properties.getKafkaConnect().getSinkConnectorName());
        deleteConnector(properties.getSource().getSourceConnectorName());
    }

    /**
     * 提交 source connector 配置（幂等 PUT）。
     */
    public void upsertSourceConnector(List<TableId> includedTables) {
        String name = properties.getSource().getSourceConnectorName();
        upsertConnector(name, buildSourceConnectorConfig(includedTables));
    }

    /**
     * 提交 Doris sink connector 配置，启用 debezium ingestion + schema evolution。
     */
    public void upsertSinkConnector(List<TableMeta> metas) {
        String name = properties.getKafkaConnect().getSinkConnectorName();
        upsertConnector(name, buildSinkConnectorConfig(metas));
    }

    public void printConnectorStatus() {
        logConnectorStatus(properties.getSource().getSourceConnectorName());
        logConnectorStatus(properties.getKafkaConnect().getSinkConnectorName());
    }

    private Map<String, String> buildSourceConnectorConfig(List<TableId> includedTables) {
        SyncProperties.Source source = properties.getSource();
        Map<String, String> cfg = new LinkedHashMap<String, String>();
        cfg.put("connector.class", "io.debezium.connector.kingbasees.PostgresConnector");
        cfg.put("database.hostname", source.getHost());
        cfg.put("database.port", String.valueOf(source.getPort()));
        cfg.put("database.user", source.getUser());
        cfg.put("database.password", source.getPassword());
        cfg.put("database.dbname", source.getDatabase());
        cfg.put("database.server.name", source.getServerName());
        cfg.put("plugin.name", source.getPluginName());
        cfg.put("slot.name", source.getSlotName());
        cfg.put("slot.drop.on.stop", String.valueOf(source.isSlotDropOnStop()));
        cfg.put("snapshot.mode", source.getSnapshotMode());
        cfg.put("table.include.list", includedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));
        List<String> schemas = parseStringList(source.getIncludeSchemas());
        if (!schemas.isEmpty()) {
            cfg.put("schema.include.list", String.join(",", schemas));
        }
        cfg.put("tombstones.on.delete", "false");
        cfg.put("decimal.handling.mode", "string");
        return cfg;
    }

    private Map<String, String> buildSinkConnectorConfig(List<TableMeta> metas) {
        SyncProperties.KafkaConnect connect = properties.getKafkaConnect();
        SyncProperties.Doris doris = properties.getDoris();
        SyncProperties.Source source = properties.getSource();

        List<String> topics = new ArrayList<String>();
        List<String> mapItems = new ArrayList<String>();
        for (TableMeta meta : metas) {
            String topic = source.getServerName() + "." + meta.getTableId().getSchema() + "." + meta.getTableId().getTable();
            DorisTable target = dorisSchemaService.route(meta.getTableId());
            topics.add(topic);
            mapItems.add(topic + ":" + target.getDatabase() + "." + target.getTable());
        }

        Map<String, String> cfg = new LinkedHashMap<String, String>();
        cfg.put("connector.class", "org.apache.doris.kafka.connector.DorisSinkConnector");
        cfg.put("tasks.max", String.valueOf(connect.getSinkTasksMax()));
        cfg.put("topics", String.join(",", topics));
        cfg.put("doris.topic2table.map", String.join(",", mapItems));
        cfg.put("doris.urls", connect.getDorisFeNodes());
        cfg.put("doris.http.port", String.valueOf(connect.getDorisHttpPort()));
        cfg.put("doris.query.port", String.valueOf(connect.getDorisQueryPort()));
        if ("schema_as_db".equals(lower(doris.getRouteMode()))) {
            cfg.put("doris.database", "");
        }
        else {
            cfg.put("doris.database", doris.getDatabase());
        }
        cfg.put("doris.user", doris.getUser());
        cfg.put("doris.password", doris.getPassword());
        cfg.put("buffer.count.records", String.valueOf(connect.getBufferCountRecords()));
        cfg.put("buffer.flush.time", String.valueOf(connect.getBufferFlushTimeMs()));
        cfg.put("max.retries", String.valueOf(connect.getMaxRetries()));
        cfg.put("enable.delete", "true");
        cfg.put("converter.mode", "debezium_ingestion");
        cfg.put("debezium.schema.evolution", "basic");
        cfg.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        cfg.put("key.converter.schemas.enable", "false");
        cfg.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        cfg.put("value.converter.schemas.enable", "false");
        return cfg;
    }

    private void upsertConnector(String name, Map<String, String> config) {
        String path = "/connectors/" + urlEncode(name) + "/config";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> body = new HttpEntity<Map<String, String>>(config, headers);
        ResponseEntity<String> response = client().exchange(connectUrl(path), HttpMethod.PUT, body, String.class);
        log.info("[Connect] upserted connector {} (status={})", name, response.getStatusCodeValue());
    }

    private void deleteConnector(String name) {
        String path = "/connectors/" + urlEncode(name);
        try {
            client().exchange(connectUrl(path), HttpMethod.DELETE, null, String.class);
            log.info("[Connect] deleted connector {}", name);
        }
        catch (HttpClientErrorException.NotFound e) {
            log.info("[Connect] connector not exists: {}", name);
        }
    }

    private void logConnectorStatus(String name) {
        String path = "/connectors/" + urlEncode(name) + "/status";
        try {
            ResponseEntity<String> response = client().exchange(connectUrl(path), HttpMethod.GET, null, String.class);
            log.info("[Connect] status {} => {}", name, response.getBody());
        }
        catch (Exception e) {
            log.warn("[Connect] failed to query status {}: {}", name, e.getMessage());
        }
    }

    private String connectUrl(String path) {
        return trimRightSlash(properties.getKafkaConnect().getConnectUrl()) + path;
    }
}

