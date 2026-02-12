package io.debezium.connector.kingbasees.console.dto.request;

import io.debezium.connector.kingbasees.console.model.DataSourceConfig;
import io.debezium.connector.kingbasees.console.model.DataSourceType;
import lombok.Data;

/**
 * 数据源创建/更新请求。
 */
@Data
public class DataSourceUpsertRequest {

    private String name;
    private DataSourceType type;
    private String host;
    private Integer port;
    private String databaseName;
    private String username;
    private String password;
    private String params;

    public DataSourceConfig toModel() {
        DataSourceConfig config = new DataSourceConfig();
        config.setName(name);
        config.setType(type);
        config.setHost(host);
        config.setPort(port);
        config.setDatabaseName(databaseName);
        config.setUsername(username);
        config.setPassword(password);
        config.setParams(params);
        return config;
    }
}
