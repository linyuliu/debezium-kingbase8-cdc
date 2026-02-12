package io.debezium.connector.kingbasees.console.model;

import lombok.Data;

@Data
public class DataSourceConfig {

    private String id;
    private String name;
    private DataSourceType type;

    private String host;
    private Integer port;
    private String databaseName;
    private String username;
    private String password;
    private String params;

    private Long createdAt;
    private Long updatedAt;
}
