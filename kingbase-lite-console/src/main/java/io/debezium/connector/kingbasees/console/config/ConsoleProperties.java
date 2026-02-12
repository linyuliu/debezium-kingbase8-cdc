package io.debezium.connector.kingbasees.console.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "lite.console")
public class ConsoleProperties {

    private String dataDir = ".lite-console";
    private String javaCommand = "java";
    private int maxLogLines = 300;
}
