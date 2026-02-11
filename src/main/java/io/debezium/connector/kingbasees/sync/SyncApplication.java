package io.debezium.connector.kingbasees.sync;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableConfigurationProperties(SyncProperties.class)
public class SyncApplication {

    public static void main(String[] args) {
        new SpringApplicationBuilder(SyncApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
