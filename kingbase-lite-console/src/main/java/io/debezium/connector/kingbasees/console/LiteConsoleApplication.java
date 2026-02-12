package io.debezium.connector.kingbasees.console;

import io.debezium.connector.kingbasees.console.config.ConsoleProperties;
import io.debezium.connector.kingbasees.sink.KingbaseToDorisSyncApp;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ConsoleProperties.class)
public class LiteConsoleApplication {

    /**
     * 主入口分两种模式：
     * 1) 控制台模式：启动 SpringBoot Web 管理端
     * 2) 子进程引擎模式：仅运行 CDC 引擎，不启动 Web
     */
    public static void main(String[] args) throws Exception {
        if ("true".equalsIgnoreCase(System.getProperty("lite.child.engine"))) {
            KingbaseToDorisSyncApp.main(args);
            return;
        }
        SpringApplication.run(LiteConsoleApplication.class, args);
    }
}
