package io.debezium.connector.kingbasees.sink;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 轻量同步程序入口：
 * 负责生命周期编排，不承载业务细节。
 */
public final class KingbaseToDorisSyncApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KingbaseToDorisSyncApp.class);

    private KingbaseToDorisSyncApp() {
    }

    public static void main(String[] args) throws Exception {
        SyncConfig config = SyncConfig.load();
        LOGGER.info("[同步引擎] 准备启动 Kingbase -> Doris 同步任务");
        config.printSummary(LOGGER);
        SinkSupport.ensureWorkDir(config.workDir);

        Class.forName("com.kingbase8.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection sourceConnection = null;
        Connection dorisConnection = null;
        SyncWriter writer = null;
        DebeziumEngine<ChangeEvent<String, String>> engine = null;
        ExecutorService executor = null;

        try {
            sourceConnection = DriverManager.getConnection(config.sourceJdbcUrl(), config.kbUser, config.kbPassword);
            dorisConnection = DriverManager.getConnection(config.dorisJdbcUrl(), config.dorisUser, config.dorisPassword);

            SourceAdmin sourceAdmin = new SourceAdmin(sourceConnection, config);
            DorisAdmin dorisAdmin = new DorisAdmin(dorisConnection, config, sourceAdmin);

            sourceAdmin.initSlotIfNeeded();
            sourceAdmin.applyReplicaIdentityFullIfNeeded();
            dorisAdmin.applyStartupActions();

            writer = new SyncWriter(sourceAdmin, dorisAdmin, dorisConnection, config);
            engine = DebeziumEngineFactory.build(config, writer);

            executor = Executors.newSingleThreadExecutor();
            executor.submit(engine);
            addShutdownHook(engine);
            awaitTermination(executor);
            LOGGER.info("[同步引擎] 同步任务执行结束");
        }
        finally {
            SinkSupport.closeQuietly(engine);
            SinkSupport.closeQuietly(writer);
            SinkSupport.closeQuietly(sourceConnection);
            SinkSupport.closeQuietly(dorisConnection);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("[同步引擎] 收到进程退出信号，正在关闭 Debezium 引擎");
            SinkSupport.closeQuietly(engine);
        }));
    }

    private static void awaitTermination(ExecutorService executor) throws InterruptedException {
        int loops = 0;
        executor.shutdown();
        while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            loops++;
            if (loops % 6 == 0) {
                LOGGER.info("[同步引擎] 任务运行中，持续等待 CDC 事件...");
            }
        }
    }
}
