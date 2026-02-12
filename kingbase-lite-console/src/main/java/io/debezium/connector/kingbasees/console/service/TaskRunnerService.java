package io.debezium.connector.kingbasees.console.service;

import io.debezium.connector.kingbasees.console.LiteConsoleApplication;
import io.debezium.connector.kingbasees.console.config.ConsoleProperties;
import io.debezium.connector.kingbasees.console.model.DataSourceConfig;
import io.debezium.connector.kingbasees.console.model.DataSourceType;
import io.debezium.connector.kingbasees.console.model.RunMode;
import io.debezium.connector.kingbasees.console.model.SyncTaskConfig;
import io.debezium.connector.kingbasees.console.model.TaskRuntimeInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TaskRunnerService {

    private static final String ENGINE_MAIN_CLASS = "io.debezium.connector.kingbasees.sink.KingbaseToDorisSyncApp";
    private static final String CHILD_ENGINE_PROPERTY = "lite.child.engine";

    private final ConsoleProperties properties;
    private final DataSourceService dataSourceService;

    private final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    private final ExecutorService ioExecutor = Executors.newCachedThreadPool();

    private final Map<String, ManagedProcess> runningMap = new ConcurrentHashMap<String, ManagedProcess>();
    private final Map<String, TaskRuntimeInfo> runtimeMap = new ConcurrentHashMap<String, TaskRuntimeInfo>();
    private final Map<String, ScheduledFuture<?>> scheduleMap = new ConcurrentHashMap<String, ScheduledFuture<?>>();

    public TaskRunnerService(ConsoleProperties properties, DataSourceService dataSourceService) {
        this.properties = properties;
        this.dataSourceService = dataSourceService;
    }

    @PostConstruct
    public void init() {
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("lite-sync-scheduler-");
        scheduler.initialize();
    }

    @PreDestroy
    public void destroy() {
        for (String taskId : new ArrayList<String>(runningMap.keySet())) {
            stopTask(taskId);
        }
        for (ScheduledFuture<?> future : scheduleMap.values()) {
            future.cancel(false);
        }
        scheduler.shutdown();
        ioExecutor.shutdownNow();
    }

    public synchronized void reloadSchedules(List<SyncTaskConfig> tasks) {
        for (ScheduledFuture<?> future : scheduleMap.values()) {
            future.cancel(false);
        }
        scheduleMap.clear();

        for (SyncTaskConfig task : tasks) {
            refreshSchedule(task);
        }
    }

    public synchronized void refreshSchedule(SyncTaskConfig task) {
        ScheduledFuture<?> previous = scheduleMap.remove(task.getId());
        if (previous != null) {
            previous.cancel(false);
        }

        if (!task.isScheduleEnabled()) {
            return;
        }
        if (isBlank(task.getScheduleCron())) {
            throw new IllegalArgumentException("开启定时后必须填写 scheduleCron");
        }
        if (!CronSequenceGenerator.isValidExpression(task.getScheduleCron())) {
            throw new IllegalArgumentException("无效的 scheduleCron: " + task.getScheduleCron());
        }

        RunMode runMode = RunMode.fromText(task.getScheduleRunMode());
        ScheduledFuture<?> future = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    runTask(task, runMode, "schedule");
                }
                catch (Exception e) {
                    TaskRuntimeInfo info = runtime(task.getId());
                    info.setMessage("定时触发失败: " + e.getMessage());
                    info.setLastStopAt(System.currentTimeMillis());
                    log.error("[任务调度] 定时触发失败 taskId={}", task.getId(), e);
                }
            }
        }, new CronTrigger(task.getScheduleCron()));
        scheduleMap.put(task.getId(), future);
    }

    public synchronized void removeTask(String taskId) {
        ScheduledFuture<?> future = scheduleMap.remove(taskId);
        if (future != null) {
            future.cancel(false);
        }
        stopTask(taskId);
        runningMap.remove(taskId);
        runtimeMap.remove(taskId);
    }

    public synchronized TaskRuntimeInfo runTask(SyncTaskConfig task, RunMode runMode, String trigger) {
        String taskId = task.getId();
        ManagedProcess current = runningMap.get(taskId);
        if (current != null && current.isAlive()) {
            TaskRuntimeInfo info = runtime(taskId);
            info.setMessage("任务已在运行中");
            return info;
        }

        DataSourceConfig source = dataSourceService.getRequired(task.getSourceDataSourceId());
        DataSourceConfig target = dataSourceService.getRequired(task.getTargetDataSourceId());
        if (source.getType() != DataSourceType.KINGBASE) {
            throw new IllegalArgumentException("源数据源类型必须是 KINGBASE");
        }
        if (target.getType() != DataSourceType.DORIS) {
            throw new IllegalArgumentException("目标数据源类型必须是 DORIS");
        }

        Path workDir = resolveTaskWorkDir(task);
        Path offsetFile = workDir.resolve("offset.dat");
        Path historyFile = workDir.resolve("history.dat");
        prepareWorkFiles(runMode, offsetFile, historyFile);

        List<String> command = buildCommand(task, source, target, runMode, workDir, offsetFile, historyFile);
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(workDir.toFile());

        try {
            Process process = processBuilder.start();
            ManagedProcess managed = new ManagedProcess(taskId, process, properties.getMaxLogLines());
            runningMap.put(taskId, managed);

            TaskRuntimeInfo info = runtime(taskId);
            info.setRunning(true);
            info.setLastRunMode(runMode.name());
            info.setLastTrigger(trigger);
            info.setLastStartAt(System.currentTimeMillis());
            info.setLastStopAt(null);
            info.setLastExitCode(null);
            info.setMessage("已启动");

            managed.appendLog("进程已启动, mode=" + runMode.name() + ", trigger=" + trigger);
            managed.appendLog("工作目录=" + workDir);

            ioExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    pipeToLogs(process.getInputStream(), managed, "OUT");
                }
            });
            ioExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    pipeToLogs(process.getErrorStream(), managed, "ERR");
                }
            });
            ioExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    waitForExit(taskId, process, managed);
                }
            });

            return info;
        }
        catch (IOException e) {
            TaskRuntimeInfo info = runtime(taskId);
            info.setRunning(false);
            info.setLastStopAt(System.currentTimeMillis());
            info.setMessage("启动失败: " + e.getMessage());
            throw new IllegalStateException("启动任务进程失败", e);
        }
    }

    public synchronized TaskRuntimeInfo stopTask(String taskId) {
        ManagedProcess managed = runningMap.get(taskId);
        TaskRuntimeInfo info = runtime(taskId);
        if (managed == null || !managed.isAlive()) {
            info.setRunning(false);
            info.setMessage("任务未运行");
            return info;
        }

        managed.appendLog("收到停止请求");
        Process process = managed.process;
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return info;
    }

    public TaskRuntimeInfo runtime(String taskId) {
        TaskRuntimeInfo info = runtimeMap.get(taskId);
        if (info != null) {
            return info;
        }
        TaskRuntimeInfo created = new TaskRuntimeInfo();
        created.setTaskId(taskId);
        created.setRunning(false);
        created.setMessage("尚未启动");
        runtimeMap.put(taskId, created);
        return created;
    }

    /**
     * 返回全部任务运行状态列表（按 taskId 排序）。
     */
    public List<TaskRuntimeInfo> listAllRuntime() {
        List<TaskRuntimeInfo> list = new ArrayList<TaskRuntimeInfo>(runtimeMap.values());
        Collections.sort(list, Comparator.comparing(TaskRuntimeInfo::getTaskId, Comparator.nullsLast(String::compareTo)));
        return list;
    }

    public List<String> logs(String taskId, Integer tail) {
        ManagedProcess managed = runningMap.get(taskId);
        if (managed == null) {
            return Collections.emptyList();
        }
        return managed.snapshotLogs(tail == null ? 200 : tail.intValue());
    }

    /**
     * 组装同步引擎启动命令：
     * 1) java -jar 启动控制台时：复用当前 jar，以子进程模式运行引擎
     * 2) IDE/spring-boot:run 场景：走 classpath 直接拉起引擎 main
     */
    private List<String> buildCommand(
            SyncTaskConfig task,
            DataSourceConfig source,
            DataSourceConfig target,
            RunMode mode,
            Path workDir,
            Path offsetFile,
            Path historyFile
    ) {
        Map<String, String> props = new HashMap<String, String>();

        props.put("sync.connector.name", "lite_" + task.getId());
        props.put("sync.work.dir", workDir.toString());
        props.put("sync.offset.file", offsetFile.toString());
        props.put("sync.history.file", historyFile.toString());
        props.put("sync.offset.flush.ms", String.valueOf(defaultLong(task.getOffsetFlushMs(), 10000L)));

        props.put("kb.host", source.getHost());
        props.put("kb.port", String.valueOf(source.getPort()));
        props.put("kb.user", source.getUsername());
        props.put("kb.password", defaultString(source.getPassword()));
        props.put("kb.db", source.getDatabaseName());
        props.put("kb.server.name", defaultString(task.getServerName(), "kingbase_server"));
        props.put("kb.server.id", defaultString(task.getServerId(), "54001"));
        props.put("kb.plugin", defaultString(task.getPluginName(), "decoderbufs"));
        props.put("kb.slot.name", defaultString(task.getSlotName(), "dbz_kingbase_slot"));
        props.put("kb.slot.init", String.valueOf(task.isInitSlot()));
        props.put("kb.slot.recreate", String.valueOf(resolveSlotRecreate(task, mode)));
        props.put("kb.slot.drop.on.stop", String.valueOf(task.isSlotDropOnStop()));
        props.put("kb.snapshot.mode", resolveSnapshotMode(mode));
        props.put("kb.replica.identity.full", String.valueOf(task.isReplicaIdentityFull()));
        props.put("kb.replica.identity.full.fail-fast", String.valueOf(task.isReplicaIdentityFullFailFast()));
        props.put("kb.replica.identity.full.tables", normalizeCsv(task.getReplicaIdentityFullTables()));
        props.put("kb.tables", normalizeCsv(task.getIncludeTables()));
        props.put("kb.schemas", normalizeCsv(task.getIncludeSchemas()));

        props.put("doris.host", target.getHost());
        props.put("doris.port", String.valueOf(target.getPort()));
        props.put("doris.user", target.getUsername());
        props.put("doris.password", defaultString(target.getPassword()));
        props.put("doris.database", defaultString(task.getDorisDatabase(), target.getDatabaseName()));
        props.put("doris.database.prefix", defaultString(task.getDorisDatabasePrefix(), "cdc_"));
        props.put("doris.table.prefix", defaultString(task.getDorisTablePrefix(), ""));
        props.put("doris.table.suffix", defaultString(task.getDorisTableSuffix(), ""));
        props.put("doris.schema.table.separator", defaultString(task.getDorisSchemaTableSeparator(), "__"));
        props.put("doris.route.mode", defaultString(task.getRouteMode(), "schema_table").toLowerCase(Locale.ROOT));
        props.put("doris.auto.create.database", String.valueOf(task.isAutoCreateTargetDatabase()));
        props.put("doris.auto.create.table", String.valueOf(task.isAutoCreateTargetTable()));
        props.put("doris.auto.add.columns", String.valueOf(task.isAutoAddTargetColumns()));
        props.put("doris.skip.delete.without.pk", String.valueOf(task.isSkipDeleteWithoutPk()));
        props.put("doris.buckets", String.valueOf(defaultInt(task.getDorisBuckets(), 10)));
        props.put("doris.replication.num", String.valueOf(defaultInt(task.getDorisReplicationNum(), 1)));
        props.put("doris.startup.drop.all.included", String.valueOf(shouldDropOnStartup(task, mode)));
        props.put("doris.startup.truncate.all.included", String.valueOf(shouldTruncateOnStartup(task, mode)));
        props.put("sync.delete.mode", defaultString(task.getDeleteSyncMode(), "PHYSICAL_DELETE").toLowerCase(Locale.ROOT));
        props.put("doris.logical.delete.column", defaultString(task.getLogicalDeleteColumn(), "__DORIS_DELETE_SIGN__"));

        props.put("sync.output.mode", defaultString(task.getOutputMode(), "JDBC_DML").toLowerCase(Locale.ROOT));
        props.put("sync.enhanced.batch.size", String.valueOf(defaultInt(task.getEnhancedBatchSize(), 1000)));
        props.put("sync.enhanced.output.file", defaultString(task.getEnhancedOutputFile(), ""));
        props.put("sync.delta.null.strategy", defaultString(task.getDeltaNullStrategy(), "SKIP").toLowerCase(Locale.ROOT));
        props.put("sync.changed.fields.enabled", String.valueOf(task.isChangedFieldsEnabled()));
        props.put("sync.deltas.enabled", String.valueOf(task.isDeltasEnabled()));
        props.put("sync.tombstone.as.delete", String.valueOf(task.isTombstoneAsDelete()));

        return buildEngineJvmCommand(props);
    }

    private List<String> buildEngineJvmCommand(Map<String, String> props) {
        List<String> command = new ArrayList<String>();
        command.add(defaultString(properties.getJavaCommand(), "java"));

        Path currentJar = resolveCurrentJar();
        if (currentJar != null) {
            for (Map.Entry<String, String> entry : props.entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }
                command.add("-D" + entry.getKey() + "=" + entry.getValue());
            }
            command.add("-D" + CHILD_ENGINE_PROPERTY + "=true");
            command.add("-jar");
            command.add(currentJar.toString());
            return command;
        }

        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            command.add("-D" + entry.getKey() + "=" + entry.getValue());
        }
        command.add(ENGINE_MAIN_CLASS);
        return command;
    }

    private Path resolveCurrentJar() {
        try {
            URI uri = LiteConsoleApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path path = Paths.get(uri).toAbsolutePath();
            if (path.toString().endsWith(".jar") && Files.isRegularFile(path)) {
                return path;
            }
        }
        catch (Exception ignored) {
        }

        String command = System.getProperty("sun.java.command");
        if (command != null) {
            String[] parts = command.trim().split("\\s+");
            if (parts.length > 0) {
                Path path = Paths.get(parts[0]).toAbsolutePath();
                if (path.toString().endsWith(".jar") && Files.isRegularFile(path)) {
                    return path;
                }
            }
        }
        return null;
    }

    private void prepareWorkFiles(RunMode runMode, Path offsetFile, Path historyFile) {
        try {
            Files.createDirectories(offsetFile.getParent());
            if (runMode == RunMode.FULL_THEN_CDC || runMode == RunMode.FORCE_FULL_THEN_CDC) {
                Files.deleteIfExists(offsetFile);
                Files.deleteIfExists(historyFile);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("准备工作目录文件失败", e);
        }
    }

    private Path resolveTaskWorkDir(SyncTaskConfig task) {
        if (!isBlank(task.getWorkDir())) {
            Path path = Paths.get(task.getWorkDir());
            createDir(path);
            return path;
        }
        Path path = Paths.get(properties.getDataDir(), "tasks", task.getId());
        createDir(path);
        return path;
    }

    private void createDir(Path path) {
        try {
            Files.createDirectories(path);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to create directory: " + path, e);
        }
    }

    private String resolveSnapshotMode(RunMode mode) {
        if (mode == RunMode.FULL_THEN_CDC || mode == RunMode.FORCE_FULL_THEN_CDC) {
            return "initial";
        }
        return "never";
    }

    private boolean resolveSlotRecreate(SyncTaskConfig task, RunMode mode) {
        if (task.isRecreateSlot()) {
            return true;
        }
        return mode == RunMode.FORCE_FULL_THEN_CDC && task.isForceRecreateSlotOnForceRun();
    }

    private boolean shouldDropOnStartup(SyncTaskConfig task, RunMode mode) {
        if (mode == RunMode.FULL_THEN_CDC || mode == RunMode.FORCE_FULL_THEN_CDC) {
            return task.isDropBeforeFull();
        }
        return false;
    }

    private boolean shouldTruncateOnStartup(SyncTaskConfig task, RunMode mode) {
        if (mode == RunMode.FULL_THEN_CDC || mode == RunMode.FORCE_FULL_THEN_CDC) {
            return task.isTruncateBeforeFull();
        }
        return false;
    }

    private void waitForExit(String taskId, Process process, ManagedProcess managed) {
        int exitCode;
        try {
            exitCode = process.waitFor();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exitCode = -1;
        }

        managed.appendLog("进程退出, exitCode=" + exitCode);

        TaskRuntimeInfo info = runtime(taskId);
        info.setRunning(false);
        info.setLastStopAt(System.currentTimeMillis());
        info.setLastExitCode(exitCode);
        info.setMessage(exitCode == 0 ? "正常停止" : "异常停止");
    }

    private void pipeToLogs(InputStream inputStream, ManagedProcess managed, String streamType) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                managed.appendLog("[" + streamType + "] " + line);
            }
        }
        catch (IOException e) {
            managed.appendLog("[" + streamType + "] 读取日志流失败: " + e.getMessage());
        }
    }

    private int defaultInt(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    private long defaultLong(Long value, long defaultValue) {
        return value == null ? defaultValue : value;
    }

    private String defaultString(String value) {
        return value == null ? "" : value;
    }

    private String defaultString(String value, String defaultValue) {
        return isBlank(value) ? defaultValue : value;
    }

    private String normalizeCsv(String csv) {
        if (isBlank(csv)) {
            return "";
        }
        String[] parts = csv.split(",");
        List<String> values = new ArrayList<String>();
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return String.join(",", values);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static final class ManagedProcess {

        private final String taskId;
        private final Process process;
        private final Deque<String> logs;
        private final int maxLines;

        private ManagedProcess(String taskId, Process process, int maxLines) {
            this.taskId = taskId;
            this.process = process;
            this.maxLines = Math.max(50, maxLines);
            this.logs = new ArrayDeque<String>();
        }

        private boolean isAlive() {
            return process != null && process.isAlive();
        }

        private void appendLog(String message) {
            String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            String line = ts + " [" + taskId + "] " + message;
            synchronized (logs) {
                logs.addLast(line);
                while (logs.size() > maxLines) {
                    logs.removeFirst();
                }
            }
        }

        private List<String> snapshotLogs(int tail) {
            int limit = Math.max(1, tail);
            synchronized (logs) {
                List<String> copy = new ArrayList<String>(logs);
                if (copy.size() <= limit) {
                    return copy;
                }
                return copy.subList(copy.size() - limit, copy.size());
            }
        }
    }
}
