package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 增强 JSON 批次输出器。
 * 用于把 CDC 事件转换结果按批输出为 JSON 数组，可对接后续 HTTP/Webhook 推送层。
 */
final class EnhancedJsonBatchEmitter implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedJsonBatchEmitter.class);

    private final int batchSize;
    private final String outputFile;
    private final List<JSONObject> buffer = new ArrayList<JSONObject>();
    private long batchNo = 0L;
    private long totalRows = 0L;

    EnhancedJsonBatchEmitter(SyncConfig config) {
        this.batchSize = Math.max(config.enhancedBatchSize, 1);
        this.outputFile = config.enhancedOutputFile;
        if (!SinkSupport.isBlank(outputFile)) {
            ensureOutputFile(outputFile);
            LOGGER.info("[增强输出] 已启用文件输出：{}", outputFile);
        }
        LOGGER.info("[增强输出] 批次大小={}", batchSize);
    }

    synchronized void append(EnhancedCdcRecord record) {
        if (record == null) {
            return;
        }

        buffer.add(record.toEnhancedJson());
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        batchNo++;
        totalRows += buffer.size();
        String payload = JSON.toJSONString(buffer);
        if (!SinkSupport.isBlank(outputFile)) {
            appendLine(outputFile, payload);
        }

        LOGGER.info("[增强输出] 已输出批次：batchNo={}，batchRows={}，totalRows={}", batchNo, buffer.size(), totalRows);
        buffer.clear();
    }

    @Override
    public synchronized void close() {
        flush();
    }

    private static void ensureOutputFile(String outputFile) {
        Path path = Paths.get(outputFile).toAbsolutePath();
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("增强 JSON 输出文件初始化失败：" + outputFile, e);
        }
    }

    private static void appendLine(String outputFile, String line) {
        try {
            Files.write(Paths.get(outputFile),
                    (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
        catch (IOException e) {
            throw new IllegalStateException("写入增强 JSON 输出文件失败：" + outputFile, e);
        }
    }
}
