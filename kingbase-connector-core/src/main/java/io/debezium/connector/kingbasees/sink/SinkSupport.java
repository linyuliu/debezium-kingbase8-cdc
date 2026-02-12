package io.debezium.connector.kingbasees.sink;

import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 通用工具方法，集中管理字符串处理、配置解析、类型映射与资源关闭逻辑。
 */
final class SinkSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkSupport.class);

    private SinkSupport() {
    }

    static void ensureWorkDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("创建工作目录失败：" + dirPath);
        }
    }

    static void closeQuietly(Object closable) {
        try {
            if (closable instanceof DebeziumEngine) {
                ((DebeziumEngine<?>) closable).close();
            }
            else if (closable instanceof Closeable) {
                ((Closeable) closable).close();
            }
            else if (closable instanceof Connection) {
                ((Connection) closable).close();
            }
        }
        catch (Exception ignored) {
        }
    }

    static String text(JSONObject node, String field) {
        if (node == null) {
            return null;
        }
        Object value = node.get(field);
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    static Integer toInteger(ResultSet rs, String field) throws SQLException {
        int value = rs.getInt(field);
        return rs.wasNull() ? null : value;
    }

    static String mapToDorisType(String dataType, String udtName, Integer precision, Integer scale, Integer length) {
        String type = !isBlank(dataType) ? dataType : udtName;
        type = lower(type);

        if (containsAny(type, "int2", "smallint", "smallserial")) {
            return "SMALLINT";
        }
        if (containsAny(type, "int8", "bigint", "bigserial")) {
            return "BIGINT";
        }
        if (containsAny(type, "int4", "integer", "serial") || "int".equals(type)) {
            return "INT";
        }
        if (containsAny(type, "float4", "real")) {
            return "FLOAT";
        }
        if (containsAny(type, "float8", "double")) {
            return "DOUBLE";
        }
        if (containsAny(type, "numeric", "decimal", "money")) {
            int s = scale == null || scale < 0 ? 4 : scale;
            return "DECIMAL(38," + Math.min(s, 18) + ")";
        }
        if (containsAny(type, "bool")) {
            return "BOOLEAN";
        }
        if (containsAny(type, "date")) {
            return "DATE";
        }
        if (containsAny(type, "timestamp", "time")) {
            return "DATETIME";
        }
        if (containsAny(type, "char", "text", "json", "uuid", "xml", "inet", "cidr", "macaddr")) {
            return "STRING";
        }
        if (containsAny(type, "bytea", "blob", "binary")) {
            return "STRING";
        }
        if (length != null && length > 0 && length <= 65533) {
            return "VARCHAR(" + length + ")";
        }
        return "STRING";
    }

    static boolean containsAny(String text, String... values) {
        if (text == null) {
            return false;
        }
        for (String value : values) {
            if (text.contains(value)) {
                return true;
            }
        }
        return false;
    }

    static String backtick(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    static String lower(String text) {
        return text == null ? "" : text.toLowerCase(Locale.ROOT);
    }

    static String sanitizeName(String raw) {
        if (raw == null || raw.isEmpty()) {
            return raw;
        }
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(Character.toLowerCase(c));
            }
            else {
                sb.append('_');
            }
        }
        String result = sb.toString();
        if (result.isEmpty()) {
            return "t";
        }
        if (Character.isDigit(result.charAt(0))) {
            return "t_" + result;
        }
        return result;
    }

    static String getSetting(String sysKey, String envKey, String defaultValue) {
        String sysValue = System.getProperty(sysKey);
        if (!isBlank(sysValue)) {
            return sysValue.trim();
        }
        String envValue = System.getenv(envKey);
        if (!isBlank(envValue)) {
            return envValue.trim();
        }
        return defaultValue;
    }

    static boolean parseBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }

    static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        }
        catch (Exception ignored) {
            return defaultValue;
        }
    }

    static long parseLong(String value, long defaultValue) {
        try {
            return Long.parseLong(value);
        }
        catch (Exception ignored) {
            return defaultValue;
        }
    }

    static String normalizeCsvList(String raw) {
        if (raw == null) {
            return "";
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(","));
    }

    static List<String> parseStringList(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<String>();
        for (String text : csv.split(",")) {
            String value = text.trim();
            if (!value.isEmpty()) {
                values.add(value);
            }
        }
        return values;
    }

    static List<SourceTableId> parseTableList(String csv) {
        if (isBlank(csv)) {
            return Collections.emptyList();
        }

        Map<String, SourceTableId> unique = new LinkedHashMap<String, SourceTableId>();
        for (String item : csv.split(",")) {
            String text = item.trim();
            if (text.isEmpty()) {
                continue;
            }
            String[] seg = text.split("\\.");
            if (seg.length != 2) {
                LOGGER.warn("[同步引擎] 忽略非法表配置（必须是 schema.table）：{}", text);
                continue;
            }
            SourceTableId id = new SourceTableId(seg[0], seg[1]);
            unique.put(id.toString(), id);
        }
        return new ArrayList<SourceTableId>(unique.values());
    }

    static String joinTables(List<SourceTableId> tables) {
        if (tables == null || tables.isEmpty()) {
            return "<无>";
        }
        return tables.stream().map(SourceTableId::toString).collect(Collectors.joining(","));
    }

    static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    static boolean valuesEqual(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }

        BigDecimal leftNumber = toBigDecimal(left);
        BigDecimal rightNumber = toBigDecimal(right);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber) == 0;
        }
        return String.valueOf(left).equals(String.valueOf(right));
    }

    static BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            try {
                return new BigDecimal(String.valueOf(value));
            }
            catch (Exception ignored) {
                return null;
            }
        }
        if (value instanceof String) {
            String text = ((String) value).trim();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return new BigDecimal(text);
            }
            catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }
}
