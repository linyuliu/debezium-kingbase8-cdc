package io.debezium.connector.kingbasees.sync.util;

import io.debezium.connector.kingbasees.sync.model.TableId;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class SyncUtil {

    private SyncUtil() {
    }

    public static List<String> parseStringList(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String item : csv.split(",")) {
            String value = item.trim();
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return result;
    }

    public static List<TableId> parseTableList(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, TableId> unique = new LinkedHashMap<String, TableId>();
        for (String item : csv.split(",")) {
            String value = item.trim();
            if (value.isEmpty()) {
                continue;
            }
            String[] split = value.split("\\.");
            if (split.length != 2) {
                throw new IllegalArgumentException("table must be schema.table, invalid: " + value);
            }
            TableId id = new TableId(split[0], split[1]);
            unique.put(id.toString(), id);
        }
        return new ArrayList<TableId>(unique.values());
    }

    public static String trimRightSlash(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        int i = url.length();
        while (i > 0 && url.charAt(i - 1) == '/') {
            i--;
        }
        return url.substring(0, i);
    }

    public static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        }
        catch (Exception e) {
            throw new IllegalStateException("url encode failed: " + value, e);
        }
    }

    public static Integer getNullableInt(ResultSet rs, String name) throws SQLException {
        int value = rs.getInt(name);
        return rs.wasNull() ? null : value;
    }

    public static String lower(String text) {
        return text == null ? "" : text.toLowerCase(Locale.ROOT);
    }

    public static String sanitizeName(String raw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(Character.toLowerCase(c));
            }
            else {
                sb.append('_');
            }
        }
        String value = sb.toString();
        if (value.isEmpty()) {
            return "t";
        }
        if (Character.isDigit(value.charAt(0))) {
            return "t_" + value;
        }
        return value;
    }

    public static String mapToDorisType(String dataType, String udtName, Integer precision, Integer scale, Integer length) {
        String type = lower((dataType == null || dataType.isEmpty()) ? udtName : dataType);
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
        if (containsAny(type, "decimal", "numeric", "money")) {
            int s = scale == null || scale < 0 ? 4 : scale.intValue();
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
        if (containsAny(type, "char", "text", "json", "uuid", "xml", "inet", "cidr", "macaddr", "bytea")) {
            return "STRING";
        }
        if (precision != null && scale != null && precision > 0) {
            return "DECIMAL(" + Math.min(precision, 38) + "," + Math.min(Math.max(scale, 0), 18) + ")";
        }
        if (length != null && length > 0 && length <= 65533) {
            return "VARCHAR(" + length + ")";
        }
        return "STRING";
    }

    private static boolean containsAny(String value, String... hints) {
        for (String hint : hints) {
            if (value.contains(hint)) {
                return true;
            }
        }
        return false;
    }
}
