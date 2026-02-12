/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbasees.connection;

import com.kingbase8.util.KBmoney;
import io.debezium.connector.kingbasees.PostgresStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.kingbasees.PostgresType;
import io.debezium.connector.kingbasees.TypeRegistry;
import io.debezium.connector.kingbasees.connection.ReplicationMessage.ColumnValue;
import io.debezium.connector.kingbasees.proto.PgProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * @author Chris Cranford
 */
public class ReplicationMessageColumnValueResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationMessageColumnValueResolver.class);
    private static final boolean DEBUG_DATUM = isDebugDatumEnabled();

    /**
     * Resolve the value of a {@link ColumnValue}.
     *
     * @param columnName the column name
     * @param type the postgres type
     * @param fullType the full type-name for the column
     * @param value the column value
     * @param connection a postgres connection supplier
     * @param includeUnknownDatatypes true to include unknown data types, false otherwise
     * @param typeRegistry the postgres type registry
     * @return
     */
    public static Object resolveValue(String columnName, PostgresType type, String fullType, ColumnValue value, final PgConnectionSupplier connection,
                                      boolean includeUnknownDatatypes, TypeRegistry typeRegistry) {
        if (value.isNull()) {
            // nulls are null
            return null;
        }

        if (DEBUG_DATUM) {
            String msg = String.format("[Proto字段解析] 字段='%s' 类型='%s' oid=%d 完整类型='%s' 原始值=%s",
                    columnName, type.getName(), type.getOid(), fullType, describeRawDatum(value.getRawValue()));
            LOGGER.info(msg);
        }

        if (!type.isRootType()) {
            return resolveValue(columnName, type.getParentType(), fullType, value, connection, includeUnknownDatatypes, typeRegistry);
        }

        if (value.isArray(type)) {
            return value.asArray(columnName, type, fullType, connection);
        }

        if (type.isEnumType()) {
            return value.asString();
        }

        final String typeName = normalizeTypeName(type.getName());
        final String fullTypeName = normalizeTypeName(fullType);
        final String effectiveTypeName = getEffectiveTypeName(typeName, fullTypeName);

        switch (effectiveTypeName) {
            // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
            // plus aliases from the shorter names produced by older wal2json
            case "boolean":
            case "bool":
                return value.asBoolean();

            case "hstore":
                return value.asString();

            case "integer":
            case "int":
            case "int4":
            case "smallint":
            case "int2":
            case "int1":
            case "int3":
            case "smallserial":
            case "serial":
            case "serial2":
            case "serial4":
            case "year":
            case "tinyint":
            case "mediumint":
            case "middleint":
                return value.asInteger();

            case "bigint":
            case "bigserial":
            case "int8":
            case "oid":
                return value.asLong();

            case "real":
            case "float4":
            case "float":
            case "binary_float":
                return value.asFloat();

            case "double precision":
            case "float8":
            case "binary_double":
                return value.asDouble();

            case "numeric":
            case "decimal":
            case "dec":
            case "number":
                return value.asDecimal();

            case "character":
            case "char":
            case "character varying":
            case "varchar":
            case "varchar2":
            case "bpchar":
            case "nvarchar":
            case "nvarchar2":
            case "nchar":
            case "text":
            case "tinytext":
            case "mediumtext":
            case "longtext":
            return value.asString();

            case "date":
                return value.asLocalDate();

            case "timestamp with time zone":
            case "timestamptz":
            case "datetimetz":
            case "timestampltz":
            case "timestamp with local time zone":
                return value.asOffsetDateTimeAtUtc();

            case "timestamp":
            case "timestamp without time zone":
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                return value.asInstant();

            case "time":
                return value.asTime();

            case "time without time zone":
                return value.asLocalTime();

            case "time with time zone":
            case "timetz":
                return value.asOffsetTimeUtc();

            case "bytea":
            case "blob":
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
                return value.asByteArray();

            // these are all PG-specific types and we use the JDBC representations
            // note that, with the exception of point, no converters for these types are implemented yet,
            // i.e. those values won't actually be propagated to the outbound message until that's the case
            case "box":
                return value.asBox();
            case "circle":
                return value.asCircle();
            case "interval":
                return value.asInterval();
            case "line":
                return value.asLine();
            case "lseg":
                return value.asLseg();
            case "money":
                final Object v = value.asMoney();
                return (v instanceof KBmoney) ? ((KBmoney) v).value : v;
            case "path":
                return value.asPath();
            case "point":
                return value.asPoint();
            case "polygon":
                return value.asPolygon();

            // PostGIS types are HexEWKB strings
            // ValueConverter turns them into the correct types
            case "geometry":
            case "geography":
                return value.asString();

            case "citext":
            case "bit":
            case "bit varying":
            case "varbit":
            case "json":
            case "jsonb":
            case "xml":
            case "uuid":
            case "tsrange":
            case "tstzrange":
            case "daterange":
            case "inet":
            case "cidr":
            case "macaddr":
            case "macaddr8":
            case "int4range":
            case "numrange":
            case "int8range":
                return value.asString();

            // catch-all for other known/builtin PG types
            // TODO: improve with more specific/useful classes here?
            case "pg_lsn":
            case "tsquery":
            case "tsvector":
            case "txid_snapshot":
                // catch-all for unknown (extension module/custom) types
            default:
                break;
        }

        return value.asDefault(typeRegistry, type.getOid(), columnName, fullType, includeUnknownDatatypes, connection);
    }

    private static String getEffectiveTypeName(String typeName, String fullTypeName) {
        if (typeName == null || typeName.isEmpty() || "unknown".equals(typeName)) {
            return fullTypeName;
        }
        return typeName;
    }

    private static String normalizeTypeName(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        int parenIndex = normalized.indexOf('(');
        if (parenIndex >= 0) {
            normalized = normalized.substring(0, parenIndex).trim();
        }
        normalized = normalized.replace('"', ' ').trim().replaceAll("\\s+", " ");
        return normalized;
    }

    private static boolean isDebugDatumEnabled() {
        return Boolean.getBoolean("kb.debug.datum") || isTruthy(System.getenv("KB_DEBUG_DATUM"));
    }

    private static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim();
        return !normalized.isEmpty()
                && !"false".equalsIgnoreCase(normalized)
                && !"0".equals(normalized)
                && !"no".equalsIgnoreCase(normalized);
    }

    private static String describeRawDatum(Object raw) {
        if (!(raw instanceof PgProto.DatumMessage)) {
            return String.valueOf(raw);
        }
        PgProto.DatumMessage datum = (PgProto.DatumMessage) raw;
        switch (datum.getDatumCase()) {
            case DATUM_NULL:
                return "null";
            case DATUM_INT32:
                return "int32:" + datum.getDatumInt32();
            case DATUM_INT64:
                return "int64:" + datum.getDatumInt64();
            case DATUM_FLOAT:
                return "float:" + datum.getDatumFloat();
            case DATUM_DOUBLE:
                return "double:" + datum.getDatumDouble();
            case DATUM_BOOL:
                return "bool:" + datum.getDatumBool();
            case DATUM_STRING:
                return "string:" + datum.getDatumString();
            case DATUM_BYTES:
                return "bytes(len=" + datum.getDatumBytes().size() + ")";
            case DATUM_POINT:
                return "point:" + datum.getDatumPoint();
            case DATUM_MISSING:
                return "missing";
            case DATUM_NOT_SET:
            default:
                return "not_set";
        }
    }
}
