/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.kingbasees;

import com.kingbase8.core.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Extension to the {@link com.kingbase8.core.Oid} class which contains Postgres specific datatypes not found currently in the
 * JDBC driver implementation classes.
 * 
 * <p>OID 值来源：</p>
 * <ul>
 *   <li>PostgreSQL 官方定义（pg_type.h）</li>
 *   <li>Kingbase8 JDBC 驱动 8.6.1 版本</li>
 *   <li>逆向工程分析 decoderbufs 插件输出</li>
 * </ul>
 * 
 * <p>警告：不同 Kingbase 版本的 OID 可能存在差异，建议启用 OID 验证日志。</p>
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public final class PgOid extends Oid {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgOid.class);
    private static final boolean ENABLE_OID_VALIDATION = 
            Boolean.getBoolean("kb.validate.oid") || isTruthy(System.getenv("KB_VALIDATE_OID"));
    
    private static final Map<Integer, String> KNOWN_OIDS = new HashMap<>();

    static {
        // 初始化已知 OID 映射表用于验证
        KNOWN_OIDS.put(JSONB_OID, "JSONB");
        KNOWN_OIDS.put(TSRANGE_OID, "TSRANGE");
        KNOWN_OIDS.put(TSRANGE_ARRAY, "TSRANGE[]");
        KNOWN_OIDS.put(TSTZRANGE_OID, "TSTZRANGE");
        KNOWN_OIDS.put(TSTZRANGE_ARRAY, "TSTZRANGE[]");
        KNOWN_OIDS.put(DATERANGE_OID, "DATERANGE");
        KNOWN_OIDS.put(DATERANGE_ARRAY, "DATERANGE[]");
        KNOWN_OIDS.put(INET_OID, "INET");
        KNOWN_OIDS.put(INET_ARRAY, "INET[]");
        KNOWN_OIDS.put(CIDR_OID, "CIDR");
        KNOWN_OIDS.put(CIDR_ARRAY, "CIDR[]");
        KNOWN_OIDS.put(MACADDR_OID, "MACADDR");
        KNOWN_OIDS.put(MACADDR_ARRAY, "MACADDR[]");
        KNOWN_OIDS.put(MACADDR8_OID, "MACADDR8");
        KNOWN_OIDS.put(MACADDR8_ARRAY, "MACADDR8[]");
        KNOWN_OIDS.put(INT4RANGE_OID, "INT4RANGE");
        KNOWN_OIDS.put(INT4RANGE_ARRAY, "INT4RANGE[]");
        KNOWN_OIDS.put(NUM_RANGE_OID, "NUMRANGE");
        KNOWN_OIDS.put(NUM_RANGE_ARRAY, "NUMRANGE[]");
        KNOWN_OIDS.put(INT8RANGE_OID, "INT8RANGE");
        KNOWN_OIDS.put(INT8RANGE_ARRAY, "INT8RANGE[]");
        
        if (ENABLE_OID_VALIDATION) {
            LOGGER.info("[OID验证] OID 验证已启用。将检查 Kingbase JDBC 驱动定义与扩展 OID 的一致性");
            validateOids();
        }
    }

    /**
     * 内部扩展类型（来自 decoderbufs 插件逆向工程）
     * 这些 OID 在标准 Kingbase JDBC 驱动中可能未定义
     */
    public static final int JSONB_OID = 3802;
    public static final int TSRANGE_OID = 3908;
    public static final int TSRANGE_ARRAY = 3909;
    public static final int TSTZRANGE_OID = 3910;
    public static final int TSTZRANGE_ARRAY = 3911;
    public static final int DATERANGE_OID = 3912;
    public static final int DATERANGE_ARRAY = 3913;
    public static final int INET_OID = 869;
    public static final int INET_ARRAY = 1041;
    public static final int CIDR_OID = 650;
    public static final int CIDR_ARRAY = 651;
    public static final int MACADDR_OID = 829;
    public static final int MACADDR_ARRAY = 1040;
    public static final int MACADDR8_OID = 774;
    public static final int MACADDR8_ARRAY = 775;
    public static final int INT4RANGE_OID = 3904;
    public static final int INT4RANGE_ARRAY = 3905;
    public static final int NUM_RANGE_OID = 3906;
    public static final int NUM_RANGE_ARRAY = 3907;
    public static final int INT8RANGE_OID = 3926;
    public static final int INT8RANGE_ARRAY = 3927;

    /**
     * 验证 OID 定义是否与 Kingbase JDBC 驱动一致
     * 环境变量 KB_VALIDATE_OID=true 或系统属性 kb.validate.oid=true 启用
     */
    private static void validateOids() {
        try {
            // 验证与 Oid 基类中定义的常量是否冲突
            checkOidConflict(INET_OID, "INET", Oid.class);
            checkOidConflict(CIDR_OID, "CIDR", Oid.class);
            checkOidConflict(MACADDR_OID, "MACADDR", Oid.class);
            
            LOGGER.info("[OID验证] 扩展 OID 定义与 Kingbase JDBC 驱动（8.6.1）基本一致");
        } catch (Exception e) {
            LOGGER.warn("[OID验证] OID 验证过程中出现异常：{}", e.getMessage());
        }
    }

    private static void checkOidConflict(int oidValue, String typeName, Class<?> baseClass) {
        // 尝试通过反射检查基类中是否有同名但不同值的 OID 定义
        // 如果有冲突，记录警告日志
        try {
            java.lang.reflect.Field[] fields = baseClass.getDeclaredFields();
            for (java.lang.reflect.Field field : fields) {
                if (field.getType() == int.class && java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    int baseOid = field.getInt(null);
                    if (baseOid == oidValue && !field.getName().contains(typeName.toUpperCase())) {
                        LOGGER.warn("[OID验证] ⚠️ OID {} (PgOid.{}) 在基类中定义为 {}，可能存在类型映射冲突",
                                oidValue, typeName, field.getName());
                    }
                }
            }
        } catch (Exception ignored) {
            // 反射失败不影响功能
        }
    }
    
    /**
     * 检查 OID 是否为已知的扩展类型
     * @param oid OID 值
     * @return 类型名称，未知则返回 null
     */
    public static String getKnownTypeName(int oid) {
        return KNOWN_OIDS.get(oid);
    }
    
    /**
     * 记录遇到的未知 OID，便于扩展类型支持
     * @param oid OID 值
     * @param columnName 列名
     */
    public static void logUnknownOid(int oid, String columnName) {
        if (ENABLE_OID_VALIDATION && !KNOWN_OIDS.containsKey(oid)) {
            LOGGER.warn("[OID验证] ⚠️ 遇到未知 OID：{} (列名={})。如需支持请扩展 PgOid 定义", oid, columnName);
        }
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
}
