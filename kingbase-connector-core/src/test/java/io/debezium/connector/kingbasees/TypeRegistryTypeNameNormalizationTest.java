package io.debezium.connector.kingbasees;

import org.junit.Assert;
import org.junit.Test;

public class TypeRegistryTypeNameNormalizationTest {

    @Test
    public void shouldNormalizeIntegerAliases() {
        Assert.assertEquals("int4", TypeRegistry.normalizeTypeName("int"));
        Assert.assertEquals("int4", TypeRegistry.normalizeTypeName("integer"));
        Assert.assertEquals("int8", TypeRegistry.normalizeTypeName("bigint"));
        Assert.assertEquals("int2", TypeRegistry.normalizeTypeName("smallint"));
    }

    @Test
    public void shouldNormalizeQuotedAndSizedTypeNames() {
        Assert.assertEquals("int4", TypeRegistry.normalizeTypeName("\"INT\""));
        Assert.assertEquals("varchar", TypeRegistry.normalizeTypeName("\"character varying\"(255)"));
        Assert.assertEquals("timestamp", TypeRegistry.normalizeTypeName("timestamp without time zone(6)"));
    }

    @Test
    public void shouldNormalizeNumericAndFloatAliases() {
        Assert.assertEquals("numeric", TypeRegistry.normalizeTypeName("decimal(18,6)"));
        Assert.assertEquals("numeric", TypeRegistry.normalizeTypeName("number(20,4)"));
        Assert.assertEquals("float8", TypeRegistry.normalizeTypeName("double"));
        Assert.assertEquals("float4", TypeRegistry.normalizeTypeName("float"));
        Assert.assertEquals("float4", TypeRegistry.normalizeTypeName("binary_float"));
        Assert.assertEquals("float8", TypeRegistry.normalizeTypeName("binary_double"));
    }
}
