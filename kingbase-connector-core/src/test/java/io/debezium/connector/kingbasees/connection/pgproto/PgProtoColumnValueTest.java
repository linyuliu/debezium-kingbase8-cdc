package io.debezium.connector.kingbasees.connection.pgproto;

import io.debezium.connector.kingbasees.proto.PgProto;
import org.junit.Assert;
import org.junit.Test;

public class PgProtoColumnValueTest {

    @Test
    public void shouldTreatDatumNullAsNull() {
        PgProto.DatumMessage datum = PgProto.DatumMessage.newBuilder()
                .setDatumNull(true)
                .build();

        PgProtoColumnValue columnValue = new PgProtoColumnValue(datum);
        Assert.assertTrue(columnValue.isNull());
    }

    @Test
    public void shouldTreatDatumMissingAsNull() {
        PgProto.DatumMessage datum = PgProto.DatumMessage.newBuilder()
                .setDatumMissing(true)
                .build();

        PgProtoColumnValue columnValue = new PgProtoColumnValue(datum);
        Assert.assertTrue(columnValue.isNull());
    }

    @Test
    public void shouldTreatPrimitiveDatumAsNotNull() {
        PgProto.DatumMessage datum = PgProto.DatumMessage.newBuilder()
                .setDatumInt64(1L)
                .build();

        PgProtoColumnValue columnValue = new PgProtoColumnValue(datum);
        Assert.assertFalse(columnValue.isNull());
    }
}
