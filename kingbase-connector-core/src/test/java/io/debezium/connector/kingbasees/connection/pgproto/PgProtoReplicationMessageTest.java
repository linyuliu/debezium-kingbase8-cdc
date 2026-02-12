package io.debezium.connector.kingbasees.connection.pgproto;

import io.debezium.connector.kingbasees.proto.PgProto;
import org.junit.Assert;
import org.junit.Test;

public class PgProtoReplicationMessageTest {

    @Test
    public void shouldReturnSchemaQualifiedTableName() {
        PgProto.RowMessage message = PgProto.RowMessage.newBuilder()
                .setSchema("form")
                .setTable("t_debug")
                .setOp(PgProto.Op.INSERT)
                .build();

        PgProtoReplicationMessage replicationMessage = new PgProtoReplicationMessage(message, null);
        Assert.assertEquals("form.t_debug", replicationMessage.getTable());
    }

    @Test
    public void shouldFallbackToTableWhenSchemaIsAbsent() {
        PgProto.RowMessage message = PgProto.RowMessage.newBuilder()
                .setTable("t_debug")
                .setOp(PgProto.Op.INSERT)
                .build();

        PgProtoReplicationMessage replicationMessage = new PgProtoReplicationMessage(message, null);
        Assert.assertEquals("t_debug", replicationMessage.getTable());
    }
}
