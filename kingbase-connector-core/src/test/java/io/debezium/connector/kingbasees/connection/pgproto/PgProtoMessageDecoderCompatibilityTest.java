package io.debezium.connector.kingbasees.connection.pgproto;

import io.debezium.connector.kingbasees.connection.MessageDecoderConfig;
import io.debezium.connector.kingbasees.proto.PgProto;
import io.debezium.connector.kingbasees.proto.PgProtoOfficial;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class PgProtoMessageDecoderCompatibilityTest {

    @Test
    public void shouldParseOfficialDecoderbufsMessage() throws Exception {
        PgProtoOfficial.RowMessage source = PgProtoOfficial.RowMessage.newBuilder()
                .setTransactionId(1)
                .setCommitTime(2)
                .setTable("form.t_debug")
                .setOp(PgProtoOfficial.Op.INSERT)
                .addNewTuple(PgProtoOfficial.DatumMessage.newBuilder()
                        .setColumnName("age")
                        .setColumnType(23)
                        .setDatumInt32(18)
                        .build())
                .build();

        PgProto.RowMessage parsed = parse(source.toByteArray());
        Assert.assertEquals(PgProto.Op.INSERT, parsed.getOp());
        Assert.assertEquals("form.t_debug", parsed.getTable());
        Assert.assertEquals(1, parsed.getNewTupleCount());
        Assert.assertEquals(18, parsed.getNewTuple(0).getDatumInt32());
    }

    private PgProto.RowMessage parse(byte[] payload) throws Exception {
        PgProtoMessageDecoder decoder = new PgProtoMessageDecoder(new MessageDecoderConfig(null, null, null, false, false, null));
        Method parseMethod = PgProtoMessageDecoder.class.getDeclaredMethod("parseRowMessage", byte[].class);
        parseMethod.setAccessible(true);
        return (PgProto.RowMessage) parseMethod.invoke(decoder, payload);
    }
}
