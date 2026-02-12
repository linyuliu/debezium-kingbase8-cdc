package io.debezium.connector.kingbasees.sink;

import io.debezium.engine.ChangeEvent;
import org.junit.Assert;
import org.junit.Test;

public class DebeziumRecordTransformerTest {

    @Test
    public void shouldBuildChangedFieldsAndDeltaForUpdate() {
        String value = "{\"payload\":{\"source\":{\"schema\":\"form\",\"table\":\"t_order\"},\"op\":\"u\",\"before\":{\"id\":1,\"amount\":10,\"name\":\"A\"},\"after\":{\"id\":1,\"amount\":15,\"name\":\"B\"}}}";
        String key = "{\"payload\":{\"id\":1}}";

        CdcEvent event = CdcEvent.parse(new SimpleEvent(key, value, "kb.form.t_order"));
        DebeziumRecordTransformer transformer = new DebeziumRecordTransformer(DeltaNullStrategy.SKIP, true, true);
        EnhancedCdcRecord record = transformer.transform(event, false);

        Assert.assertNotNull(record);
        Assert.assertFalse(record.isDeleted());
        Assert.assertEquals("u", record.getOp());
        Assert.assertEquals("15", String.valueOf(record.getData().get("amount")));
        Assert.assertTrue(record.getChangedFields().contains("amount"));
        Assert.assertTrue(record.getChangedFields().contains("name"));
        Assert.assertEquals("5", String.valueOf(record.getDeltas().get("amount")));
    }

    @Test
    public void shouldConvertDeleteAsDeletedRecord() {
        String value = "{\"payload\":{\"source\":{\"schema\":\"form\",\"table\":\"t_user\"},\"op\":\"d\",\"before\":{\"id\":7,\"score\":99},\"after\":null}}";
        String key = "{\"payload\":{\"id\":7}}";

        CdcEvent event = CdcEvent.parse(new SimpleEvent(key, value, "kb.form.t_user"));
        DebeziumRecordTransformer transformer = new DebeziumRecordTransformer(DeltaNullStrategy.SKIP, true, true);
        EnhancedCdcRecord record = transformer.transform(event, false);

        Assert.assertNotNull(record);
        Assert.assertTrue(record.isDeleted());
        Assert.assertEquals("d", record.getOp());
        Assert.assertEquals("7", String.valueOf(record.getData().get("id")));
        Assert.assertTrue(record.getChangedFields().contains("id"));
        Assert.assertTrue(record.getDeltas().isEmpty());
    }

    @Test
    public void shouldTreatTombstoneAsDeleteWhenEnabled() {
        CdcEvent event = CdcEvent.parse(new SimpleEvent("{\"payload\":{\"id\":1001}}", null, "kb.form.t_pay"));
        DebeziumRecordTransformer transformer = new DebeziumRecordTransformer(DeltaNullStrategy.SKIP, true, true);

        EnhancedCdcRecord asTombstone = transformer.transform(event, false);
        Assert.assertTrue(asTombstone.isTombstone());
        Assert.assertFalse(asTombstone.isDeleted());

        EnhancedCdcRecord asDelete = transformer.transform(event, true);
        Assert.assertTrue(asDelete.isTombstone());
        Assert.assertTrue(asDelete.isDeleted());
        Assert.assertEquals("d", asDelete.getOp());
        Assert.assertEquals("1001", String.valueOf(asDelete.getData().get("id")));
    }

    @Test
    public void shouldUseZeroWhenConfiguredForNullDelta() {
        String value = "{\"payload\":{\"source\":{\"schema\":\"form\",\"table\":\"t_score\"},\"op\":\"u\",\"before\":{\"id\":1,\"score\":10},\"after\":{\"id\":1,\"score\":null}}}";
        CdcEvent event = CdcEvent.parse(new SimpleEvent("{\"payload\":{\"id\":1}}", value, "kb.form.t_score"));

        DebeziumRecordTransformer transformer = new DebeziumRecordTransformer(DeltaNullStrategy.ZERO, true, true);
        EnhancedCdcRecord record = transformer.transform(event, false);

        Assert.assertEquals("-10", String.valueOf(record.getDeltas().get("score")));
    }

    private static final class SimpleEvent implements ChangeEvent<String, String> {
        private final String key;
        private final String value;
        private final String destination;

        private SimpleEvent(String key, String value, String destination) {
            this.key = key;
            this.value = value;
            this.destination = destination;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public String destination() {
            return destination;
        }
    }
}
