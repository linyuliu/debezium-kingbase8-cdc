/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kingbasees.connection.pgproto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.kingbase8.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.connector.kingbasees.TypeRegistry;
import io.debezium.connector.kingbasees.connection.AbstractMessageDecoder;
import io.debezium.connector.kingbasees.connection.MessageDecoderConfig;
import io.debezium.connector.kingbasees.connection.ReplicationStream.ReplicationMessageProcessor;
import io.debezium.connector.kingbasees.proto.PgProto;
import io.debezium.connector.kingbasees.proto.PgProto.Op;
import io.debezium.connector.kingbasees.proto.PgProto.RowMessage;
import io.debezium.connector.kingbasees.proto.PgProtoOfficial;
import io.debezium.util.Collect;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * ProtoBuf deserialization of message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</a>.
 * Only one message is delivered for processing.
 *
 * @author Jiri Pechanec
 *
 */
public class PgProtoMessageDecoder extends AbstractMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PgProtoMessageDecoder.class);
    private static final Set<Op> SUPPORTED_OPS = Collect.unmodifiableSet(Op.INSERT, Op.UPDATE, Op.DELETE, Op.BEGIN, Op.COMMIT);
    private static final boolean DEBUG_RAW_WAL = isDebugRawWalEnabled();
    private static final boolean STRICT_PROTO_PARSE = isStrictProtoParseEnabled();

    private boolean warnedOnUnkownOp = false;

    public PgProtoMessageDecoder(MessageDecoderConfig config) {
        super(config);
    }

    @Override
    public void processNotEmptyMessage(final ByteBuffer buffer, ReplicationMessageProcessor processor, TypeRegistry typeRegistry)
            throws SQLException, InterruptedException {
        byte[] content = null;
        try {
            if (!buffer.hasArray()) {
                throw new IllegalStateException(
                        "Invalid buffer received from Postgres server during streaming replication");
            }
            final byte[] source = buffer.array();
            final int offset = buffer.arrayOffset() + buffer.position();
            final int length = buffer.remaining();
            content = Arrays.copyOfRange(source, offset, offset + length);
            if (DEBUG_RAW_WAL) {
                logRawMessage(content);
            }

            final RowMessage message = parseRowMessage(content);
            LOGGER.trace("Received protobuf message from the server {}", message);
            if (!message.getNewTypeinfoList().isEmpty() && message.getNewTupleCount() != message.getNewTypeinfoCount()) {
                throw new ConnectException(String.format("Message from transaction %s has %s data columns but only %s of type info",
                        Integer.toUnsignedLong(message.getTransactionId()),
                        message.getNewTupleCount(),
                        message.getNewTypeinfoCount()));
            }
            if (!SUPPORTED_OPS.contains(message.getOp())) {
                if (!warnedOnUnkownOp) {
                    LOGGER.warn("Received message with type '{}' that is unknown to this version of connector, consider upgrading", message.getOp());
                    warnedOnUnkownOp = true;
                }
                return;
            }
            processor.process(new PgProtoReplicationMessage(message, typeRegistry));
        }
        catch (InvalidProtocolBufferException e) {
            if (STRICT_PROTO_PARSE) {
                throw new ConnectException(e);
            }
            LOGGER.warn("Skipping undecodable WAL message len={}, preview={}, cause={}",
                    content == null ? -1 : content.length,
                    toHexPreview(content, 96),
                    e.getMessage());
        }
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    @Override
    public ChainedLogicalStreamBuilder optionsWithoutMetadata(ChainedLogicalStreamBuilder builder) {
        return builder;
    }

    private void logRawMessage(byte[] content) {
        System.out.println("KB WAL raw len=" + content.length + " preview=" + toHexPreview(content, 96));
    }

    private RowMessage parseRowMessage(byte[] content) throws InvalidProtocolBufferException {
        List<byte[]> payloadCandidates = extractPayloadCandidates(content);

        for (byte[] payload : payloadCandidates) {
            RowMessage parsed = parseWithCurrentProto(payload);
            if (parsed != null) {
                return parsed;
            }
        }

        for (byte[] payload : payloadCandidates) {
            RowMessage parsed = parseWithOfficialProto(payload);
            if (parsed != null) {
                return parsed;
            }
        }

        throw new InvalidProtocolBufferException("Unable to parse decoderbufs row message; unsupported framing or protocol mismatch");
    }

    private RowMessage parseWithCurrentProto(byte[] payload) {
        try {
            RowMessage message = PgProto.RowMessage.parseFrom(payload);
            if (looksLikeMisparsedOfficialMessage(message)) {
                return null;
            }
            return message;
        }
        catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private RowMessage parseWithOfficialProto(byte[] payload) {
        try {
            PgProtoOfficial.RowMessage message = PgProtoOfficial.RowMessage.parseFrom(payload);
            return convertFromOfficial(message);
        }
        catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    private List<byte[]> extractPayloadCandidates(byte[] content) {
        List<byte[]> candidates = new ArrayList<>();
        candidates.add(content);

        if (content.length > 4) {
            int beLength = ((content[0] & 0xFF) << 24)
                    | ((content[1] & 0xFF) << 16)
                    | ((content[2] & 0xFF) << 8)
                    | (content[3] & 0xFF);
            if (beLength > 0 && beLength == content.length - 4) {
                candidates.add(Arrays.copyOfRange(content, 4, content.length));
            }

            int leLength = (content[0] & 0xFF)
                    | ((content[1] & 0xFF) << 8)
                    | ((content[2] & 0xFF) << 16)
                    | ((content[3] & 0xFF) << 24);
            if (leLength > 0 && leLength == content.length - 4) {
                candidates.add(Arrays.copyOfRange(content, 4, content.length));
            }
        }

        try {
            com.google.protobuf.CodedInputStream input = com.google.protobuf.CodedInputStream.newInstance(content);
            int varintLength = input.readRawVarint32();
            int headerSize = input.getTotalBytesRead();
            if (varintLength > 0 && varintLength == content.length - headerSize) {
                candidates.add(Arrays.copyOfRange(content, headerSize, content.length));
            }
        }
        catch (Exception ignored) {
        }

        return candidates;
    }

    private RowMessage convertFromOfficial(PgProtoOfficial.RowMessage source) {
        RowMessage.Builder target = RowMessage.newBuilder()
                .setTransactionId(source.getTransactionId())
                .setCommitTime(source.getCommitTime())
                .setOp(mapOp(source.getOp()));

        if (source.hasTable()) {
            target.setTable(source.getTable());
        }

        for (PgProtoOfficial.DatumMessage datum : source.getNewTupleList()) {
            target.addNewTuple(convertDatumFromOfficial(datum));
        }
        for (PgProtoOfficial.DatumMessage datum : source.getOldTupleList()) {
            target.addOldTuple(convertDatumFromOfficial(datum));
        }
        for (PgProtoOfficial.TypeInfo typeInfo : source.getNewTypeinfoList()) {
            target.addNewTypeinfo(PgProto.TypeInfo.newBuilder()
                    .setModifier(typeInfo.getModifier())
                    .setValueOptional(typeInfo.getValueOptional())
                    .build());
        }
        return target.build();
    }

    private PgProto.DatumMessage convertDatumFromOfficial(PgProtoOfficial.DatumMessage source) {
        PgProto.DatumMessage.Builder target = PgProto.DatumMessage.newBuilder()
                .setColumnName(source.getColumnName())
                .setColumnType(source.getColumnType());

        switch (source.getDatumCase()) {
            case DATUM_INT32:
                target.setDatumInt32(source.getDatumInt32());
                break;
            case DATUM_INT64:
                target.setDatumInt64(source.getDatumInt64());
                break;
            case DATUM_FLOAT:
                target.setDatumFloat(source.getDatumFloat());
                break;
            case DATUM_DOUBLE:
                target.setDatumDouble(source.getDatumDouble());
                break;
            case DATUM_BOOL:
                target.setDatumBool(source.getDatumBool());
                break;
            case DATUM_STRING:
                target.setDatumString(source.getDatumString());
                break;
            case DATUM_BYTES:
                target.setDatumBytes(source.getDatumBytes());
                break;
            case DATUM_POINT:
                target.setDatumPoint(PgProto.Point.newBuilder()
                        .setX(source.getDatumPoint().getX())
                        .setY(source.getDatumPoint().getY())
                        .build());
                break;
            case DATUM_MISSING:
                target.setDatumMissing(source.getDatumMissing());
                break;
            case DATUM_NOT_SET:
            default:
                break;
        }
        return target.build();
    }

    private boolean isSupported(Op op) {
        return SUPPORTED_OPS.contains(op);
    }

    private boolean isSupported(PgProtoOfficial.Op op) {
        return op == PgProtoOfficial.Op.INSERT
                || op == PgProtoOfficial.Op.UPDATE
                || op == PgProtoOfficial.Op.DELETE
                || op == PgProtoOfficial.Op.BEGIN
                || op == PgProtoOfficial.Op.COMMIT;
    }

    private Op mapOp(PgProtoOfficial.Op op) {
        switch (op) {
            case INSERT:
                return Op.INSERT;
            case UPDATE:
                return Op.UPDATE;
            case DELETE:
                return Op.DELETE;
            case BEGIN:
                return Op.BEGIN;
            case COMMIT:
                return Op.COMMIT;
            case UNKNOWN:
            default:
                return Op.UNKNOWN;
        }
    }

    private boolean looksLikeMisparsedOfficialMessage(RowMessage message) {
        // Official decoderbufs payload may be parsed as current proto with:
        // schema=<table>, table empty, op UNKNOWN, and no tuple payload.
        return message.getOp() == Op.UNKNOWN
                && message.hasSchema()
                && !message.hasTable()
                && message.getNewTupleCount() == 0
                && message.getOldTupleCount() == 0;
    }

    private static boolean isDebugRawWalEnabled() {
        return Boolean.getBoolean("kb.debug.rawwal") || isTruthy(System.getenv("KB_DEBUG_RAW_WAL"));
    }

    private static boolean isStrictProtoParseEnabled() {
        return Boolean.getBoolean("kb.strict.proto.parse") || isTruthy(System.getenv("KB_STRICT_PROTO_PARSE"));
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

    private static String toHexPreview(byte[] content, int maxBytes) {
        if (content == null || content.length == 0) {
            return "";
        }
        int previewLen = Math.min(content.length, maxBytes);
        StringBuilder sb = new StringBuilder(previewLen * 3);
        for (int i = 0; i < previewLen; i++) {
            sb.append(String.format("%02x", content[i]));
            if (i + 1 < previewLen) {
                sb.append(' ');
            }
        }
        return sb.toString();
    }
}
