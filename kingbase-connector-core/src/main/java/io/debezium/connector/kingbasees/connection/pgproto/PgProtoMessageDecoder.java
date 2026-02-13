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
    private static final boolean DEBUG_PROTO_PARSE = isDebugProtoParseEnabled();
    private static final boolean STRICT_PROTO_PARSE = isStrictProtoParseEnabled();

    private boolean warnedOnUnknownOp = false;

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
                        "流复制阶段收到无效缓冲区：buffer 不支持 array 访问");
            }
            final byte[] source = buffer.array();
            final int offset = buffer.arrayOffset() + buffer.position();
            final int length = buffer.remaining();
            content = Arrays.copyOfRange(source, offset, offset + length);
            if (DEBUG_RAW_WAL) {
                logRawMessage(content);
            }

            final ParsedRowMessage parsed = parseRowMessageDetailed(content);
            final RowMessage message = parsed.message;
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("收到 Protobuf 变更消息：{}", message);
            }
            if (!message.getNewTypeinfoList().isEmpty() && message.getNewTupleCount() != message.getNewTypeinfoCount()) {
                throw new ConnectException(String.format("事务 %s 的消息中数据列数量为 %s，但类型信息数量仅为 %s",
                        Integer.toUnsignedLong(message.getTransactionId()),
                        message.getNewTupleCount(),
                        message.getNewTypeinfoCount()));
            }
            if (DEBUG_PROTO_PARSE) {
                LOGGER.info("[Proto解析] 解析成功：来源={}，帧={}，事务={}，操作={}，表={}，newTuple={}，oldTuple={}，typeInfo={}",
                        parsed.parserName,
                        parsed.candidate.shortDescription(),
                        Integer.toUnsignedLong(message.getTransactionId()),
                        message.getOp(),
                        describeTable(message),
                        message.getNewTupleCount(),
                        message.getOldTupleCount(),
                        message.getNewTypeinfoCount());
                // 输出列详情便于诊断版本兼容性问题
                logColumnDetails(message);
            }
            if (!SUPPORTED_OPS.contains(message.getOp())) {
                if (!warnedOnUnknownOp) {
                    LOGGER.warn("[Proto解析] 收到当前版本不支持的操作类型：{}。如需处理请升级连接器或扩展协议映射", message.getOp());
                    warnedOnUnknownOp = true;
                }
                return;
            }
            processor.process(new PgProtoReplicationMessage(message, typeRegistry));
        }
        catch (InvalidProtocolBufferException e) {
            if (STRICT_PROTO_PARSE) {
                throw new ConnectException("严格模式下 Proto 解析失败：" + e.getMessage(), e);
            }
            LOGGER.warn("[Proto解析] 跳过无法解析的 WAL 消息：长度={}，预览={}，原因={}",
                    content == null ? -1 : content.length,
                    toHexPreview(content, 96),
                    e.getMessage());
            if (DEBUG_PROTO_PARSE) {
                LOGGER.warn("[Proto解析] 可设置 kb.strict.proto.parse=true（或环境变量 KB_STRICT_PROTO_PARSE=true）让解析失败直接抛错");
            }
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

    /**
     * 记录原始 WAL 包信息，便于逆向分析和版本兼容性诊断
     * 环境变量 KB_DEBUG_RAW_WAL=true 或系统属性 kb.debug.rawwal=true 启用
     */
    private void logRawMessage(byte[] content) {
        LOGGER.info("[Proto解析] 原始 WAL 包：长度={} 字节，预览（前96字节）={}", content.length, toHexPreview(content, 96));
        if (content.length > 96) {
            LOGGER.info("[Proto解析] 完整16进制转储：{}", toHexPreview(content, content.length));
        }
    }

    private RowMessage parseRowMessage(byte[] content) throws InvalidProtocolBufferException {
        return parseRowMessageDetailed(content).message;
    }

    private ParsedRowMessage parseRowMessageDetailed(byte[] content) throws InvalidProtocolBufferException {
        List<PayloadCandidate> payloadCandidates = extractPayloadCandidates(content);
        if (DEBUG_PROTO_PARSE) {
            LOGGER.info("[Proto解析] 候选帧数量={}，明细={}", payloadCandidates.size(), describeCandidates(payloadCandidates));
        }

        for (PayloadCandidate candidate : payloadCandidates) {
            RowMessage parsed = parseWithCurrentProto(candidate);
            if (parsed != null) {
                return new ParsedRowMessage(parsed, "current-proto", candidate);
            }
        }

        for (PayloadCandidate candidate : payloadCandidates) {
            RowMessage parsed = parseWithOfficialProto(candidate);
            if (parsed != null) {
                return new ParsedRowMessage(parsed, "official-proto", candidate);
            }
        }

        throw new InvalidProtocolBufferException("无法解析 decoderbufs 行消息：帧格式或协议版本不匹配");
    }

    private RowMessage parseWithCurrentProto(PayloadCandidate candidate) {
        try {
            RowMessage message = PgProto.RowMessage.parseFrom(candidate.payload);
            if (looksLikeMisparsedOfficialMessage(message)) {
                if (DEBUG_PROTO_PARSE) {
                    LOGGER.debug("[Proto解析] 当前协议命中疑似误解析，转官方协议重试：{}", candidate.shortDescription());
                }
                return null;
            }
            if (DEBUG_PROTO_PARSE) {
                LOGGER.debug("[Proto解析] 使用当前协议解析成功：{}，op={}，tx={}",
                        candidate.shortDescription(),
                        message.getOp(),
                        Integer.toUnsignedLong(message.getTransactionId()));
            }
            return message;
        }
        catch (InvalidProtocolBufferException e) {
            if (DEBUG_PROTO_PARSE) {
                LOGGER.debug("[Proto解析] 当前协议解析失败：{}，原因={}", candidate.shortDescription(), e.getMessage());
            }
            return null;
        }
    }

    private RowMessage parseWithOfficialProto(PayloadCandidate candidate) {
        try {
            PgProtoOfficial.RowMessage message = PgProtoOfficial.RowMessage.parseFrom(candidate.payload);
            if (DEBUG_PROTO_PARSE) {
                LOGGER.debug("[Proto解析] 使用官方协议解析成功：{}，op={}，tx={}，支持操作={}",
                        candidate.shortDescription(),
                        message.getOp(),
                        Integer.toUnsignedLong(message.getTransactionId()),
                        isSupported(message.getOp()));
            }
            return convertFromOfficial(message);
        }
        catch (InvalidProtocolBufferException e) {
            if (DEBUG_PROTO_PARSE) {
                LOGGER.debug("[Proto解析] 官方协议解析失败：{}，原因={}", candidate.shortDescription(), e.getMessage());
            }
            return null;
        }
    }

    private List<PayloadCandidate> extractPayloadCandidates(byte[] content) {
        List<PayloadCandidate> candidates = new ArrayList<PayloadCandidate>();
        addCandidate(candidates, "raw", 0, content);

        if (content.length > 4) {
            int beLength = ((content[0] & 0xFF) << 24)
                    | ((content[1] & 0xFF) << 16)
                    | ((content[2] & 0xFF) << 8)
                    | (content[3] & 0xFF);
            if (beLength > 0 && beLength == content.length - 4) {
                addCandidate(candidates, "be32-length-prefix", 4, Arrays.copyOfRange(content, 4, content.length));
            }

            int leLength = (content[0] & 0xFF)
                    | ((content[1] & 0xFF) << 8)
                    | ((content[2] & 0xFF) << 16)
                    | ((content[3] & 0xFF) << 24);
            if (leLength > 0 && leLength == content.length - 4) {
                addCandidate(candidates, "le32-length-prefix", 4, Arrays.copyOfRange(content, 4, content.length));
            }
        }

        try {
            com.google.protobuf.CodedInputStream input = com.google.protobuf.CodedInputStream.newInstance(content);
            int varintLength = input.readRawVarint32();
            int headerSize = input.getTotalBytesRead();
            if (varintLength > 0 && varintLength == content.length - headerSize) {
                addCandidate(candidates, "varint-length-prefix", headerSize, Arrays.copyOfRange(content, headerSize, content.length));
            }
        }
        catch (Exception ignored) {
            if (DEBUG_PROTO_PARSE) {
                LOGGER.debug("[Proto解析] varint 帧识别失败，忽略该候选格式");
            }
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

    private static boolean isDebugProtoParseEnabled() {
        return Boolean.getBoolean("kb.debug.proto.parse") || isTruthy(System.getenv("KB_DEBUG_PROTO_PARSE"));
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

    private void addCandidate(List<PayloadCandidate> candidates, String frameType, int headerSize, byte[] payload) {
        for (PayloadCandidate existing : candidates) {
            if (Arrays.equals(existing.payload, payload)) {
                return;
            }
        }
        candidates.add(new PayloadCandidate(frameType, headerSize, payload));
    }

    private static String describeCandidates(List<PayloadCandidate> candidates) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < candidates.size(); i++) {
            if (i > 0) {
                sb.append("; ");
            }
            sb.append(i + 1).append(". ").append(candidates.get(i).shortDescription());
        }
        return sb.toString();
    }

    private static String describeTable(RowMessage message) {
        String schema = message.hasSchema() ? message.getSchema() : "";
        String table = message.hasTable() ? message.getTable() : "";
        if (schema.isEmpty() && table.isEmpty()) {
            return "<未知>";
        }
        if (schema.isEmpty()) {
            return table;
        }
        if (table.isEmpty()) {
            return schema;
        }
        return schema + "." + table;
    }

    /**
     * 输出列级别详情，便于逆向分析不同 Kingbase 版本的 Proto 格式差异
     */
    private void logColumnDetails(RowMessage message) {
        if (message.getNewTupleCount() > 0) {
            StringBuilder cols = new StringBuilder();
            for (int i = 0; i < message.getNewTupleCount(); i++) {
                PgProto.DatumMessage datum = message.getNewTuple(i);
                if (i > 0) cols.append(", ");
                cols.append(String.format("%s(type=%d)", datum.getColumnName(), datum.getColumnType()));
            }
            LOGGER.info("[Proto解析] 新行列详情：{}", cols.toString());
        }
        if (message.getOldTupleCount() > 0) {
            StringBuilder cols = new StringBuilder();
            for (int i = 0; i < message.getOldTupleCount(); i++) {
                PgProto.DatumMessage datum = message.getOldTuple(i);
                if (i > 0) cols.append(", ");
                cols.append(String.format("%s(type=%d)", datum.getColumnName(), datum.getColumnType()));
            }
            LOGGER.info("[Proto解析] 旧行列详情：{}", cols.toString());
        }
    }

    private static final class PayloadCandidate {
        private final String frameType;
        private final int headerSize;
        private final byte[] payload;

        private PayloadCandidate(String frameType, int headerSize, byte[] payload) {
            this.frameType = frameType;
            this.headerSize = headerSize;
            this.payload = payload;
        }

        private String shortDescription() {
            return frameType + "(header=" + headerSize + ",payload=" + payload.length + ")";
        }
    }

    private static final class ParsedRowMessage {
        private final RowMessage message;
        private final String parserName;
        private final PayloadCandidate candidate;

        private ParsedRowMessage(RowMessage message, String parserName, PayloadCandidate candidate) {
            this.message = message;
            this.parserName = parserName;
            this.candidate = candidate;
        }
    }
}
