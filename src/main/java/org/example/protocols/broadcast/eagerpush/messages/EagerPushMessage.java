package org.example.protocols.broadcast.eagerpush.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class EagerPushMessage extends ProtoMessage {
    // Unique ID for this message type
    public static final short MSG_ID = 202;

    private final UUID mid;        // Unique message ID (to prevent duplicates)
    private final Host sender;     // The node that originated the broadcast

    private final short sourceProto; // Protocol ID to deliver the notification to
    private final byte[] content;    // The actual message payload

    @Override
    public String toString() {
        return "EagerPushMessage{" +
                "mid=" + mid +
                ", sender=" + sender +
                '}';
    }

    public EagerPushMessage(UUID mid, Host sender, short sourceProto, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.sourceProto = sourceProto;
        this.content = content;
    }

    public Host getSender() { return sender; }

    public UUID getMid() { return mid; }

    public byte[] getContent() { return content; }

    public short getSourceProto() { return sourceProto; }

    /* ------------------------- Serializer ------------------------- */

    public static ISerializer<EagerPushMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(EagerPushMessage msg, ByteBuf out) throws IOException {
            // Write the UUID (16 bytes)
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());

            // Use Host's native serializer for the address
            Host.serializer.serialize(msg.sender, out);

            out.writeShort(msg.sourceProto);

            // Write the content length and bytes
            out.writeInt(msg.content.length);
            if (msg.content.length > 0) {
                out.writeBytes(msg.content);
            }
        }

        @Override
        public EagerPushMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);

            Host sender = Host.serializer.deserialize(in);

            short sourceProto = in.readShort();

            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new EagerPushMessage(mid, sender, sourceProto, content);
        }
    };
}