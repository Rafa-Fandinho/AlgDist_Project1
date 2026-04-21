package org.example.protocols.broadcast.selfdesigned.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class SelfDesignedMessage extends ProtoMessage {
    public static final short MSG_ID = 201;

    private final UUID mid;
    private final Host sender;
    private Integer hops;

    private final short toDeliver;
    private final byte[] content;

    @Override
    public String toString() {
        return "SelfDesignedMessage{" +
                "mid=" + mid +
                '}';
    }

    public SelfDesignedMessage(UUID mid, Host sender, short toDeliver, byte[] content, int hops) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.content = content;
        this.hops = hops;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public byte[] getContent() {
        return content;
    }

    public Integer getHops() { return hops; }

    public void incrementHops() {
        hops++;
    }

    public static ISerializer<org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage selfDesignedMessage, ByteBuf out) throws IOException {
            out.writeLong(selfDesignedMessage.mid.getMostSignificantBits());
            out.writeLong(selfDesignedMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(selfDesignedMessage.sender, out);
            out.writeShort(selfDesignedMessage.toDeliver);
            out.writeInt(selfDesignedMessage.content.length);
            if (selfDesignedMessage.content.length > 0) {
                out.writeBytes(selfDesignedMessage.content);
            }
            out.writeInt(selfDesignedMessage.hops);
        }

        @Override
        public org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);
            int hops = in.readInt();

            return new org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage(mid, sender, toDeliver, content, hops);
        }
    };
}
