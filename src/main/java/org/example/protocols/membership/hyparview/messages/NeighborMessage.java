package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class NeighborMessage extends ProtoMessage {

    public final static short MSG_ID = 106; // ID único para o JoinMessage

    private final Host sender; // O nó de contato 
    private final boolean highPriority;

    public NeighborMessage(Host sender, boolean highPriority) {
        super(MSG_ID);
        this.sender = sender;
        this.highPriority = highPriority;
    }

    public Host getSender() {
        return sender;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    @Override
    public String toString() {
        return "NeighborMessage{" +
                "sender=" + sender +
                ", highPriority=" + highPriority +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<NeighborMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
            out.writeBoolean(msg.highPriority);
        }

        @Override
        public NeighborMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            Boolean highPriority = in.readBoolean();
            return new NeighborMessage(sender, highPriority);
        }
    };
}