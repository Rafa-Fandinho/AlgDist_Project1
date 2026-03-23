package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class NeighborReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 107; // ID único para o JoinMessage

    private final Host sender; // O nó de contato 
    private final boolean accepted; // True: aceitou a promoção, false: rejeitou

    public NeighborReplyMessage(Host sender, boolean accepted) {
        super(MSG_ID);
        this.sender = sender;
        this.accepted = accepted;
    }

    public Host getSender() {
        return sender;
    }

    public boolean isAccepted(){
        return accepted;
    }

    @Override
    public String toString() {
        return "NeighborReplyMessage{" +
                "sender=" + sender +
                ", accepted=" + accepted +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<NeighborReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborReplyMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
            out.writeBoolean(msg.accepted);
        }

        @Override
        public NeighborReplyMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            boolean accepted = in.readBoolean();
            return new NeighborReplyMessage(sender, accepted);
        }
    };
}