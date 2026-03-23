package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 105; // ID único para o JoinMessage

    private final Host sender; // O nó de contato 

    public JoinReplyMessage(Host sender) {
        super(MSG_ID);
        this.sender = sender;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "JoinReplyMessage{" +
                "sender=" + sender +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<JoinReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReplyMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
        }

        @Override
        public JoinReplyMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            return new JoinReplyMessage(sender);
        }
    };
}