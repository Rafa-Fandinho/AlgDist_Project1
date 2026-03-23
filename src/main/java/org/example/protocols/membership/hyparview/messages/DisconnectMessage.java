package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class DisconnectMessage extends ProtoMessage {

    public final static short MSG_ID = 102; // ID único

    private final Host sender; // O nó de contato 

    public DisconnectMessage(Host sender) {
        super(MSG_ID);
        this.sender = sender;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "DisconnectMessage{" +
                "newNode=" + sender +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<DisconnectMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DisconnectMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
        }

        @Override
        public DisconnectMessage deserialize(ByteBuf in) throws IOException {
            Host node = Host.serializer.deserialize(in);
            return new DisconnectMessage(node);
        }
    };
}