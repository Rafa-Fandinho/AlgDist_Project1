package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinMessage extends ProtoMessage {

    public final static short MSG_ID = 104; // ID único para o JoinMessage

    private final Host newNode; // nó que quer entrar

    public JoinMessage(Host newNode) {
        super(MSG_ID);
        this.newNode = newNode;
    }

    public Host getNewNode() {
        return newNode;
    }

    @Override
    public String toString() {
        return "JoinMessage{" +
                "newNode=" + newNode +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.newNode, out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            Host node = Host.serializer.deserialize(in);
            return new JoinMessage(node);
        }
    };
}