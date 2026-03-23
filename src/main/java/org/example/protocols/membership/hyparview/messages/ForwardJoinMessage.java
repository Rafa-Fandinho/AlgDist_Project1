package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ForwardJoinMessage extends ProtoMessage {

    public final static short MSG_ID = 103; // ID único para o JoinMessage

    private final Host newNode;
    private final int ttl;
    private final Host sender; // O nó de contato 

    public ForwardJoinMessage(Host newNode, int ttl, Host sender) {
        super(MSG_ID);
        this.newNode = newNode;
        this.ttl = ttl;
        this.sender = sender;
    }

    public Host getNewNode() {
        return newNode;
    }

    public int getTtl() {
        return ttl;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{" +
                "sender=" + sender +
                '}';
    }

    // Serializer para enviar a mensagem pela rede
    public static ISerializer<ForwardJoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardJoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
            out.writeInt(msg.ttl);
            Host.serializer.serialize(msg.sender, out);
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws IOException {
            Host newNode = Host.serializer.deserialize(in);
            int ttl = in.readInt();
            Host sender = Host.serializer.deserialize(in);
            return new ForwardJoinMessage(newNode, ttl, sender);
        }
    };
}