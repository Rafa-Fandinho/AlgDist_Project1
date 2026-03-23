package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class ShuffleReplyMessage extends ProtoMessage {
    public final static short MSG_ID = 109;

    private final Host sender;
    private final Set<Host> sample;

    public ShuffleReplyMessage(Host sender, Set<Host> sample) {
        super(MSG_ID);
        this.sender = sender;
        this.sample = sample;
    }

    public Host getSender(){
        return sender;
    }

    public Set<Host> getSample(){
        return sample;
    }


    public static ISerializer<ShuffleReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleReplyMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
            out.writeInt(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            int size = in.readInt();
            Set<Host> sample = new HashSet<>();
            for (int i = 0; i < size; i++) {
                sample.add(Host.serializer.deserialize(in));
            }
            return new ShuffleReplyMessage(sender, sample);
        }
    };
}