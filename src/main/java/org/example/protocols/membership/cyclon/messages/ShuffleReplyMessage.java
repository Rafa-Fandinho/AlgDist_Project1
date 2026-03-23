package org.example.protocols.membership.cyclon.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ShuffleReplyMessage extends ProtoMessage {
    public final static short MSG_ID = 101;

    private final Map<Host,Integer> shuffleReply;

    public ShuffleReplyMessage(Map<Host,Integer> shuffleReply) {
        super(MSG_ID);
        this.shuffleReply = shuffleReply;
    }

    public Map<Host,Integer> getSample() {
        return shuffleReply;
    }

    @Override
    public String toString() {
        return "ShuffleRequestMessage{" +
                "subset=" + shuffleReply.toString() +
                '}';
    }

    public static ISerializer<ShuffleReplyMessage> serializer = new ISerializer<ShuffleReplyMessage>() {
        @Override
        public void serialize(ShuffleReplyMessage shuffleReplyMessage, ByteBuf out) throws IOException {
            out.writeInt(shuffleReplyMessage.shuffleReply.size());
            for (Map.Entry<Host,Integer> entry: shuffleReplyMessage.shuffleReply.entrySet()){
                Host.serializer.serialize(entry.getKey(), out);
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Host,Integer> subset = new HashMap<>(size);
            for (int i = 0; i < size; i++)
                subset.put(Host.serializer.deserialize(in), in.readInt());
            return new ShuffleReplyMessage(subset);
        }
    };
}
