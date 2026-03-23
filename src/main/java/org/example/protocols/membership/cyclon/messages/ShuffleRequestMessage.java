package org.example.protocols.membership.cyclon.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ShuffleRequestMessage extends ProtoMessage {
    public final static short MSG_ID = 101;

    private final Map<Host,Integer> shuffleRequest;

    public ShuffleRequestMessage(Map<Host,Integer> shuffleRequest) {
        super(MSG_ID);
        this.shuffleRequest = shuffleRequest;
    }

    public Map<Host,Integer> getSample() {
        return shuffleRequest;
    }

    @Override
    public String toString() {
        return "ShuffleRequestMessage{" +
                "subset=" + shuffleRequest.toString() +
                '}';
    }

    public static ISerializer<ShuffleRequestMessage> serializer = new ISerializer<ShuffleRequestMessage>() {
        @Override
        public void serialize(ShuffleRequestMessage shuffleRequestMessage, ByteBuf out) throws IOException {
            out.writeInt(shuffleRequestMessage.shuffleRequest.size());
            for (Map.Entry<Host,Integer> entry: shuffleRequestMessage.shuffleRequest.entrySet()){
                Host.serializer.serialize(entry.getKey(), out);
                out.writeInt(entry.getValue());
            }
        }

        @Override
        public ShuffleRequestMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Map<Host,Integer> subset = new HashMap<>(size);
            for (int i = 0; i < size; i++)
                subset.put(Host.serializer.deserialize(in), in.readInt());
            return new ShuffleRequestMessage(subset);
        }
    };
}
