package org.example.protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ShuffleMessage extends ProtoMessage {
    public final static short MSG_ID = 108;

    private final Host originalSender; // Quem iniciou o shuffle
    private final Host sender;         // Quem está encaminhando agora
    private final int ttl;
    private final Set<Host> sample;    // Amostra de nós trocados

    public ShuffleMessage(Host originalSender, Host sender, int ttl, Set<Host> sample) {
        super(MSG_ID);
        this.originalSender = originalSender;
        this.sender = sender;
        this.ttl = ttl;
        this.sample = sample;
    }

    public Host getOriginalSender() { return originalSender; }
    public Host getSender() { return sender; }
    public int getTtl() { return ttl; }
    public Set<Host> getSample() { return sample; }

    public static ISerializer<ShuffleMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.originalSender, out);
            Host.serializer.serialize(msg.sender, out);
            out.writeInt(msg.ttl);
            
            // Serializar coleção (Set)
            out.writeInt(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf in) throws IOException {
            Host original = Host.serializer.deserialize(in);
            Host sender = Host.serializer.deserialize(in);
            int ttl = in.readInt();
            
            int size = in.readInt();
            Set<Host> sample = new HashSet<>();
            for (int i = 0; i < size; i++) {
                sample.add(Host.serializer.deserialize(in));
            }
            
            return new ShuffleMessage(original, sender, ttl, sample);
        }
    };
}