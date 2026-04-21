package org.example.protocols.broadcast.selfdesigned.timers;

import org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class WaitTimer extends ProtoTimer {

    public static final short TIMER_ID = 203;

    private final SelfDesignedMessage msg;
    private final Host from;

    public WaitTimer(SelfDesignedMessage msg, Host from) {
        super(TIMER_ID);
        this.msg = msg;
        this.from = from;
    }

    public SelfDesignedMessage getMsg() { return msg; }

    public Host getFrom() { return from; }

    @Override
    public ProtoTimer clone() { return new WaitTimer(msg, from); }

}
