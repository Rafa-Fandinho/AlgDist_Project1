package org.example.protocols.broadcast.selfdesigned;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.broadcast.common.BroadcastRequest;
import org.example.protocols.broadcast.common.DeliverNotification;
import org.example.protocols.broadcast.selfdesigned.messages.SelfDesignedMessage;
import org.example.protocols.broadcast.selfdesigned.timers.WaitTimer;
import org.example.protocols.membership.common.notifications.ChannelCreated;
import org.example.protocols.membership.common.notifications.NeighbourDown;
import org.example.protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class SelfDesignedBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(org.example.protocols.broadcast.selfdesigned.SelfDesignedBroadcast.class);
    private static final Integer MAX_HOPS = 10;
    private int counterThreshold;
    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "Self-Designed";
    public static final short PROTOCOL_ID = 202;

    public static final String PAR_COUNTER_THRESHOLD = "protocol.broadcast.counter_threshold";
    public static final String PAR_DEFAULT_COUNTER_THRESHOLD = "5";

    private final Host myself; //My own address/port
    private final Set<Host> neighbours; //My known neighbours (a.k.a peers the membership protocol told me about)
    private final Set<UUID> received; //Set of received messages (since we do not want to deliver the same msg twice)
    private final Map<UUID, Integer> counters;

    //We can only start sending messages after the membership protocol informed us that the channel is ready
    private boolean channelReady;

    public SelfDesignedBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        neighbours = new HashSet<>();
        received = new HashSet<>();
        counters = new HashMap<>();
        channelReady = false;

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(WaitTimer.TIMER_ID, this::uponWaitTimer);
    }

    @Override
    public void init(Properties props) {
        this.counterThreshold = Integer.parseInt(
                props.getProperty(PAR_COUNTER_THRESHOLD, PAR_DEFAULT_COUNTER_THRESHOLD)
        );
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, SelfDesignedMessage.MSG_ID, SelfDesignedMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, SelfDesignedMessage.MSG_ID, this::uponSelfDesignedMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady) return; //Ideally we would buffer this message to transmit when the channel is ready :)

        //Create the message object.
        SelfDesignedMessage msg = new SelfDesignedMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg(), 0, 0);

        //Call the auxiliary function to send the message to our neighborhood
        forward(msg, myself);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponSelfDesignedMessage(SelfDesignedMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received {} from {}", msg, from);
        //If the message has already exceeded the max number of hops, do nothing at all
        if (msg.getHops() <= MAX_HOPS) {
            //If we already received it once, or the message has already exceeded the max number of hops, do nothing (or we would end up with a nasty infinite loop)
            if (received.add(msg.getMid())) {
                //Deliver the message to the application (even if it came from it)
                triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
                //In order to reduce the overload in the first round, there is a probability to not rebroadcast
                if(msg.getHops()==1 && msg.getSequence()<2*counterThreshold){
                    forward(msg, from);
                }
                else {
                    //Add the message to the counters array
                    counters.put(msg.getMid(), 1);
                    //Finally, we manage the timer for it to eventually handle the forwarding
                    int wait_time = CalculateDelay(msg);
                    WaitTimer wait_timer = new WaitTimer(msg, from);
                    setupTimer(wait_timer, wait_time);

                }
            }
            else{
                if (counters.containsKey(msg.getMid())) { //If we are still in waiting mode for this message
                    int actual = counters.get(msg.getMid()) + 1;
                    counters.put(msg.getMid(), actual);   //We increment the number of received copies and return
                }
            }
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void forward(SelfDesignedMessage msg, Host from){
        msg.incrementHops();    //We increment one hop since the message is passing through us
        //Simply send the message to every known neighbour (who will then do the same). In the first round sequence number matters so we send copies in order to not
        //overwrite the sent variable, every other time we save the work of doing that.
        if(msg.getHops() == 1){
            neighbours.forEach(host -> {
                if (!host.equals(from)) {
                    logger.trace("Sent {} to {}", msg, host);
                    sendMessage(new SelfDesignedMessage(msg), host);
                    msg.incrementSequence();
                }
            });
        }
        else {
            neighbours.forEach(host -> {
                if (!host.equals(from)) {
                    logger.trace("Sent {} to {}", msg, host);
                    sendMessage(msg, host);
                }
            });
        }
    }
    //Function to generate a random wait time for the duplicate collection
    private Integer CalculateDelay(SelfDesignedMessage msg){
        if (msg.getHops()<2){   //At first, we want a short wait, since we are expecting high liveness
            return 10 + (int) (Math.random()*20);
        }
        else{   //Later, the wait is longer
            return 50 + (int) (Math.random()*100);
        }
    }
    //When the wait timer runs out, we count the number of duplicates and decide whether to forward the message
    private void uponWaitTimer(WaitTimer timer, long timerId){
        if(counters.get(timer.getMsg().getMid()) < counterThreshold){
            forward(timer.getMsg(), timer.getFrom());
        }
        counters.remove(timer.getMsg().getMid());
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    //When the membership protocol notifies of a new neighbour (or leaving one) simply update my list of neighbours.
    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            neighbours.add(h);
            logger.info("New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
            neighbours.remove(h);
            logger.info("Neighbour down: " + h);
        }
    }
}

