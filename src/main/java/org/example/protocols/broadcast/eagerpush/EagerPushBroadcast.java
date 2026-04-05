package org.example.protocols.broadcast.eagerpush;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.broadcast.common.BroadcastRequest;
import org.example.protocols.broadcast.common.DeliverNotification;
import org.example.protocols.broadcast.eagerpush.messages.EagerPushMessage;
import org.example.protocols.membership.common.notifications.ChannelCreated;
import org.example.protocols.membership.common.notifications.NeighbourDown;
import org.example.protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class EagerPushBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(EagerPushBroadcast.class);

    // Protocol information to register in Babel
    public static final String PROTOCOL_NAME = "EagerPush";
    public static final short PROTOCOL_ID = 201;

    private final Host myself; // My own address/port
    private final Set<Host> neighbours; // Peers informed by the membership protocol
    private final Set<UUID> received; // Set of message IDs to ensure exactly-once delivery/forwarding
    private boolean channelReady;

    // The FANOUT parameter defines how many neighbors we "push" the information to
    private final int fanout;

    public EagerPushBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        this.neighbours = new HashSet<>();
        this.received = new HashSet<>();
        this.channelReady = false;

        // Read the fanout parameter from the configuration file (defaults to 3 if not found)
        this.fanout = Integer.parseInt(properties.getProperty("fanout", "6"));

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
        // Initialization logic if needed
    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int cId = notification.getChannelId();
        registerSharedChannel(cId);

        // Register the message serializer for EagerPushMessage
        registerMessageSerializer(cId, EagerPushMessage.MSG_ID, EagerPushMessage.serializer);

        try {
            registerMessageHandler(cId, EagerPushMessage.MSG_ID, this::uponEagerPushMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        this.channelReady = true;
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady) return;

        // Create the message object
        EagerPushMessage msg = new EagerPushMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

        // Start the dissemination from myself
        handleDissemination(msg, myself);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponEagerPushMessage(EagerPushMessage msg, Host from, short sourceProto, int channelId) {
        handleDissemination(msg, from);
    }

    private void handleDissemination(EagerPushMessage msg, Host from) {
        // Only proceed if we haven't seen this message ID before
        if (received.add(msg.getMid())) {
            // Deliver the message to the local application layer
            triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

            // EAGER PUSH logic:
            List<Host> targets = new ArrayList<>(neighbours);
            targets.remove(from); // Avoid sending the message back to the sender

            // Randomize target selection (Gossip characteristic)
            Collections.shuffle(targets);

            // Push to at most 'fanout' neighbors
            int numTargets = Math.min(fanout, targets.size());
            for (int i = 0; i < numTargets; i++) {
                sendMessage(msg, targets.get(i));
                logger.trace("Eager Push: Sent {} to {}", msg.getMid(), targets.get(i));
            }
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable.getMessage());
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            if (neighbours.add(h)) {
                logger.info("New neighbour up: " + h);
            }
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for (Host h : notification.getNeighbours()) {
            if (neighbours.remove(h)) {
                logger.info("Neighbour down: " + h);
            }
        }
    }
}