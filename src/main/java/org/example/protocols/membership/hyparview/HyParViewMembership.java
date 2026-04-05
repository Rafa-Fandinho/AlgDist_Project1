package org.example.protocols.membership.hyparview;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.membership.common.notifications.ChannelCreated;
import org.example.protocols.membership.common.notifications.NeighbourDown;
import org.example.protocols.membership.common.notifications.NeighbourUp;
import org.example.protocols.membership.full.messages.SampleMessage;
import org.example.protocols.membership.full.timers.InfoTimer;
import org.example.protocols.membership.full.timers.SampleTimer;
import org.example.protocols.membership.hyparview.messages.*;
import org.example.protocols.membership.hyparview.timers.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class HyParViewMembership extends GenericProtocol{

    private static final Logger logger = LogManager.getLogger(HyParViewMembership.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "HyParView";

    private final Host self;     //My own address/port
    //private final Set<Host> membership; //Peers I am connected to
    private final Set<Host> activeView;
    private final Set<Host> passiveView;
    private final Set<Host> pending; //Peers I am trying to connect to

    private final int maxActive = 4;
    private final int maxPassive = 10;

    private final int ARWL = 6; // Active Random Walk Length
    private final int PRWL = 3; // Passive Random Walk Length

    public final static String PAR_SAMPLE_TIME = "protocol.membership.sampletime";
    public final static String PAR_DEFAULT_SAMPLE_TIME = "2000"; //2 seconds
    private final int sampleTime; //param: timeout for samples
    
    public final static String PAR_SAMPLE_SIZE = "protocol.membership.samplesize";
    public final static String PAR_DEFAULT_SAMPLE_SIZE = "6";
    private final int subsetSize; //param: maximum size of sample;

    private final Random rnd;
    private final int channelId; //Id of the created channel

    public HyParViewMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        //this.membership = new HashSet<>();
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();
        this.pending = new HashSet<>();
        this.rnd = new Random();

        //Get some configurations from the Properties object
        this.subsetSize = Integer.parseInt(props.getProperty(PAR_SAMPLE_SIZE, PAR_DEFAULT_SAMPLE_SIZE));
        this.sampleTime = Integer.parseInt(props.getProperty(PAR_SAMPLE_TIME, PAR_DEFAULT_SAMPLE_TIME));
        
        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("babel.address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("babel.port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, JoinReplyMessage.MSG_ID, JoinReplyMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_ID, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, NeighborMessage.MSG_ID, NeighborMessage.serializer);
        registerMessageSerializer(channelId, NeighborReplyMessage.MSG_ID, NeighborReplyMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_ID, DisconnectMessage.serializer);
        registerMessageSerializer(channelId, ShuffleMessage.MSG_ID, ShuffleMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_ID, ShuffleReplyMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoin);
        registerMessageHandler(channelId, JoinReplyMessage.MSG_ID, this::uponJoinReply);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_ID, this::uponForwardJoin);
        registerMessageHandler(channelId, NeighborMessage.MSG_ID, this::uponNeighbour);
        registerMessageHandler(channelId, NeighborReplyMessage.MSG_ID, this::uponNeighbourReply);
        registerMessageHandler(channelId, DisconnectMessage.MSG_ID, this::uponDisconnect);
        registerMessageHandler(channelId, ShuffleMessage.MSG_ID, this::uponShuffle);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_ID, this::uponShuffleReply);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);
        //registerTimerHandler(HelloTimer.TIMER_ID, this::uponHelloTimer);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    /* PSEUDOCODIGO
    upon init do
        Send(JOIN, contactNode, myself);
    */
    public void init(Properties props) {

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));
        setupPeriodicTimer(new ShuffleTimer(), sampleTime, sampleTime);

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                pending.add(contactHost);
                openConnection(contactHost);
                sendMessage(new JoinMessage(self), contactHost);
                logger.debug("[{}] Sent JoinMessage to contact node {}", self, contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();    
                System.exit(-1);
            }
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */

    /* PSEUDOCODIGO
    upon Receive(JOIN, newNode) do
        trigger addNodeActiveView(newNode)
        foreach n ∈ activeView and n 6= newNode do
            Send(FORWARDJOIN, n, newNode, ARWL, myself)
    */
    private void uponJoin(JoinMessage msg, Host from, short sourceProto, int channelId){
        Host newNode = msg.getNewNode();
        logger.info("[{}] Received JOIN {} from {}", self, msg, newNode);

        addToActiveView(newNode);
        sendMessage(new JoinReplyMessage(self), newNode);
        logger.debug("[{}] Sent JoinReplyMessage to {}", self, newNode);

        for (Host n: activeView){
            if(!n.equals(newNode)){
                sendMessage(new ForwardJoinMessage(newNode, ARWL, self), n);
                logger.debug("[{}] Sent ForwardJoinMessage (node={}) to {}", self, newNode, n);
            }
        }
    }

    private void uponJoinReply(JoinReplyMessage msg, Host from, short sourceProto, int channelId) {
        Host sender = msg.getSender();
        logger.info("[{}] Received JOIN REPLY from {}", self, sender);
        pending.remove(sender);
        addToActiveView(sender);
    }


    /* POWERPOINT

    @WHEN a node p receives a FORWARDJOIN it performs the following steps in sequence:
    - if TTL == 0 or p has only one active view member: add n to active view (dropping a random node if full)
    - if TTL == PRWL: add n to passive view.
    - Decrement TTL
    - if n was not added to the active view: forward FORWARDJOIN to a random active view member (different from the sender).
    - Note: a new node ends up in the active view of one node and the passive view of another node along the random walk for each random walk for each random walk - spreading it spresence in the overlay

    PSEUDOCODIGO

    upon Receive(FORWARDJOIN, newNode, timeToLive, sender) do
        if timeToLive== 0k#activeView== 1 then
            trigger addNodeActiveView(newNode)
        else
            if timeToLive==PRWL then
                trigger addNodePassiveView(newNode)
            n ←− n ∈ activeView and n 6= sender
            Send(FORWARDJOIN, n, newNode, timeToLive-1, myself)
    */
    private void uponForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId){
        Host newNode = msg.getNewNode();
        int ttl = msg.getTtl();
        logger.debug("[{}] Received FORWARD JOIN for {} with TTL {}", self, newNode, ttl);

        if(ttl == 0 || activeView.size() <= 1){
            addToActiveView(newNode);
        } else {
            if (ttl == PRWL) {
                addToPassiveView(newNode);
            }

            Host n = getRandomSubsetExcluding(activeView, 1, msg.getSender()).stream().findFirst().orElse(null);
    
            if (n != null) {
                sendMessage(new ForwardJoinMessage(newNode, ttl - 1, self), n);
                logger.debug("[{}] Forwarded FORWARD JOIN for {} to {} with TTL {}", self, newNode, n, ttl - 1);
            }
        }
    }

    /* POWERPOINT
    The NEIGHBOR request carries a priority value:
    - High priority: the sender has no elements in its active view. The recipient always accepts (drops a random node from its view if full, sending a DISCONNECT).
    - Low priority: the sender still has other active view members. The recipient only accepts if it has a free slot.
    */
    private void uponNeighbour(NeighborMessage msg, Host from, short sourceProto, int channelId){
        Host sender = msg.getSender();
        logger.info("[{}] Received Neighbour from {}", self, sender);
        if (msg.isHighPriority() || activeView.size() < maxActive){
            addToActiveView(sender);
            sendMessage(new NeighborReplyMessage(self, true), sender);
            logger.debug("[{}] Sent NeighborReplyMessage (Accepted=true) to {}", self, sender);
        } else {
            sendMessage(new NeighborReplyMessage(self, false), sender);
            logger.debug("[{}] Sent NeighborReplyMessage (Accepted=false) to {}", self, sender);
        }
    }

    /* POWERPOINT
    If the request is accepted: q is moved from passive to active view.
    If the request is rejected: q stays in the passive view, another passive view node is tried.
    */
    private void uponNeighbourReply(NeighborReplyMessage msg, Host from, short sourceProto, int channelId){
        Host sender = msg.getSender();
        logger.info("[{}] Received Neighbour Reply (Accepted={}) from {}", self, msg.isAccepted(), sender);
        pending.remove(sender); 
        if(msg.isAccepted()){ addToActiveView(sender); } else { passiveView.remove(sender); tryPromoteNeighbor();}
    }

    /* PSEUDOCODIGO
    upon Receive(DISCONNECT, peer) do
        if peer ∈ activeView then
            activeView ←− activeView \ {peer}
            addNodePassiveView(peer)
    */
    private void uponDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId){
            Host peer = msg.getSender();
            logger.info("[{}] Received Disconnect from {}", self, peer);

            if (activeView.contains(peer)) {
                activeView.remove(peer);
                addToPassiveView(peer);
                closeConnection(peer);
                triggerNotification(new NeighbourDown(peer));
                logger.trace("[{}] Removed {} from Active View (Disconnect). Current Active: {}", self, peer, activeView);
                tryPromoteNeighbor();
            }

    }

    /* POWERPOINT
    The SHUFFLE travels as a random walk (with TTL) through the overlay; The node where it stops sends back a SHUFFLEREPLY with an equal number of its own passive view entries.
    Both nodes integrade received identifiers into their passive views, evicting old entires if necessary, giving preference to evict nodes sent to other peer. 
    This gradually removes dead nodes and introduces live ones.
    */
    private void uponShuffle(ShuffleMessage msg, Host from, short sourceProto, int channelId){
        int ttl = msg.getTtl() - 1;

        if(ttl > 0 && activeView.size() > 1){
            Host nextHop = getRandomSubsetExcluding(activeView, 1, msg.getSender()).stream().findFirst().orElse(null);

            if (nextHop != null){
                sendMessage(new ShuffleMessage(msg.getOriginalSender(), self, ttl, msg.getSample()), nextHop);
                logger.debug("[{}] Forwarded Shuffle to {} with TTL {}", self, nextHop, ttl);
            }
        } else {
            Set<Host> replySample = getRandomSubsetExcluding(passiveView, msg.getSample().size(), self);
            sendMessage(new ShuffleReplyMessage(self, replySample), from);
            logger.debug("[{}] Sent ShuffleReplyMessage to {} with {} nodes", self, from, replySample.size());

            for(Host h: msg.getSample()){
                addToPassiveView(h);
            }
        }
    }
    
    private void uponShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId){
        logger.debug("[{}] Received ShuffleReplyMessage from {} with {} nodes", self, from, msg.getSample().size());
        for (Host h : msg.getSample()){
            addToPassiveView(h);
        }
    }

    private void uponSample(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
            Throwable throwable, int channelId) {

    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponSampleTimer(SampleTimer timer, long timerId) {

    }

    /* POWERPOINT
    Periodically each node initiates a SHUFFLE operation to refresh its passive view (inspired in Cyclon).
    The initiating node builds an exchange list containing:
    - Its own identifier.
    - ka nodes from its active view.
    - kp nodes from its passive view.
    */
    private void uponShuffleTimer(ShuffleTimer timer, long timerId){
        if (activeView.isEmpty()) return;

        Host randomNeighbor = getRandom(activeView);
        if (randomNeighbor == null) return;

        Set<Host> sample = new HashSet<>();
        sample.add(self);

        int ka = subsetSize / 2;     // active view nodes
        int kp = subsetSize - ka -1; // passive view nodes

        sample.addAll(getRandomSubsetExcluding(activeView, ka, randomNeighbor));
        sample.addAll(getRandomSubsetExcluding(passiveView, kp, self));

        sendMessage(new ShuffleMessage(self, self, PRWL, sample), randomNeighbor);
        logger.debug("[{}] Timer triggered! Sent initial ShuffleMessage to {}", self, randomNeighbor);          
    }

    //Gets a random element from the set of peers
    private Host getRandom(Set<Host> hostSet) {
        int idx = rnd.nextInt(hostSet.size());
        int i = 0;
        for (Host h : hostSet) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }

    //Gets a random subset from the set of peers
    private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

        /*--------------------------------- HyParView FUNCTIONS ---------------------------------------- */

    /* PSEUDOCODIGO
    upon dropRandomElementFromActiveView do
        n ←− n ∈ activeView
        Send(DISCONNECT, n, myself)
        activeView ←− activeView \{n}
        passiveView ←− passiveView ∪{n}
    */
    private void dropRandomElementFromActiveView() {
        Host n = getRandom(activeView);
        sendMessage(new DisconnectMessage(self), n);
        activeView.remove(n);
        passiveView.add(n);

        // [Específico do Babel]
        closeConnection(n); // Tem que fechar o TCP
        triggerNotification(new NeighbourDown(n)); // Avisa a aplicação
        logger.trace("[{}] Dropped {} from Active View to Passive View. Current Active: {}", self, n, activeView);
    }

    /* PSEUDOCODIGO
    upon addNodeActiveView(node) do
        if node 6= myself and node ∈/ activeView then
            if isfull(activeView) then
                trigger dropRandomElementFromActiveView
            activeView ←− activeView ∪ node
    */
    private void addToActiveView(Host node) {
        if (!node.equals(self) && !activeView.contains(node)) {
            if (activeView.size() >= maxActive) { // isFull
                dropRandomElementFromActiveView();
            }
            activeView.add(node);
            passiveView.remove(node);

            openConnection(node); // Garante TCP aberto
            triggerNotification(new NeighbourUp(node)); // Avisa a aplicação

            logger.trace("[{}] Added {} to Active View. Current Active: {}", self, node, activeView);
        }
    }

    /* PSEUDOCODIGO
    upon addNodePassiveView(node) do
        if node 6= myself and node ∈/ activeView and node ∈/ passiveView then
            if isfull(passiveView) then
                n ←− n ∈ passiveView
                passiveView ←− passiveView \{n}
            passiveView ←− passiveView ∪ node
    */
    private void addToPassiveView(Host node) {
        if (!node.equals(self) && !activeView.contains(node) && !passiveView.contains(node)) {
            if (passiveView.size() >= maxPassive) { // isFull
                Host n = getRandom(passiveView);
                passiveView.remove(n);
            }
            passiveView.add(node);
            logger.trace("[{}] Added {} to Passive View. Current Passive: {}", self, node, passiveView);
        }
    }

    /* POWERPOINT
    @WHEN a node in the active view is detected as failed (due to the failure of a TCP connection):
    - A random node q is selected from the passive view.
    - A TCP connection to q is attemped. If it fails, q is removed from the passive view and another node is tried.
    - If connection succeeds, a NEIGHBOUR request is sent with a priority value.

    @WHEN a node in the active view is detected as failed (due to the failure of a TCP connection):
    - A random node q is selected from the passive view.
    - A TCP connection to q is attamped. If it fails, q is removed from the passive view and another node is tried.
    - If connection succeeds, a NEIGHBOR request is sent with a priority value.
    */
    private void tryPromoteNeighbor() {
        if (activeView.size() >= maxActive || passiveView.isEmpty())
            return;

        Host q = getRandom(passiveView);
        pending.add(q);
        openConnection(q);

        boolean highPriority = activeView.isEmpty();
        sendMessage(new NeighborMessage(self, highPriority), q);
        logger.debug("[{}] Trying to promote {} (High priority: {})", self, q, highPriority);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("[{}] Connection to {} is up", self, peer);
        //pending.remove(peer);
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("[{}] Connection to {} is down cause {}", self, peer, event.getCause());
        if (activeView.remove(peer)) {
            triggerNotification(new NeighbourDown(peer));
                        logger.trace("[{}] Removed {} from Active View due to TCP Down. Current Active: {}", self, peer, activeView);
            tryPromoteNeighbor();
        }
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        Host peer = event.getNode();
        logger.debug("[{}] Connection to {} failed cause: {}", self, event.getNode(), event.getCause());
        pending.remove(event.getNode());

        if(passiveView.remove(peer)){
            logger.trace("[{}] Removed {} from Passive View due to TCP Connection failure.", self, peer);
            tryPromoteNeighbor();
        }
    }

    //If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    // (not the smartest protocol, but its simple)
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("[{}] Connection from {} is up", self, event.getNode());
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("[{}] Connection from {} is down, cause: {}", self, event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present at runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("HyParView Metrics:\n");
        sb.append("Active: ").append(activeView).append("\n");
        sb.append("Passive: ").append(passiveView).append("\n");
        sb.append("Pending: ").append(pending).append("\n");
        logger.info(sb);
    }

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}


/*
GOALS:
Obtain high values of reliability in gossip broadcast protocols operating on top of it, aiming at 100%.
Use low values of anout, in the order of log(n).
Support large fractions of concurrent node failures.
Minimize the number of messages whose realiability is affected by massive failures.

OVERVIEW:
Relies on the use of two disting partial views: an active view and a passive view.
The active view of all nodes define an overaly used for message dissemination.
Links define by active view are symmetric: if q is in p's active view, then p is in q's active view.
TCP is used for all communication - as transport and as an unreliable failure detector.
The gossip target selection is deterministic: flood all active view members (except the sender).
The overlay itself is built at random by the membership protocol - combining determinism in dissemination with topology randomness.

ACTIVE VIEW:
Size: Fanout + 1 (small, typically 5).
Symmetric: Links are bidirectional by construction.
Strategy: Maintained using a reactive strategy - only changes in response to node joins or failures.
TCP connections: Each node keeps an open TCP connection to every member. Feasible because the view is very small.
Failure detection: The entire active view is testes implicitly at every broadcast step (assumes flood gossip)

PASSIVE VIEW:
Size: larger than log(n) by some factor C.
Purpose: a backup list of nodes that can replace failed members of an active view.
Strategy: maintained using a cyclic strategy - periodically refreshed via shuffle exchanges.
No open connections: nodes in the passive view are not connected. Overhead is minimal
Connectivity insurance: ensures the system can remain connected even under very high failure rates.
This hybrid approach - small reactive active view + large cyclic passive view - is the key novelty of HyParView

JOIN MECHANISM:
A new node n must know at least one contact node c already in the overlay (similar to Cyclon)
A new node (say n) adds c to its active view, establishes a TCP connection to c and sends a JOIN request.
c adds n to its active view (dropping a random node if full, sending it a DISCONNECT)
c propagates a FORWARDJOIN (with n's identifier and a TLL) to all other active view members.
Two configuration parameters govern the walk:
    ARWL (Active Random Walk Length): maximum number of hops the FORWARDJOIN propagates, the node where the random walk terminates adds n to its active view and replites to n (JOINREPLY)
    PRWL (Passive Random Walk Length): the hop at which n is inserted into a passive view.

@WHEN a node p receives a FORWARDJOIN it performs the following steps in sequence:
    - if TTL == 0 or p has only one active view member: add n to active view (dropping a random node if full)
    - if TTL == PRWL: add n to passive view.
    - Decrement TTL
    - if n was not added to the active view: forward FORWARDJOIN to a random active view member (different from the sender).
    - Note: a new node ends up in the active view of one node and the passive view of another node along the random walk for each random walk for each random walk - spreading it spresence in the overlay

ACTIVE VIEW MANAGEMENT:
@WHEN a node in the active view is detected as failed (due to the failure of a TCP connection):
    - A random node q is selected from the passive view.
    - A TCP connection to q is attemped. If it fails, q is removed from the passive view and another node is tried.
    - If connection succeeds, a NEIGHBOUR request is sent with a priority value.

@WHEN a node in the active view is detected as failed (due to the failure of a TCP connection):
    - A random node q is selected from the passive view.
    - A TCP connection to q is attamped. If it fails, q is removed from the passive view and another node is tried.
    - If connection succeeds, a NEIGHBOR request is sent with a priority value.

This means failure detection is very fast: all active view members are tested at every broadcast.
The use of TCP as a failure detector is key: a node failure is discovered within a single gossip round, not after a full membership cycle. If the failure was not real that also does not affect the correctness of the proccess.

The NEIGHBOR request carries a priority value:
    - High priority: the sender has no elements in its active view. The recipient always accepts (drops a random node from its view if full, sending a DISCONNECT).
    - Low priority: the sender still has other active view members. The recipient only accepts if it has a free slot.

If the request is accepted: q is moved from passive to active view.
If the request is rejected: q stays in the passive view, another passive view node is tried.

PASSIVE VIEW MANAGEMENT:
Periodically each node initiates a SHUFFLE operation to refresh its passive view (inspired in Cyclon).
The initiating node builds an exchange list containing:
    - Its own identifier.
    - ka nodes from its active view.
    - kp nodes from its passive view.
The SHUFFLE travels as a random walk (with TTL) through the overlay; The node where it stops sends back a SHUFFLEREPLY with an equal number of its own passive view entries.
Both nodes integrade received identifiers into their passive views, evicting old entires if necessary, giving preference to evict nodes sent to other peer. This gradually removes dead nodes and introduces live ones.
*/
