package org.example.protocols.membership.cyclon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.membership.common.notifications.ChannelCreated;
import org.example.protocols.membership.common.notifications.NeighbourDown;
import org.example.protocols.membership.common.notifications.NeighbourUp;
import org.example.protocols.membership.cyclon.messages.ShuffleReplyMessage;
import org.example.protocols.membership.cyclon.messages.ShuffleRequestMessage;
import org.example.protocols.membership.cyclon.timers.ShuffleTimer;
import org.example.protocols.membership.full.timers.InfoTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class CyclonMembership extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(CyclonMembership.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;        //For instance
    public final static String PROTOCOL_NAME = "Cyclon";

    private final Host self;     //My own address/port
    private final Map<Host, Integer> neigh; //Set of neighbors of the process (partial view)
    private int maxN;    //maximum number of neighbors
    private Map<Host,Integer> sample; //Sample of neighbors sent to the other process in the last shuffle

    public final static String PAR_MAX_NEIGHBORS = "protocol.membership.maxN";
    public final static String PAR_DEFAULT_MAX_NEIGHBORS = "15"; //For instance

    public final static String PAR_SAMPLE_TIME = "protocol.membership.sampletime";
    public final static String PAR_DEFAULT_SAMPLE_TIME = "1000"; //1 seconds
    private final int sampleTime; //param: timeout for samples

    public final static String PAR_SAMPLE_SIZE = "protocol.membership.samplesize";
    public final static String PAR_DEFAULT_SAMPLE_SIZE = "8";
    private final int subsetSize; //param: maximum size of sample;

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public CyclonMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.neigh = new HashMap<>();
        this.sample = new HashMap<>();

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
        registerMessageSerializer(channelId, ShuffleRequestMessage.MSG_ID, ShuffleRequestMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_ID, ShuffleReplyMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, ShuffleRequestMessage.MSG_ID, this::uponShuffleRequest, this::uponMsgFail);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        this.maxN=Integer.parseInt(props.getProperty(PAR_MAX_NEIGHBORS, PAR_DEFAULT_MAX_NEIGHBORS));

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                neigh.put(contactHost,0);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Setup the timer used to execute shuffles (we registered its handler on the constructor)
        setupPeriodicTimer(new ShuffleTimer(), this.sampleTime, this.sampleTime);

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }



    /*--------------------------------- Requests ---------------------------------------- */
    private Map<Host,Integer> uponGetNeighbors(){
        return new HashMap<>(neigh);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponShuffleRequest(ShuffleRequestMessage msg, Host from, short sourceProto, int channelId) {
        //Received a shuffle request from a neighbor. We answer with a sample of our own neighbors and merge the received host set
        //with our own neighbor list.
        logger.debug("Received {} from {}", msg, from);
        openConnection(from);
        Map<Host,Integer> temporarySample = getRandomSubsetExcluding(neigh,subsetSize,from);
        sendMessage(new ShuffleReplyMessage(temporarySample),from);
        mergeViews(msg.getSample(),temporarySample);
    }
    private void uponShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId) {
        //Received a shuffle response from a neighbor. We merge the received host set with our own neighbor list
        logger.debug("Received {} from {}", msg, from);
        mergeViews(msg.getSample(),sample);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void mergeViews(Map<Host,Integer> peerSample, Map<Host, Integer> mySample) {
        for(Map.Entry<Host,Integer> peerEntry: peerSample.entrySet()) {
            if (peerEntry.getKey().equals(self)) continue;

            if(neigh.containsKey(peerEntry.getKey())) {
                // already have the neighbour, update only if the age is lower
                if(neigh.get(peerEntry.getKey()) > peerEntry.getValue()) {
                    neigh.put(peerEntry.getKey(), peerEntry.getValue());
                }
            }

            else if(neigh.size() < maxN) {
                // neighbourhood not full yet
                neigh.put(peerEntry.getKey(), peerEntry.getValue());
                openConnection(peerEntry.getKey());
            }

            else {
                // There is no space in the neighbourhood, we have to remove
                Host hostToRemove = null;
                for(Host myHost: mySample.keySet()) {
                    if (neigh.containsKey(myHost)) {
                        hostToRemove=myHost;
                        break;
                    }
                }
                if(hostToRemove == null) {
                    hostToRemove = getRandom(neigh.keySet());
                }

                // safe remove
                if (hostToRemove != null) {
                    neigh.remove(hostToRemove);

                    // IMPORTANT: only close the connection if it actually was opened
                    triggerNotification(new NeighbourDown(hostToRemove));
                    closeConnection(hostToRemove);
                }

                // add the new one
                neigh.put(peerEntry.getKey(), peerEntry.getValue());
                openConnection(peerEntry.getKey());
            }
        }
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        //When the ShuffleTimer is triggered, increment the age of all registered neighbors and send a shuffle request
        logger.debug("Shuffle Time: neighbourhood{}", neigh);
        for(Map.Entry<Host,Integer> neighbor : neigh.entrySet()){
            neighbor.setValue(neighbor.getValue()+1);
        }
        Host p = getOldest();
        if(p != null){
            sample=getRandomSubsetExcluding(neigh,subsetSize-1,p);
            sample.put(self,0);

            neigh.remove(p);

            openConnection(p);
            sendMessage(new ShuffleRequestMessage(sample), p);
        }
    }

    //Gets a random element from the set of peers
    private Host getOldest() {
        int highestAge=-1;
        Host oldest = null;
        for (Host h : neigh.keySet()) {
            if (highestAge < neigh.get(h)){
                oldest = h;
                highestAge = neigh.get(h);
            }
        }
        return oldest;
    }

    //Gets a random element from the set of peers
    private Host getRandom(Set<Host> hostSet) {
        if (hostSet.isEmpty()) return null;
        int idx = rnd.nextInt(hostSet.size());
        int i = 0;
        for (Host h : hostSet) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }

    //Gets a random subset from the set of neighbors
    private static Map<Host, Integer> getRandomSubsetExcluding(Map<Host, Integer> hostMap, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostMap.keySet());
        list.remove(exclude);
        Collections.shuffle(list);
        int size = Math.min(sampleSize, list.size());
        Map<Host, Integer> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            Host host = list.get(i);
            map.put(host, hostMap.get(host));
        }
        return map;
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();

        // only notificate if the peer is still in the neigh
        if(neigh.containsKey(peer)) {
            // put peer with age 0
            neigh.put(peer, 0);
            triggerNotification(new NeighbourUp(peer));
            logger.debug("Connection to {} is up", peer);
        } else {
            // if it already left the map , close the connection we opened by mistake
            closeConnection(peer);
        }
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        neigh.remove(event.getNode());
        triggerNotification(new NeighbourDown(event.getNode()));
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
    }

    //If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    // (not the smartest protocol, but its simple)
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present at runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("Membership Metrics:\n");
        sb.append("Neighbors: ").append(neigh).append("\n");
        sb.append("Sample: ").append(sample).append("\n");
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