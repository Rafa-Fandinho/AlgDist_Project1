package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.apps.BroadcastApp;
import org.example.protocols.broadcast.eagerpush.EagerPushBroadcast;
import org.example.protocols.broadcast.flood.FloodBroadcast;
import org.example.protocols.membership.cyclon.CyclonMembership;
import org.example.protocols.membership.full.GossipBasedFullMembership;
import org.example.protocols.membership.hyparview.HyParViewMembership;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.data.Host;
import org.example.utils.InterfaceToIp;

import java.net.InetAddress;
import java.util.Properties;


public class Main {

    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "babel_config.properties";

    public static void main(String[] args) throws Exception {

        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();
        
        //Loads properties from the configuration file, and merges them with properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the IP of that interface
        //and create a property "address=ip" to be used later by the channels.
        InterfaceToIp.addInterfaceIp(props);

        //The Host object is an address/port pair that represents a network host. It is used extensively in babel
        //It implements equals and hashCode, and also includes a serializer that makes it easy to use in network messages
        Host myself =  new Host(InetAddress.getByName(props.getProperty("babel.address")),
                Integer.parseInt(props.getProperty("babel.port")));

        logger.info("Hello, I am {}", myself);

        // Choose which protocol to use
        String membershipType = props.getProperty("membership", "full");
        String broadcastType = props.getProperty("broadcast", "flood");

        // Broadcast Protocol
        GenericProtocol broadcast = switch (broadcastType.toLowerCase()) {
            case "flood" -> new FloodBroadcast(props, myself);
            case "eagerpush" -> new EagerPushBroadcast(props, myself);
            default -> throw new IllegalArgumentException("Unknown broadcast: " + broadcastType);
        };

        // Membership Protocol
        GenericProtocol membership = switch (membershipType.toLowerCase()) {
            case "cyclon" -> new CyclonMembership(props, myself);
            case "hyparview" -> new HyParViewMembership(props, myself);
            case "full" -> new GossipBasedFullMembership(props, myself);
            default -> throw new IllegalArgumentException("Unknown membership: " + membershipType);
        };

        // Application
        BroadcastApp broadcastApp = new BroadcastApp(myself, props, broadcast.getProtoId());

        logger.info("Using membership: {}", membershipType);
        logger.info("Using broadcast: {}", broadcastType);

        //Register applications in babel
        babel.registerProtocol(broadcastApp);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(membership);

        //Init the protocols. This should be done after creating all protocols, since there can be inter-protocol
        //communications in this step.
        broadcastApp.init(props);
        broadcast.init(props);
        membership.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));

    }

}
