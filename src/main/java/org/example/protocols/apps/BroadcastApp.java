package org.example.protocols.apps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.protocols.apps.notifications.OperationBCastSwitchNotification;
import org.example.protocols.apps.notifications.OperationCrashNotification;
import org.example.protocols.apps.timers.BroadcastTimer;
import org.example.protocols.broadcast.common.BroadcastRequest;
import org.example.protocols.broadcast.common.DeliverNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class BroadcastApp extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(BroadcastApp.class);

    //Protocol information, to register in babel
    public static final String PROTO_NAME = "BroadcastApp";
    public static final short PROTO_ID = 300;

    private final short broadcastProtoId;

    public static final String PAR_APP_PAYLOAD_SIZE = "app.payload.size";
    public static final String DEFAULT_APP_PAYLOAD_SIZE = "1000";
    
    //Size of the payload of each message (in bytes)
    private final int payloadSize;
    
    public static final String PAR_APP_BROADCAST_PERIOD = "app.broadcast.period";
    public static final String DEFAULT_APP_BROADCAST_PERIOD = "3000";
    
    //Interval between each broadcast
    private final int broadcastInterval;

    private final Host self;

    private long broadCastTimer;

    private AtomicBoolean executing;
	
	private String nodeLabel;
	
	private static final String PAR_FILE_FOLDER_TARGET = "app.op.folder";
	private static final String DEFAULT_FILE_FOLDER_TARGET = "~/operations";
	
	private String basePath;
	
	private static final String PAR_FILE_BROADCAST_TRIGGER = "app.op.start.file";
	private static final String DEFAULT_FILE_BROADCAST_TRIGGER = "start.flag";
	
	private String fileBroadcastTrigger;
	
	private static final String PAR_FILE_FAILURE_TRIGGER = "app.of.fail.file";
	private static final String DEFAULT_FILE_FAILURE_TRIGGER = "failure.flag";
	
	private String fileFailureTrigger;
	
	private static final String PAR_FILE_POOLING_PERIOD = "app.op.file.pool";
	private static final long DEFAULT_FILE_POOLING_PERIOD = 1000; 
	
	private long filePoolingPeriod;
	
	private AtomicBoolean fileMonitorExecute = null;
	
	private Thread fileSystemMonitor = new Thread(new Runnable() {
		
		private boolean startFile = false;
		private boolean failureFile = false;
		
		@Override
		public void run() {
			while(fileMonitorExecute.get()) {
				
				try {
					
					if(!startFile && Files.exists(Path.of(basePath, fileBroadcastTrigger))) {
						startFile = true;
						triggerNotification(new OperationBCastSwitchNotification());
					}
					
					else if(startFile && !Files.exists(Path.of(basePath, fileBroadcastTrigger))) {
						startFile = false;
						triggerNotification(new OperationBCastSwitchNotification());
					}
					
					if(!failureFile && Files.exists(Path.of(basePath, fileFailureTrigger))) {
						failureFile = true;
						try (BufferedReader reader = Files.newBufferedReader(Path.of(basePath, fileFailureTrigger))) {
					        String line;
					        while ((line = reader.readLine()) != null) {
					            if (line.contains(nodeLabel)) {
					            	triggerNotification(new OperationCrashNotification());
					            	break;
					            }
					        }
					    }
					}
					
					Thread.sleep(filePoolingPeriod);
					
				} catch (Exception e) {
					//Nothing to be done here
				}
				
			}
		}
	});
    
    public BroadcastApp(Host self, Properties properties, short broadcastProtoId) throws HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.broadcastProtoId = broadcastProtoId;
        this.self = self;

        //Read configurations
        this.payloadSize = Integer.parseInt(properties.getProperty(PAR_APP_PAYLOAD_SIZE,DEFAULT_APP_PAYLOAD_SIZE));
       
        this.broadcastInterval = Integer.parseInt(properties.getProperty(PAR_APP_BROADCAST_PERIOD, DEFAULT_APP_BROADCAST_PERIOD)); //in milliseconds

        //Setup handlers
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
        subscribeNotification(OperationBCastSwitchNotification.NOTIFICATION_ID, this::handleOperationBCastSwitchNotification);
		subscribeNotification(OperationCrashNotification.NOTIFICATION_ID, this::handleOperationCrashNotification);
		
		registerTimerHandler(BroadcastTimer.TIMER_ID, this::uponBroadcastTimer);
		
    }

    @Override
    public void init(Properties props) {
    	this.basePath = props.getProperty(PAR_FILE_FOLDER_TARGET, DEFAULT_FILE_FOLDER_TARGET);
		this.fileBroadcastTrigger = props.getProperty(PAR_FILE_BROADCAST_TRIGGER, DEFAULT_FILE_BROADCAST_TRIGGER);
		this.fileFailureTrigger = props.getProperty(PAR_FILE_FAILURE_TRIGGER, DEFAULT_FILE_FAILURE_TRIGGER);
		
		logger.info(PAR_FILE_FOLDER_TARGET + ": " + this.basePath);
		logger.info(PAR_FILE_BROADCAST_TRIGGER + ": " + this.fileBroadcastTrigger);
		logger.info(PAR_FILE_FAILURE_TRIGGER + ": " + this.fileFailureTrigger);
		
		if(props.containsKey(PAR_FILE_POOLING_PERIOD))
			this.filePoolingPeriod = Long.parseLong(props.getProperty(PAR_FILE_POOLING_PERIOD));
		else 
			this.filePoolingPeriod = DEFAULT_FILE_POOLING_PERIOD;
		
		this.fileMonitorExecute = new AtomicBoolean(true);
		
		this.fileSystemMonitor.setDaemon(true);
		this.fileSystemMonitor.start();
		 
		this.executing = new AtomicBoolean(false);
    }

    private void uponBroadcastTimer(BroadcastTimer broadcastTimer, long timerId) {
    	if(!this.executing.get()) return; //If we are not broadcasting do nothing...
    	
        //Upon triggering the broadcast timer, create a new message
        String toSend = randomCapitalLetters(Math.max(0, payloadSize));
        //ASCII encodes each character as 1 byte
        byte[] payload = toSend.getBytes(StandardCharsets.US_ASCII);

        BroadcastRequest request = new BroadcastRequest(UUID.randomUUID(), self, payload);
        logger.info(getTimeStamp() + " " + nodeLabel + " SEND " + request.getMsgId() + " " + request.getMsg().length);
        //And send it to the dissemination protocol
        sendRequest(request, broadcastProtoId);
    }

    private void handleOperationBCastSwitchNotification(OperationBCastSwitchNotification not, short proto) {
		this.executing.set(!this.executing.get());
		if(this.executing.get()) {
			setupPeriodicTimer(new BroadcastTimer(), new Random(System.currentTimeMillis()).nextInt(broadcastInterval), broadcastInterval);
		} else {
			cancelTimer(broadCastTimer);
		}
	}
	
	private void handleOperationCrashNotification(OperationCrashNotification not, short proto) {
		this.executing.set(false);
		this.fileMonitorExecute.set(false);
		logger.info("Terminating due to failure.");
		System.exit(0);
	}
    
	private String getTimeStamp() {
		return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
		        .format(new java.util.Date());
	}
	
    private void uponDeliver(DeliverNotification msg, short sourceProto) {
    	logger.info(getTimeStamp() + " " + nodeLabel + " RECV " + msg.getMsgId() + " " + msg.getMsg().length);
        
    }

    public static String randomCapitalLetters(int length) {
        int leftLimit = 65; // letter 'A'
        int rightLimit = 90; // letter 'Z'
        Random random = new Random();
        return random.ints(leftLimit, rightLimit + 1).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }
}
