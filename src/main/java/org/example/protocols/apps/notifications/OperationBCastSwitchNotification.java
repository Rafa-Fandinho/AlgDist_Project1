package org.example.protocols.apps.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class OperationBCastSwitchNotification extends ProtoNotification {

	public final static short NOTIFICATION_ID = 9998;
	
	public OperationBCastSwitchNotification() {
		super(NOTIFICATION_ID);
		
	}

}
