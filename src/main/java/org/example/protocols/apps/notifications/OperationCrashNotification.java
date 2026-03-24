package org.example.protocols.apps.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class OperationCrashNotification extends ProtoNotification {

	public final static short NOTIFICATION_ID = 9997;
	
	public OperationCrashNotification() {
		super(NOTIFICATION_ID);
	}

}
