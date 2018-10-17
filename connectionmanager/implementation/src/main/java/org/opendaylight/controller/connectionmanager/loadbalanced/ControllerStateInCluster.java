package org.opendaylight.controller.connectionmanager.loadbalanced;

import java.io.Serializable;
import java.util.Map;

public class ControllerStateInCluster implements Serializable {
	private static final long serialVersionUID = -2951885851207432553L;
	private double packetInAvailable;
	private Map<Long, Long> rtt;
	private long timeStamp; // minimal interval between migration
	private ControllerLocalState state;
	
	public ControllerStateInCluster(double packetIns, Map<Long, Long> rtt, ControllerLocalState state) {
		this.packetInAvailable = packetIns;
		this.rtt = rtt;
		timeStamp = 0L;
		this.state = state;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public double getPacketInAvailable() {
		return packetInAvailable;
	}
	
	public Map<Long, Long> getRtt() {
		return rtt;
	}

	public ControllerLocalState getState() {
		return state;
	}

	public void setState(ControllerLocalState state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return "ControllerStateInCluster [packetInAvailable="
				+ packetInAvailable + ", rtt=" + rtt + ", timeStamp="
				+ timeStamp + ", state=" + state + "]\n";
	}

	public String str() {
		return "ControllerStateInCluster [packetInAvailable="
				+ packetInAvailable +  ", timeStamp="
				+ timeStamp + ", state=" + state + "]\n";
	}
}
