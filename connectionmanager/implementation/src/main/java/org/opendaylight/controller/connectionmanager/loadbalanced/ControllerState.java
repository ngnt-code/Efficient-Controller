package org.opendaylight.controller.connectionmanager.loadbalanced;

import java.util.Map;

public class ControllerState {
	private double cpuLeft;
	private long memLeft;
	private long netErrors;
	private long netRxByte;
	private long netTxByte;
	private double packetIns;
	private long processTime;
	private Map<Long, Long> rtt;
	
	public ControllerState(double cpu, long mem, long net, long netRxByte, long netTxByte, double packets, long pt, Map<Long, Long> rtt){
		this.cpuLeft = cpu;
		this.memLeft = mem;
		this.netErrors = net;
		this.netRxByte = netRxByte;
		this.netTxByte = netTxByte;
		this.packetIns = packets;
		this.processTime = pt;
		this.rtt = rtt;
	}

	public double getCpuLeft() {
		return cpuLeft;
	}

	public long getMemLeft() {
		return memLeft;
	}

	public long getNetErrors() {
		return netErrors;
	}

	public double getPacketIns() {
		return packetIns;
	}

	public void setPacketIns(double packetIns) {
		this.packetIns = packetIns;
	}

	public long getProcessTime() {
		return processTime;
	}

	public Map<Long, Long> getRtt() {
		return rtt;
	}
	
	public long getNetRxByte() {
		return netRxByte;
	}

	public long getNetTxByte() {
		return netTxByte;
	}

	@Override
	public String toString() {
		return "ControllerState [cpuLeft=" + cpuLeft + ", memLeft=" + memLeft
				+ ", netErrors=" + netErrors + ", packetIns=" + packetIns
				+ ", processTime=" + processTime + ", rtt=" + rtt + "]\n";
	}
}


