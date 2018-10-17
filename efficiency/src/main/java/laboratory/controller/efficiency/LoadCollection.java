package laboratory.controller.efficiency;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.osgi.framework.console.CommandInterpreter;
import org.eclipse.osgi.framework.console.CommandProvider;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;
import org.opendaylight.controller.forwardingrulesmanager.FlowEntry;
import org.opendaylight.controller.forwardingrulesmanager.IForwardingRulesManager;
import org.opendaylight.controller.hosttracker.IfIptoHost;
import org.opendaylight.controller.hosttracker.hostAware.HostNodeConnector;
import org.opendaylight.controller.protocol_plugin.openflow.core.IController;
import org.opendaylight.controller.protocol_plugin.openflow.core.ISwitch;
import org.opendaylight.controller.sal.action.Action;
import org.opendaylight.controller.sal.action.Output;
import org.opendaylight.controller.sal.action.SetDlDst;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.Property;
import org.opendaylight.controller.sal.core.UpdateType;
import org.opendaylight.controller.sal.flowprogrammer.Flow;
import org.opendaylight.controller.sal.match.Match;
import org.opendaylight.controller.sal.match.MatchField;
import org.opendaylight.controller.sal.match.MatchType;
import org.opendaylight.controller.sal.packet.Ethernet;
import org.opendaylight.controller.sal.packet.IDataPacketService;
import org.opendaylight.controller.sal.packet.IListenDataPacket;
import org.opendaylight.controller.sal.packet.IPv4;
import org.opendaylight.controller.sal.packet.Packet;
import org.opendaylight.controller.sal.packet.PacketResult;
import org.opendaylight.controller.sal.packet.RawPacket;
import org.opendaylight.controller.sal.packet.UDP;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.sal.utils.EtherTypes;
import org.opendaylight.controller.sal.utils.NetUtils;
import org.opendaylight.controller.sal.utils.Status;
import org.opendaylight.controller.sal.utils.StatusCode;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.opendaylight.controller.topologymanager.ITopologyManagerAware;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.util.EdgeType;

/***
 * Rewrite the function of flow install and calculate the load balance.
 * @author ngnt-ycy
 *
 */
public class LoadCollection implements CommandProvider, ITopologyManagerAware, IListenDataPacket {
    private static Logger log = LoggerFactory.getLogger(LoadCollection.class);
    private ITopologyManager topologyManager = null;
    private IDataPacketService dataPacketService = null;
    private IForwardingRulesManager forwarder = null;
//    private IConnectionManager connectionManager = null;
    private IfIptoHost hostTracker = null;
    private Graph<Node, Edge> topo;
    private DijkstraShortestPath<Node, Edge> shortPath;
    private static AtomicInteger numsOfPacketIn = new AtomicInteger(0);
//    private static int numsOfPacketIn = 0;
    private static AtomicLong processTime = new AtomicLong(0L);
    private static ConcurrentMap<Node, AtomicInteger> loadMap = new ConcurrentHashMap<Node, AtomicInteger>();

    private IController controller;
    private static Map<Long, ISwitch> switches;
//  private IStatisticsManager statisticsManager = null;
//  private IFlowProgrammerService programmer = null;
//  private ISwitchManager switchManager = null;

    // int tranform net address
    static private InetAddress intToInetAddress(int i) {
        byte b[] = new byte[] { (byte) ((i >> 24) & 0xff),
                (byte) ((i >> 16) & 0xff), (byte) ((i >> 8) & 0xff),
                (byte) (i & 0xff) };
        InetAddress addr;
        try {
            addr = InetAddress.getByAddress(b);
        } catch (UnknownHostException e) {

            return null;
        }
        return addr;
    }

    public void init() {
        log.debug("flowInstall init() is called");
        topo = new SparseMultigraph<Node, Edge>();
        shortPath = new DijkstraShortestPath<Node, Edge>(topo);
    }
    
    void start() {
        registerWithOSGIConsole();
        Map<Edge, Set<Property>> edges = topologyManager.getEdges();
        if (edges.isEmpty()) {
            return;
        }
        List<TopoEdgeUpdate> topoedgeupdateList = new ArrayList<TopoEdgeUpdate>();
        log.debug("Creating routing database from the topology");
        for (Iterator<Map.Entry<Edge, Set<Property>>> i = edges.entrySet()
                .iterator(); i.hasNext();) {
            Map.Entry<Edge, Set<Property>> entry = i.next();
            Edge e = entry.getKey();
            Set<Property> props = entry.getValue();
            TopoEdgeUpdate topoedgeupdate = new TopoEdgeUpdate(e, props,
                    UpdateType.ADDED);
            topoedgeupdateList.add(topoedgeupdate);
        }
        edgeUpdate(topoedgeupdateList);
    }
    
    void stop() {
        
    }
    
    private boolean installFlow(String flowName, Flow flow, Node node) {
    	flow.setIdleTimeout((short)1);
    	flow.setHardTimeout((short)1);
        Status status = forwarder.modifyOrAddFlowEntry(new FlowEntry("loadbalanced", flowName, flow, node));
//    	Status status = new Status(StatusCode.SUCCESS);

    	// System.out.println(flow);
        if (!status.isSuccess()) {
            log.error(
                    "SDN Plugin failed to program the flow: {} @ {}. Failure is: {}",
                    flow, node, status.getDescription());
            return false;
        } else {
            // log.info(flow.toString());
            return true;
        }
    }
    
    private Path getshortPath(Node src, Node dst) {
        List<Edge> path = shortPath.getPath(src, dst);// call shortpath function
        Path res;
        try {
            res = new Path(path);
        } catch (ConstructionException e) {
            log.debug("A vertex is yet not known between {} {}", src, dst);
            return null;
        }
        return res;
    }
    

    public PacketResult receiveDataPacket(RawPacket inPkt) {
    	long startTime = System.nanoTime();
        if (inPkt == null) {
            return PacketResult.IGNORED;
        }
        Packet fmtPkt = this.dataPacketService.decodeDataPacket(inPkt);
        if (!(fmtPkt instanceof Ethernet)) {
            return PacketResult.IGNORED;
        } 
        else if (fmtPkt.getPayload() instanceof IPv4) {
            final IPv4 ipv4 = (IPv4) fmtPkt.getPayload();
            NodeConnector incoming_connector = inPkt.getIncomingNodeConnector();
            Node snode = incoming_connector.getNode();
            // load collecting here.
            // Maybe concurrency happens, but the effect of this is small.
            
            numsOfPacketIn.incrementAndGet();
            if ( !loadMap.containsKey(snode)) {
            	loadMap.putIfAbsent(snode, new AtomicInteger(1));
            }
            else {
            	loadMap.get(snode).incrementAndGet();
            }
            int num = loadMap.get(snode).get();
            try {
                int srip = ipv4.getSourceAddress(), dvip = ipv4.getDestinationAddress();
                InetAddress sIP = NetUtils.getInetAddress(srip);
                InetAddress dIP = NetUtils.getInetAddress(dvip);
                
                if (dIP == null) {
                    return PacketResult.IGNORED;
                }

                HostNodeConnector host = hostTracker.hostFind(dIP);

                Node dnode = host.getnodeconnectorNode();

                Match smatch = new Match();
                smatch.setField(new MatchField(MatchType.DL_TYPE, EtherTypes.IPv4.shortValue()));
//                smatch.setField(new MatchField(MatchType.DL_TYPE, EtherTypes.IPv4.shortValue()));
                smatch.setField(new MatchField(MatchType.NW_SRC, intToInetAddress(srip)));
                smatch.setField(new MatchField(MatchType.NW_DST, intToInetAddress(dvip)));
            	if ( ipv4.getPayload() instanceof UDP ) {
                	final UDP udp = (UDP) ipv4.getPayload();
                	short dport = udp.getDestinationPort();
                	if ( dport != (short)67 ) {
                        smatch.setField(new MatchField(MatchType.TP_DST, dport));
                        smatch.setField(new MatchField(MatchType.NW_PROTO, (byte)0x11));
                	}
            	}

                Match dmatch=new Match();
                dmatch.setField(new MatchField(MatchType.DL_TYPE, EtherTypes.IPv4.shortValue()));
                dmatch.setField(new MatchField(MatchType.NW_SRC, intToInetAddress(dvip)));
                dmatch.setField(new MatchField(MatchType.NW_DST, intToInetAddress(srip)));
            	if ( ipv4.getPayload() instanceof UDP ) {
                	final UDP udp = (UDP) ipv4.getPayload();
                	short dport = udp.getDestinationPort();
                	if ( dport != (short)67 ) {
                        smatch.setField(new MatchField(MatchType.TP_SRC, dport));
                        smatch.setField(new MatchField(MatchType.NW_PROTO, (byte)0x11));
                	}
            	}

                if(!snode.equals(dnode)){
                	Path go = getshortPath(snode, dnode);
//                	for (Edge edge : go.getEdges()) {
//                		NodeConnector sec = edge.getTailNodeConnector();
//                		Node hnode = sec.getNode();
//                		List<Action> acts = new ArrayList<Action>(1);
//                		acts.add(new Output(sec));
//                		installFlow("LoadBalance", new Flow(smatch.clone(), acts), hnode);
//                	}
//                	
//                	for (Edge edge : go.getEdges()) {
//                		NodeConnector sec = edge.getHeadNodeConnector();
//                		Node hnode = sec.getNode();
//                		List<Action> acts = new ArrayList<Action>(1);
//                		acts.add(new Output(sec));
//                		installFlow("LoadBalance", new Flow(dmatch.clone(), acts), hnode);
//                	}
                	List<Edge> list = go.getEdges();
                	if ( list.size() != 0 ) {
                		for( Edge edge: list) {
                			NodeConnector sec = edge.getTailNodeConnector();
                			Node hnode = sec.getNode();
                    		List<Action> acts = new ArrayList<Action>(1);
                    		acts.add(new Output(sec));
                    		installFlow("LoadBalance", new Flow(smatch.clone(), acts), hnode);
                		}
                        RawPacket rp = this.dataPacketService.encodeDataPacket(ipv4.getParent());
//                        rp.setOutgoingNodeConnector(sec);
                        rp.setOutgoingNodeConnector(host.getnodeConnector());
                        this.dataPacketService.transmitDataPacket(rp);
                	}
                }
                else {
                	List<Action> acts = new ArrayList<Action>(1);
                	acts.add(new SetDlDst(host.getDataLayerAddressBytes()));
                	installFlow("LoadBalance", new Flow(smatch.clone(), acts), dnode);
                    RawPacket rp = this.dataPacketService.encodeDataPacket(ipv4.getParent());
                    rp.setOutgoingNodeConnector(host.getnodeConnector());
                    this.dataPacketService.transmitDataPacket(rp);
                }
//                	acts = new ArrayList<Action>(2);
//                	acts.add(new SetDlDst(hostTracker.hostFind(sIP).getDataLayerAddressBytes()));
//                	acts.add(new Output(incoming_connector));
//                	installFlow("multi", new Flow(dmatch.clone(), acts), snode);

//                RawPacket rp = this.dataPacketService.encodeDataPacket(ipv4.getParent());
//                rp.setOutgoingNodeConnector(incoming_connector);
//                rp.setOutgoingNodeConnector(host.getnodeConnector());
                return PacketResult.KEEP_PROCESSING;
            }
            finally {
          	    long endTime=System.nanoTime();
          	    processTime.addAndGet(endTime-startTime);
            }
        }
//        else {
//        	log.info("others: {}", fmtPkt.getPayload());
//        }
        return PacketResult.IGNORED;
    }
    
    public static int getNumbersOfPacketIn() { // Maybe concurrency happens, but the effect of this is small.
//    	return numsOfPacketIn;
    	return numsOfPacketIn.get();
    }
    
    public static int getAndClearPacketIn() {
//    	return numsOfPacketIn;
    	return numsOfPacketIn.getAndSet(0);
    }
    
    public static long getAndClearProcessTime() {
    	return processTime.getAndSet(0L);
    }
    
    public static Map<Node, AtomicInteger> getAndRenewLoadMap() {
    	Map<Node, AtomicInteger> clonedMap = new HashMap<Node, AtomicInteger>(loadMap); // forbidding the modification of loadMap.

		Iterator<Map.Entry<Node, AtomicInteger>> entries = loadMap.entrySet().iterator();
		ConcurrentMap<Node, AtomicInteger> newMap = new ConcurrentHashMap<Node, AtomicInteger>();
        while (entries.hasNext()) {
        	Map.Entry<Node, AtomicInteger> entry = entries.next();
        	newMap.put(entry.getKey(), new AtomicInteger(0));
        }
        loadMap = newMap;
    	return clonedMap;
    }
    
    public void _testL(CommandInterpreter ci){
    	System.out.println(LoadCollection.getRtt());
    }
    
    public void _lct(CommandInterpreter ci) {
    	System.out.println(LoadCollection.getNumbersOfPacketIn());
    }

    public static Map<Long, Long> getRtt() {
    	Map<Long, Long> rttMap = new HashMap<Long, Long>();
    	if ( switches == null || switches.size() == 0) {
    		return rttMap;
    	}
    	for (Map.Entry<Long, ISwitch> entry : switches.entrySet()) {
    		rttMap.put(entry.getKey(), entry.getValue().getRtt());
    	}  
    	return rttMap;
    }

    private synchronized boolean updateTopo(Edge edge, UpdateType type) {
        boolean edgePresentInGraph = false;
        // spt = new DijkstraShortestPath(topo);
        NodeConnector src = edge.getTailNodeConnector();
        NodeConnector dst = edge.getHeadNodeConnector();

        switch (type) {
        case ADDED: // Make sure the vertex are there before adding the edge
            topo.addVertex(src.getNode());
            topo.addVertex(dst.getNode());
            // Add the link between
            edgePresentInGraph = topo.containsEdge(edge);
            if (!edgePresentInGraph) {
                try {
                    topo.addEdge(new Edge(src, dst), src.getNode(),
                            dst.getNode(), EdgeType.DIRECTED);

                } catch (final ConstructionException e) {
                    log.error("", e);
                    return edgePresentInGraph;
                }
            }
        case CHANGED: // Mainly raised only on properties update, so not really
                      // useful in this case
            break;
        case REMOVED: // Remove the edge
            try {
                topo.removeEdge(new Edge(src, dst));
            } catch (final ConstructionException e) {
                log.error("", e);
                return edgePresentInGraph;
            }

            // If the src and dst vertex don't have incoming or
            // outgoing links we can get ride of them
            if (topo.containsVertex(src.getNode())
                    && (topo.inDegree(src.getNode()) == 0)
                    && (topo.outDegree(src.getNode()) == 0)) {
                log.debug("Removing vertex {}", src);
                topo.removeVertex(src.getNode());
            }
            if (topo.containsVertex(dst.getNode())
                    && (topo.inDegree(dst.getNode()) == 0)
                    && (topo.outDegree(dst.getNode()) == 0)) {
                log.debug("Removing vertex {}", dst);
                topo.removeVertex(dst.getNode());
            }
            break;
        }
        shortPath.reset(); // something wrong? just reset?

        return edgePresentInGraph;
    }

    private boolean edgeUpdate(Edge e, UpdateType type, Set<Property> props,
            boolean local) {
        // log.trace("Got an edgeUpdate: {} of type {} with props: {} local? {}",
        // new Object[] { e, type,
        // props, local });

        if ((e == null) || (type == null)) {
            log.warn("Edge or Update type are null!");
            return false;
        }
        String srcType = e.getTailNodeConnector().getType();
        String dstType = e.getHeadNodeConnector().getType();

        if (srcType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)
                || dstType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
            log.debug("Skip updates for {}", e);
            return false;
        }

        return !updateTopo(e, type);
        // Update base topo
        // if (newEdge && bw.getValue() != baseBW) {
        // Update BW topo // updateTopo(e, type);
    }

    // topology update. All edges updating will call this function including which is not local.
    public void edgeUpdate(List<TopoEdgeUpdate> topoedgeupdateList) {
        for (TopoEdgeUpdate u : topoedgeupdateList) { // e, type, p, isLocal
            edgeUpdate(u.getEdge(), u.getUpdateType(), u.getProperty(), u.isLocal());
        }
    }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

    public void edgeOverUtilized(Edge edge) {
        
    }

    public void edgeUtilBackToNormal(Edge edge) {
        
    }

//    public void setIConnectionManager(IConnectionManager s) {
//        this.connectionManager = s;
//    }
//
//    public void unsetIConnectionManager(IConnectionManager s) {
//        if (s == this.connectionManager) {
//            this.connectionManager = null;
//        }
//    }
    public void setController(IController core) {
        this.controller = core;
        LoadCollection.switches = this.controller.getSwitches();
    }

    public void unsetController(IController core) {
        if (this.controller == core) {
            this.controller = null;
        }
    }

    public void setDataPacketService(IDataPacketService s) {
        log.trace("Set DataPacketService.");
        this.dataPacketService = s;
    }

    public void unsetDataPacketService(IDataPacketService s) {
        log.trace("Removed DataPacketService.");

        if (this.dataPacketService == s) {
            this.dataPacketService = null;
        }
    }

    public void setTopologyManager(ITopologyManager tm) {
        this.topologyManager = tm;
    }

    public void unsetTopologyManager(ITopologyManager tm) {
        if (this.topologyManager == tm) {
            this.topologyManager = null;
        }
    }

    public void setForwarder(IForwardingRulesManager s) {
        this.forwarder = s;
    }

    public void unsetForwarder(IForwardingRulesManager s) {
        if (this.forwarder == s) {
            this.forwarder = null;
        }
    }

    public void setHostTracker(IfIptoHost hostTracker) {
        log.trace("Setting HostTracker");
        this.hostTracker = hostTracker;
    }

    public void unsetHostTracker(IfIptoHost s) {
        log.trace("UNSetting HostTracker");
        if (this.hostTracker == s) {
            this.hostTracker = null;
        }
    }
    
//    public void setStatisticsManager(IStatisticsManager s) {
//        log.debug("StatisticsManager set");
//        this.statisticsManager = s;
//    }
//
//    public void unsetStatisticsManager(IStatisticsManager s) {
//        if (this.statisticsManager == s) {
//            log.debug("StatisticsManager removed!");
//            this.statisticsManager = null;
//        }
//    }
//
//    public void setFlowProgrammerService(IFlowProgrammerService s) {
//        log.trace("Set FlowProgrammerService.");
//
//        this.programmer = s;
//    }
//
//    public void unsetFlowProgrammerService(IFlowProgrammerService s) {
//        log.trace("Removed FlowProgrammerService.");
//
//        if (this.programmer == s) {
//            this.programmer = null;
//        }
//    }
//    
//    public void setSwitchManager(ISwitchManager s) {
//        log.trace("Set SwitchManager.");
//        this.switchManager = s;
//    }
//
//    public void unsetSwitchManager(ISwitchManager s) {
//        log.trace("Removed SwitchManager.");
//        if (this.switchManager == s) {
//             this.switchManager = null;
//        }
//    }


    public String getHelp() {
        StringBuffer help = new StringBuffer();
        help.append("--- Load Collection commands---\n");
        help.append("load collection : load collection function\n");
        return help.toString();
    }
    
    private void registerWithOSGIConsole() {
        BundleContext bundleContext = FrameworkUtil.getBundle(this.getClass())
                .getBundleContext();
        bundleContext.registerService(CommandProvider.class.getName(), this,
                null);
    }

    public void _lcProp(CommandInterpreter ci) {
    	long startTime = System.nanoTime();
    	try {
    		property();
            System.out.println("----------------------------------"); 
            cpu(); 
            System.out.println("----------------------------------");  
            memory(); 
            System.out.println("----------------------------------");  
            os(); 
            System.out.println("----------------------------------"); 
            net(); 
            System.out.println("----------------------------------"); 
    	} catch (Exception e1) { 
            e1.printStackTrace(); 
        } 
  	    long endTime=System.nanoTime();
  	    System.out.println("Process Time:"+(endTime-startTime)+"ns");
    }
    
    public void _lcNet(CommandInterpreter ci) {
    	long startTime = System.nanoTime();
    	try {
    		specifiedNet();
            System.out.println("----------------------------------"); 
    	} catch (Exception e1) { 
            e1.printStackTrace(); 
        } 
  	    long endTime=System.nanoTime();
  	    System.out.println("Process Time:"+(endTime-startTime)+"ns");
    }
    
    private static void property() throws UnknownHostException { 
        Runtime r = Runtime.getRuntime(); 
        Properties props = System.getProperties(); 
        InetAddress addr; 
        addr = InetAddress.getLocalHost(); 
        String ip = addr.getHostAddress(); 
        System.out.println("OS name:\t" + props.getProperty("os.name")); 
        System.out.println("OS architecture:\t" + props.getProperty("os.arch")); 
        System.out.println("OS version:\t" + props.getProperty("os.version")); 
        System.out.println("Java version:\t" + props.getProperty("java.version")); 
        System.out.println("JVM specification name:\t " + props.getProperty("java.vm.specification.name")); 
        System.out.println("JVM specification version:\t" + props.getProperty("java.vm.specification.version")); 
        System.out.println("JVM release name:\t" + props.getProperty("java.vm.name")); 
        System.out.println("JVM release version:\t" + props.getProperty("java.vm.version")); 
        System.out.println("Java specification name:\t" + props.getProperty("java.specification.name")); 
        System.out.println("Java specification version:\t" + props.getProperty("java.specification.version")); 
        System.out.println("Java library path:\t" + props.getProperty("java.library.path")); 
        System.out.println("JVM total memory:\t" + r.totalMemory()); // <--------
        System.out.println("JVM free memory:\t" + r.freeMemory());   // <--------
        System.out.println("JVM available process:\t" + r.availableProcessors()); 
        System.out.println("Java IO temp directory:\t" + props.getProperty("java.io.tmpdir")); 
        System.out.println("IP Address:\t" + ip); 
    } 

    private static void cpu() throws SigarException { 
        Sigar sigar = new Sigar(); 
        CpuInfo infos[] = sigar.getCpuInfoList(); 
        CpuPerc cpuList[] = null; 
        cpuList = sigar.getCpuPercList(); 
        for (int i = 0; i < infos.length; i++) { 
            CpuInfo info = infos[i]; 
            System.out.println("CPU [" + (i + 1) + "]"); 
            System.out.println("CPU Frequency: \t" + info.getMhz());
            System.out.println("CPU MFRS.:\t" + info.getVendor());
            System.out.println("CPU Category:\t" + info.getModel());
            System.out.println("CPU Cache Size:\t" + info.getCacheSize()); 
            printCpuPerc(cpuList[i]); 
            if( i != infos.length - 1) {
            	System.out.println();
            }
        } 
    } 

    private static void printCpuPerc(CpuPerc cpu) { 
        System.out.println("CPU us: \t" + CpuPerc.format(cpu.getUser()));  
        System.out.println("CPU sys: \t" + CpuPerc.format(cpu.getSys())); 
        System.out.println("CPU wa: \t" + CpuPerc.format(cpu.getWait())); 
        System.out.println("CPU ni: \t" + CpuPerc.format(cpu.getNice()));
        System.out.println("CPU id: \t" + CpuPerc.format(cpu.getIdle())); 
        System.out.println("CPU total usage: \t" + CpuPerc.format(cpu.getCombined())); // <--------
    } 

    private static void memory() throws SigarException { 
        Sigar sigar = new Sigar(); 
        Mem mem = sigar.getMem();  
        System.out.println("Total Memory: \t" + mem.getTotal() / 1024L + "K av");   // <-------- ?
        System.out.println("Used Memory: \t" + mem.getUsed() / 1024L + "K used");   // <-------- ?
        System.out.println("Free Memory: \t" + mem.getFree() / 1024L + "K free");   // <-------- ?
        Swap swap = sigar.getSwap();  
        System.out.println("Total Swap: \t" + swap.getTotal() / 1024L + "K av");  
        System.out.println("Used Swap: \t" + swap.getUsed() / 1024L + "K used"); 
        System.out.println("Free Swap: \t" + swap.getFree() / 1024L + "K free"); 
    } 

    private static void os() { 
        OperatingSystem OS = OperatingSystem.getInstance(); 
        System.out.println("OS Decription: \t" + OS.getDescription());  
        System.out.println("OS Version: \t" + OS.getVersion()); 
        System.out.println("OS Architecture: \t" + OS.getArch()); 
        System.out.println("OS CPU Endian: \t" + OS.getCpuEndian());
        System.out.println("OS Data Model: \t" + OS.getDataModel());  
        // System.out.println("OS Vendor: \t" + OS.getVendor());  
        // System.out.println("OS Vendor CodeName \t:    " + OS.getVendorCodeName());  
        System.out.println("OS Vendor Name: \t" + OS.getVendorName());  
        System.out.println("OS Vendor Version: \t" + OS.getVendorVersion()); 
    } 

    private static void specifiedNet() throws Exception { // <--------
        Sigar sigar = new Sigar(); 
        String ifNames[] = sigar.getNetInterfaceList();
        String provisions = System.getProperty("net.provisioning"); // read from ./configuration/config.ini
        String[] array = provisions.split(";");
        Set<String> provision = new HashSet<String>();
        for( String str: array) {
        	provision.add(str);
        }
        for (int i = 0; i < ifNames.length; i++) { 
            String name = ifNames[i]; 
            if ( !provision.contains(name) ) {
            	continue;
            }
            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name); 
            System.out.println("Network Name: \t" + name); 
            System.out.println("IP Address: \t" + ifconfig.getAddress()); 
            System.out.println("Net Mask: \t" + ifconfig.getNetmask()); 
            if ((ifconfig.getFlags() & 1L) <= 0L) { 
                System.out.println("!IFF_UP...skipping getNetInterfaceStat"); 
                continue; 
            } 
            NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name); 
            System.out.println(name + " rx packets: \t" + ifstat.getRxPackets()); // minus
            System.out.println(name + " tx packets: \t" + ifstat.getTxPackets()); 
            System.out.println(name + " rx bytes: \t" + ifstat.getRxBytes()); 
            System.out.println(name + " tx bytes: \t" + ifstat.getTxBytes()); 
            System.out.println(name + " rx errors: \t" + ifstat.getRxErrors()); 
            System.out.println(name + " tx errors: \t" + ifstat.getTxErrors()); 
            System.out.println(name + " rx dropped: \t" + ifstat.getRxDropped());
            System.out.println(name + " tx dropped: \t" + ifstat.getTxDropped()); 
        }
    }

    private static void net() throws Exception { 
        Sigar sigar = new Sigar(); 
        String ifNames[] = sigar.getNetInterfaceList();
        for (int i = 0; i < ifNames.length; i++) { 
            String name = ifNames[i]; 
            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name); 
            System.out.println("Network Name: \t" + name); 
            System.out.println("IP Address: \t" + ifconfig.getAddress()); 
            System.out.println("Net Mask: \t" + ifconfig.getNetmask());  
            if ((ifconfig.getFlags() & 1L) <= 0L) { 
                System.out.println("!IFF_UP...skipping getNetInterfaceStat"); 
                continue; 
            } 
            NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name); 
            System.out.println(name + " rx packets: \t" + ifstat.getRxPackets());
            System.out.println(name + " tx packets: \t" + ifstat.getTxPackets()); 
            System.out.println(name + " rx bytes: \t" + ifstat.getRxBytes()); 
            System.out.println(name + " tx bytes: \t" + ifstat.getTxBytes()); 
            System.out.println(name + " rx errors: \t" + ifstat.getRxErrors()); 
            System.out.println(name + " tx errors: \t" + ifstat.getTxErrors()); 
            System.out.println(name + " rx dropped: \t" + ifstat.getRxDropped());
            System.out.println(name + " tx dropped: \t" + ifstat.getTxDropped());  
        } 
    } 
}
