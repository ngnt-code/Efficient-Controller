package laboratory.controller.efficiency;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.felix.dm.Component;
import org.opendaylight.controller.forwardingrulesmanager.IForwardingRulesManager;
import org.opendaylight.controller.hosttracker.IfIptoHost;
import org.opendaylight.controller.protocol_plugin.openflow.core.IController;
import org.opendaylight.controller.sal.core.ComponentActivatorAbstractBase;
import org.opendaylight.controller.sal.packet.IDataPacketService;
import org.opendaylight.controller.sal.packet.IListenDataPacket;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.opendaylight.controller.topologymanager.ITopologyManagerAware;

public class Activator extends ComponentActivatorAbstractBase {
    public Object[] getImplementations() {
        Object[] res = { LoadCollection.class };
        return res;
    }

    @Override
    public void configureInstance(Component c, Object imp, String containerName) {
        if (imp.equals(LoadCollection.class)) {
            // export the service needed to listen to topology updates
             final Dictionary<String, Object> props = new Hashtable<String, Object>();
             props.put("StatisgetListenerName", "Statisget");
             props.put("salListenerName", "datapacketget");
             props.put("topoListenerName", "topoget");
             //listen topology to update topo & handle packet_in
             c.setInterface(new String[] { ITopologyManagerAware.class.getName(),
                     IListenDataPacket.class.getName() }, props);

//             c.add(createServiceDependency().setService(IConnectionManager.class)
//                     .setCallbacks("setIConnectionManager", "unsetIConnectionManager").setRequired(false));
           //topology service
             c.add(createServiceDependency()
                     .setService(IController.class, "(name=Controller)")
                     .setCallbacks("setController", "unsetController")
                     .setRequired(true));
            c.add(createContainerServiceDependency(containerName).setService(
                    ITopologyManager.class).setCallbacks(
                    "setTopologyManager", "unsetTopologyManager")
                    .setRequired(true));
            c.add(createContainerServiceDependency(containerName).setService(IfIptoHost.class)
                    .setCallbacks("setHostTracker", "unsetHostTracker").setRequired(true));
            c.add(createContainerServiceDependency(containerName).setService(
                    IDataPacketService.class).setCallbacks(
                    "setDataPacketService",
                    "unsetDataPacketService").setRequired(true));
            c.add(createContainerServiceDependency(containerName)
                    .setService(IForwardingRulesManager.class).setCallbacks("setForwarder", "unsetForwarder")
                    .setRequired(true));

            //statistics service
//            c.add(createContainerServiceDependency(containerName).setService(
//                    IStatisticsManager.class).setCallbacks(
//                    "setStatisticsManager", "unsetStatisticsManager")
//                    .setRequired(false));
            //flowprogrammer service
//            c.add(createContainerServiceDependency(containerName).setService(
//                    IFlowProgrammerService.class).setCallbacks("setFlowProgrammerService",
//                    "unsetFlowProgrammerService").setRequired(true));
            //switch service
//          c.add(createContainerServiceDependency(containerName).setService(
//                  ISwitchManager.class).setCallbacks(
//                  "setSwitchManager", "unsetSwitchManager")
//                  .setRequired(false));
        }
    }

}
