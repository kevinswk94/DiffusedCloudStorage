package sg.edu.nyp.sit.pvfs.svc;

import java.util.ArrayList;

import javax.bluetooth.BluetoothStateException;
import javax.bluetooth.DeviceClass;
import javax.bluetooth.DiscoveryAgent;
import javax.bluetooth.DiscoveryListener;
import javax.bluetooth.LocalDevice;
import javax.bluetooth.RemoteDevice;
import javax.bluetooth.ServiceRecord;
import javax.bluetooth.UUID;

import sg.edu.nyp.sit.pvfs.Main;

/**
 * Bluetooth agent that performs various bluetooth tasks (connection, discovery etc) on behalf of the calling class
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class BTAgent implements DiscoveryListener {
	private static UUID[] uuids = new UUID[]{Main.btUUID};
	
	private ArrayList<RemoteDevice> arrDevices=new ArrayList<RemoteDevice>();
	private String connUrl=null;
	private Object lock=new Object();
	
	private final DiscoveryAgent agent;
	private final IBTAgent client;
	
	/**
	 * Creates a bluetooth agent object and enable callbacks for object that implements the IBTAgent interface 
	 * 
	 * @param client Any object that implements the IBTAgent interface
	 * 
	 * @throws BluetoothStateException Occurs when there is problem retrieving the bluetooth stack
	 */
	public BTAgent(IBTAgent client) throws BluetoothStateException{
		agent = LocalDevice.getLocalDevice().getDiscoveryAgent();
		this.client=client;
	}
	
	/**
	 * Creates a bluetooth agent object
	 * 
	 * @throws BluetoothStateException Occurs when there is problem retrieving the bluetooth stack
	 */
	public BTAgent() throws BluetoothStateException{
		agent = LocalDevice.getLocalDevice().getDiscoveryAgent();
		this.client=null;
	}
	
	/**
	 * Starts a bluetooth device discovery process
	 * 
	 * @throws BluetoothStateException Occurs when there is problem accessing the bluetooth stack
	 */
	public void startDiscovery() throws BluetoothStateException {
		agent.cancelInquiry(this);
		arrDevices.clear();
		
		agent.startInquiry(DiscoveryAgent.GIAC, this);
	}
	
	/**
	 * Cancels the ongoing bluetooth device discovery process
	 */
	public void cancelDiscovery(){
		agent.cancelInquiry(this);
	}
	
	/**
	 * Search for the specific service on the given bluetooth client. 
	 * 
	 * The UUID used must match the UUID found in the sg.edu.nyp.sit.pvfs.pvfs.Main class. This method will block until the search is complete.
	 * 
	 * @see sg.edu.nyp.sit.pvfs.Main#btUUID
	 * @param d Given bluetooth client to search for the service
	 * @return The service connection URL
	 */
	public String findDeviceService(RemoteDevice d){
		try {
			agent.searchServices(null,uuids,d,this);
			
			try { synchronized(lock){ lock.wait(); } } 
		    catch (InterruptedException e) { e.printStackTrace(); }
		    
		    return connUrl;
		} catch (BluetoothStateException ex) {
			ex.printStackTrace();
			return null;
		} 
	}
	
	/**
	 * Callback method by the bluetooth implementation library when each device is discovered by the discovery process.
	 * To be used internally.
	 */
	@Override
	public void deviceDiscovered(RemoteDevice btDevice, DeviceClass cod) {
		//add the device to the vector
        if(!arrDevices.contains(btDevice)){
        	arrDevices.add(btDevice);
        }
	}

	/**
	 * Callback method by the bluetooth implementation library when the device discovery is completed.
	 * This method will in turn called the IBTAgent's interface to inform the calling method that the discovery is completed. 
	 * To be used internally.
	 * 
	 * @see sg.edu.nyp.sit.pvfs.svc.IBTAgent#deviceDiscoveryCompleted(java.util.List)
	 */
	@Override
	public void inquiryCompleted(int discType) {
		if(client!=null)client.deviceDiscoveryCompleted(arrDevices);
	}

	/**
	 * Callback method by the bluetooth implementation library when the service search is completed on the given bluetooth client.
	 * To be used internally.
	 * 
	 * @see sg.edu.nyp.sit.pvfs.svc.BTAgent#findDeviceService(RemoteDevice)
	 */
	@Override
	public void serviceSearchCompleted(int transID, int respCode) {
		synchronized(lock){ lock.notify(); }
	}

	/**
	 * Callback method by the bluetooth implementation library when the specific service is found on the given bluetooth client.
	 * To be used internally.
	 * 
	 * @see sg.edu.nyp.sit.pvfs.svc.BTAgent#findDeviceService(RemoteDevice)
	 */
	@Override
	public void servicesDiscovered(int transID, ServiceRecord[] servRecord) {
		if(servRecord!=null && servRecord.length>0){
        	connUrl=servRecord[0].getConnectionURL(0,false);
        }
	}
}
