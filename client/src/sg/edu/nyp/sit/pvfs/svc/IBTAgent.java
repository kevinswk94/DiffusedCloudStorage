package sg.edu.nyp.sit.pvfs.svc;

import java.util.List;

import javax.bluetooth.RemoteDevice;

/**
 * Interface for classes that wants to implement bluetooth related functions
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public interface IBTAgent {
	/**
	 * Callback method when the bluetooth discovery is completed
	 * 
	 * @param arrDevices List of devices found during the discovery process
	 */
	public void deviceDiscoveryCompleted(List<RemoteDevice> arrDevices);
}
