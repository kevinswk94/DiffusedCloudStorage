package sg.edu.nyp.sit.pvfs;

import javax.bluetooth.RemoteDevice;
import javax.microedition.io.StreamConnection;

import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.svds.metadata.User;

/**
 * Interface for PVFS application
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public interface IMain {
	public static final long serialVersionUID = 2L;
	
	/**
	 * To check if the virtual drive is created.
	 * 
	 * Implementation is optional if the implementing class does not support creation of virtual drive.
	 * 
	 * @return True if the virtual drive is created, false otherwise
	 */
	public boolean isDriveMounted();
	/**
	 * Callback method when the connection has been established with the mobile application and the virtual drive is created.
	 * 
	 * Implementation is optional if the implementing class does not support creation of virtual drive.
	 * 
	 * @param fs Virtual disk (implements the IVirtualDisk interface) that is created
	 * @param svc Object that handles the connection between the PC and the mobile application
	 */
	public void driveMounted(IVirtualDisk fs, PVFSSvc svc);
	
	public void driveMounted(IVirtualDisk fs);
	
	/**
	 * To dismount the virtual drive.
	 * 
	 * Implementation is optional if the implementing class does not support creation of virtual drive.
	 */
	public void driveUnmounted();
	
	/**
	 * Callback method when the connection has been established with the mobile application
	 * 
	 * @param c Object (implements javax.microedition.io.StreamConnection) representing the bluetooth connection with the mobile application
	 * @param d	Object (implements javax.bluetooth.RemoteDevice) representing the mobile device which the bluetooth connection is made 
	 */
	public void deviceConnected(StreamConnection c, RemoteDevice d);
	
	public void proxyConnected(User usr);
	
	/**
	 * Implementation should define how the application gets password from the user to authenticate to the mobile application
	 * 
	 * @param title 
	 * @return The password that user has input
	 */
	public String getDirectConnectPassword(String title);
	
	public User getProxyConnectPassword();
	
	/**
	 * Displays a error message to the user
	 * 
	 * @param msg Error message to display
	 */
	public void showError(String msg);
	/**
	 * Display a informative message to the user
	 * 
	 * @param msg Message to display
	 */
	public void showMsg(String msg);
	/**
	 * Display a error message in the system tray
	 * 
	 * @param msg Error message to display
	 */
	public void showTrayError(String msg);
	/**
	 * Display a informative message in the system tray
	 * 
	 * @param msg Message to display
	 */
	public void showTrayMsg(String msg);
	
	/**
	 * Updates the password that is used to authenticate to the mobile application.
	 * 
	 * @param pwd Password to update
	 */
	public void updateDirectLogonPwd(String pwd);
}
