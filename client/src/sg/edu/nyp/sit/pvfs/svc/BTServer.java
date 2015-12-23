package sg.edu.nyp.sit.pvfs.svc;

import java.io.IOException;

import javax.bluetooth.LocalDevice;
import javax.bluetooth.RemoteDevice;
import javax.bluetooth.ServiceRecord;
import javax.bluetooth.UUID;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;
import javax.microedition.io.StreamConnectionNotifier;

import sg.edu.nyp.sit.pvfs.Main;

/**
 * Class that runs a thread as a bluetooth server
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class BTServer extends Thread{
	public static final long serialVersionUID = 2L;
	
	private StreamConnectionNotifier svr=null;
	private final UUID btUUID;
	private boolean isDone=false;
	
	/**
	 * Creates a bluetooth server using the given service ID
	 * 
	 * @param btUUID Service ID to uniquely identify the bluetooth server
	 */
	public BTServer(UUID btUUID){
		this.btUUID=btUUID;
	}
	
	/**
	 * Starts the bluetooth server to listen for connection request.
	 * This method should not be called directly by calling class.
	 * 
	 * @see java.lang.Thread#start()
	 */
	public void run(){
		//Create the service URL
		String connStr = "btspp://localhost:" + btUUID + ";name=PVFS_pc;authenticate=false;encrypt=false";

		try {
			//Open server URL
			svr = (StreamConnectionNotifier)Connector.open(connStr);

			//Wait for client connection
			System.out.print("\nServer started. ");
			//get service record
			ServiceRecord svrRec = LocalDevice.getLocalDevice().getRecord(svr);
			System.out.println("Service registered url: " + 
					svrRec.getConnectionURL(ServiceRecord.NOAUTHENTICATE_NOENCRYPT, false));
		}catch(Exception ex){
			ex.printStackTrace();
			Main.showError(ex.getMessage());
			return;
		}
		
		try{
			System.out.println("\nWaiting for clients to connect...");
			
			//this will result in a blocking call until a connection is received
			//NOTE: if bluetooth is disconnected, then this thread will block forever,
			//as it will not be notified of any error
			StreamConnection c = svr.acceptAndOpen();
			
			RemoteDevice d = RemoteDevice.getRemoteDevice(c);
			String dName="";
			try{ dName=d.getFriendlyName(true); }catch (IOException ex){}
			Main.showTrayMsg("Receive connection from [" + d.getBluetoothAddress()+"] " 
					+ dName);
			System.out.println("Received connection from "+ d.getFriendlyName(true) 
					+ " ["+d.getBluetoothAddress()+"]...");
			
			//close the server because only accept 1 client at a time
	        svr.close();
	        svr=null;
	        isDone=true;
	        
	        Main.deviceConnected(c, d);
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception ex){
			Main.showTrayError(ex.getMessage());
		}
	}
	
	/**
	 * To check if a bluetooth connection has been received or is the server has been shutdown
	 * 
	 * @return True if a bluetooth connection has been received or the server has been shutdown, false otherwise
	 */
	public boolean isDone(){
		return isDone;
	}
	
	/**
	 * Shutdowns the bluetooth server
	 */
	public void cancel(){
		try {
			isDone=true;
			if(svr!=null) svr.close();
		} catch (IOException e) { e.printStackTrace(); }
	}
}