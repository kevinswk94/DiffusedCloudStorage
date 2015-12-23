package sg.edu.nyp.sit.pvfs.svc;

import java.io.IOException;

import javax.bluetooth.RemoteDevice;
import javax.microedition.io.StreamConnection;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.pvfs.virtualdisk.VirtualDiskFactory;
import sg.edu.nyp.sit.svds.metadata.User;

/**
 * Runs as a thread to create the virtual drive after the bluetooth connection is established
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class DeviceConnected extends Thread {
	public static final long serialVersionUID = 4L;
	
	private final StreamConnection c;
	private final RemoteDevice d;
	private final char mountLetter;
	
	/**
	 * Creates a device connected object given the bluetooth connection (that is already established) and the connecting bluetooth device
	 * 
	 * @param c Bluetooth connection that is already established
	 * @param d Connecting bluetooth device
	 * @throws InstantiationException Occurrs when the virtual drive is already created
	 */
	public DeviceConnected(StreamConnection c, RemoteDevice d)
		throws InstantiationException {
		if(Main.isDriveMounted()){
			try { c.close(); } catch (IOException e) { e.printStackTrace(); }
			throw new InstantiationException ("Drive is already mounted!");
		}
		
		this.c=c;
		this.d=d;
		this.mountLetter=Main.getAvailableDriveLetter();
	}
	
	/**
	 * Starts a new PVFSSvc to maintain the bluetooth connection between the PC and the mobile application.
	 * Also creates the virtual drive for PC.
	 * This method should not be called directly by calling class.
	 * 
	 * @see java.lang.Thread#start()
	 */
	public void run(){
		PVFSSvc svc=new PVFSSvc(c, d);
		svc.start();
		
		//mounts the virtual drive (dokan or eldos)
		int status;
		//checks if the PC bluetooth master implementation is ready to handle request and response
		//only proceed if it is ready
		while((status=svc.getMasterStatus())==0){
			Thread.yield();
		}
		if(status==-1){
			//if indication should stop, the thread should exit instead of waiting forever
			return;
		}
		
		System.out.println("Ready to mount");
		
		try{ 
			//pass in dummy user object as direct connection does not need a user
			User usr=new User("owner", null);
			IVirtualDisk fs=VirtualDiskFactory.getInstance(usr);
			if(fs==null) throw new NullPointerException("Unable to get instance of virtual disk.");
			
			Main.setUser(usr);
			Main.driveMounted(fs, svc);	
			Main.showTrayMsg("Drive has been mounted successfully.");
			
			//mount the drive the last because it seems to "take the thread" (for dokan)
			fs.mount(mountLetter);
		}catch(Exception ex){
			ex.printStackTrace();
			Main.showTrayError("Error occurred with mounted drive.");
			svc.shutdown(false); 
		}
	}
}
