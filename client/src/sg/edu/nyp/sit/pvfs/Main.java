package sg.edu.nyp.sit.pvfs;

import java.awt.Image;
import java.io.File;
import java.util.HashSet;

import javax.bluetooth.RemoteDevice;
import javax.bluetooth.UUID;
import javax.microedition.io.StreamConnection;

import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.svds.metadata.User;

/**
 * Wrapper class to be used by classes implementing the IMain interface. 
 * 
 * This class provides a point of entry of related classes to access the methods and properties the need to know the exact class that is used
 * to start the PVFS application.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class Main {
	public static final long serialVersionUID = 2L;
	
	public static final String REMOTE_ENDED_MSG="Remote session has ended. Virtual drive is unmounted.";
	
	/**
	 * A unique ID that represents the bluetooth service in both the PC and mobile application
	 */
	public static final UUID btUUID = new UUID("FF7D900303A7959F270B3A53EED6A345", false);
	/**
	 * The name of the PVFS application
	 */
	public static final String sysName="Personal Virtual File System";
	/**
	 * The icon used for the PVFS application
	 */
	public static Image sysIcon=null;
	
	private static IMain m;
	
	private static User usr=null;
	
	/**
	 * Sets the exact class object that was started as the PVFS application
	 *  
	 * @param obj Object that implements the IMain interface
	 */
	public static void init(IMain obj){
		m=obj;
	}
	
	public static void setUser(User usr){
		Main.usr=usr;
	}
	
	public static User getUser(){
		return usr;
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#isDriveMounted()
	 */
	public static boolean isDriveMounted(){
		return m.isDriveMounted();
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#driveMounted(IVirtualDisk, PVFSSvc)
	 */
	public static void driveMounted(IVirtualDisk fs, PVFSSvc svc){
		m.driveMounted(fs, svc);
	}
	
	public static void driveMounted(IVirtualDisk fs){
		m.driveMounted(fs);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#driveUnmounted()
	 */
	public static void driveUnmounted(){
		m.driveUnmounted();
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#getDirectConnectPassword(String)
	 */
	public static String getDirectConnectPassword(String title){
		return m.getDirectConnectPassword(title);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showError(String)
	 */
	public static void showError(String msg){
		m.showError(msg);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showMsg(String)
	 */
	public static void showMsg(String msg){
		m.showMsg(msg);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showTrayError(String)
	 */
	public static void showTrayError(String msg){
		m.showTrayError(msg);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showTrayMsg(String)
	 */
	public static void showTrayMsg(String msg){
		m.showTrayMsg(msg);
	}
	
	/**
	 * 
	 * @see sg.edu.nyp.sit.pvfs.IMain#deviceConnected(StreamConnection, RemoteDevice)
	 */
	public static void deviceConnected(StreamConnection c, RemoteDevice d) throws Exception{
		m.deviceConnected(c, d);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#updateDirectLogonPwd(String)
	 */
	public static void updateDirectLogonPwd(String pwd){
		m.updateDirectLogonPwd(pwd);
	}
	
	/**
	 * Finds a available letter in the PC (A-Z) to assign to the virtual drive. The default letter is T.
	 * This method uses the listRoots method in java.io.File which will only return drives that are
	 * accessible (if the drive is a network drive, it must be in connected state).
	 * For more reliable checking, show also run the command "net use" to
	 * list all the network drives mapped (including those disconnected ones)
	 * then process the output to find the drive letter in used for each one.
	 * 
	 * However, for current similicity sake, check against the list returned
	 * by the listRoots method will do.
	 * 
	 * @return
	 * @throws InstantiationException Occurs when there are no available letters 
	 */
	public static char getAvailableDriveLetter() throws InstantiationException{
		HashSet<Character> usedDrives=new HashSet<Character>();
		for(File d: File.listRoots()){
			usedDrives.add(d.getPath().charAt(0));
		}
		
		char defLetter='T';
		if(!usedDrives.contains(defLetter)) return Character.toLowerCase(defLetter);

		for(int i='D'; i<='Z'; i++){
			if(!usedDrives.contains((char)i)) return Character.toLowerCase((char)i);
		}
		
		throw new InstantiationException("Unable to locate free drive letter.");
	}
}
