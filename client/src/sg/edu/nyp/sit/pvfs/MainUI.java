package sg.edu.nyp.sit.pvfs;

import java.awt.AWTException;
import java.awt.Menu;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.bluetooth.RemoteDevice;
import javax.microedition.io.StreamConnection;
import javax.swing.ImageIcon;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.svc.BTServer;
import sg.edu.nyp.sit.pvfs.svc.DeviceConnected;
import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.pvfs.svc.ProxyConnected;
import sg.edu.nyp.sit.pvfs.svc.ProxySvc;
import sg.edu.nyp.sit.pvfs.ui.*;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.metadata.User;

/**
 * PVFS application class that implements IMain and sg.edu.nyp.sit.svc.IBTAgent interfaces.
 * 
 * This class provide interface to the user and allows user to create and mount a virtual drive in the PC after connection
 * has been establish with the mobile application.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class MainUI implements IMain {
	public static final long serialVersionUID = 5L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(MainUI.class);

	//static final String namespace="urn:sit.nyp.edu.sg";
	
	//tray menu items
	final Menu proxyMenu = new Menu("Proxy");
	final Menu directMenu=new Menu("Direct");
	
	final MenuItem proxy_connectItem=new MenuItem("Connect");
	final MenuItem proxy_disconnectItem=new MenuItem("Disconnect");
	final MenuItem proxy_sliceStoresItem=new MenuItem("Slice Stores");
	
	final MenuItem direct_unmountItem=new MenuItem("Unmount");
	final MenuItem direct_mountItem=new MenuItem("Mount");
	final MenuItem direct_settingsItem=new MenuItem("Settings");
	final MenuItem direct_sliceStoresItem=new MenuItem("Slice Stores");
	
	//UI forms
	private FormDevice frmDevice=null;
	private DialogAbout dialogAbout=null;
	private DialogPwd dialogPwd=null;
	private DialogProxy dialogProxy=null;
	private FormSettings frmSettings=null;
	private FormSliceStores frmSliceStores=null;
	
	private boolean isMounted=false;
	private PVFSSvc directSvc=null;
	private ProxySvc proxySvc=null;
	private IVirtualDisk fs=null;
	
	private TrayIcon trayIcon=null;
	
	private BTServer btSvr=null;

	public static void main(String[] args) throws Exception {
		MainUI m=new MainUI();
		
		Main.init(m);
		
		//starts the server
		m.startBTServer();
	}
	
	/**
	 * Creates the interface for user to interact with the PVFS application. Also starts the bluetooth server to listen for
	 * incomming connections from mobile application.
	 * @throws Exception 
	 */
	public MainUI() throws Exception{
		try {
			Main.sysIcon=new ImageIcon(Resources.findFile("harddrive.png"), Main.sysName).getImage();
            UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
		}catch(Exception ex){
			ex.printStackTrace();
			throw ex;
		}
		
		if (!SystemTray.isSupported()) {
            //System.out.println("SystemTray is not supported! Application will exit.");
            throw new Exception("System tray is not supported!");
        }
		
		SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI();
            }
        });
	}
	
	public void startBTServer(){
		btSvr=new BTServer(Main.btUUID);
		btSvr.start();
	}
	
	/**
	 * Creates a system tray icon with options for user to select. 
	 */
	private void createAndShowGUI() {
		try{
			trayIcon=new TrayIcon(Main.sysIcon);
		}catch(Exception ex){
			System.out.println("Error locating icon! Application will exit.");
			return;
		}
		trayIcon.setImageAutoSize(true);

		final PopupMenu popup = new PopupMenu();
		final SystemTray tray = SystemTray.getSystemTray();

		final MenuItem exitItem = new MenuItem("Exit");
		final MenuItem aboutItem=new MenuItem("About");

		createDirectMenu();
		createProxyMenu();
		
		popup.add(directMenu);
		popup.add(proxyMenu);
		popup.addSeparator();
		popup.add(aboutItem);
		popup.addSeparator();
		popup.add(exitItem);
		
		//testing
		/*
		final MenuItem tItem=new MenuItem("test C2DM");
		popup.add(tItem);
		tItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	try {
					svc.sendMetadata("UPDATE\t/tt.txt\tabc\t1\t0\t1\ts1\t1\t1\t\ttestFS\t0");
				} catch (Exception ex) {
					ex.printStackTrace();
					showError(ex.getMessage());
				}
            }
        });
        
		final MenuItem tItem=new MenuItem("test");
		popup.add(tItem);
		tItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	try{
            		IMasterNamespaceTable mns=MasterTableFactory.getNamespaceInstance();
            		String[] urls=mns.getSharedAccessURL("s3fs1", "test");
            		for(String n:urls)
            			System.out.println(n);
            	}catch(Exception ex){
            		ex.printStackTrace();
            	}
            }
		});
		*/
		
		trayIcon.setPopupMenu(popup);
		
		try {
            tray.add(trayIcon);
        } catch (AWTException ex) {
            System.out.println("Error adding application to system tray! Application will exit.");
            return;
        }

        aboutItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	if(dialogAbout==null) {
            		try{
            			dialogAbout=new DialogAbout(null);
            		}catch(Exception ex){
            			showTrayError("Error showing about window!");
            		}
            	}
            	
            	dialogAbout.setVisible(true);
            }
        });
        
        exitItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	if(JOptionPane.showConfirmDialog(null, "Are you sure you want to exit?", "Exit", 
            			JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION){
	            	unmountDrive();
	            	if(proxy_disconnectItem.isEnabled()){
	        			try{
	        				if(!proxySvc.checkEnded(Main.getUser())){
	        					if(JOptionPane.showConfirmDialog(null, "Do you want to end your remote session?", "Remote connection", 
	        		        			JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION){
	        						proxySvc.end(Main.getUser());
	        					}
	        				}
	        			}catch(Exception ex){
	        				showError("Unable to connect to proxy server.");
	        			}
	        		}
	            	if(btSvr!=null) btSvr.cancel();
	                tray.remove(trayIcon);
	                System.exit(0);
            	}
            }
        });
	}
	
	private void createProxyMenu(){
		proxyMenu.add(proxy_connectItem);
		proxyMenu.add(proxy_disconnectItem);
		proxyMenu.addSeparator();
		proxyMenu.add(proxy_sliceStoresItem);
		
		proxy_disconnectItem.setEnabled(false);
		proxy_sliceStoresItem.setEnabled(false);
		
		proxy_connectItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	User usr;
            	boolean success;
            	if(proxySvc==null) proxySvc=new ProxySvc();
            	
            	while(true){
            		usr=getProxyConnectPassword();
            		if(usr==null) break;

            		try{
            			success=proxySvc.auth(usr);
            		}catch(Exception ex){
            			showError("Unable to connect to proxy server.");
            			break;
            		}
            		
            		if(!success)
            			showError("Incorrect user ID and/or token.");
            		else{
            			proxyConnected(usr);
            			break;
            		}
            	}
            }
        });
		
		proxy_disconnectItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	//unmountDrive();
            	driveUnmounted();
            }
        });
		
		proxy_sliceStoresItem.addActionListener(new ActionListener(){
       		public void actionPerformed(ActionEvent e) {
            	if(!isMounted){
            		showTrayError("Drive is not mounted. Unable to show settings.");
            		return;
            	}
            	
            	if(frmSliceStores==null) {
	           		try{
	           			frmSliceStores=new FormSliceStores();
	           		}catch(Exception ex){
	           			showTrayError("Error showing slice stores window!");
	           			return;
	           		}
	           	}
           	
            	frmSliceStores.setVisible(true);
       		}
       });
	}
	
	private void createDirectMenu(){
		directMenu.add(direct_mountItem);
		directMenu.add(direct_unmountItem);
		directMenu.addSeparator();
		directMenu.add(direct_sliceStoresItem);
		directMenu.add(direct_settingsItem);
		
		direct_unmountItem.setEnabled(false);
		direct_mountItem.setEnabled(true);
		direct_settingsItem.setEnabled(false);
		direct_sliceStoresItem.setEnabled(false);
		
		direct_mountItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	if(frmDevice==null) {
            		try{
            			frmDevice=new FormDevice();
            		}catch(Exception ex){
            			showTrayError("Error showing device window!");
            		}
            	}
            	
            	frmDevice.setVisible(true);
            }
        });
        
        direct_unmountItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	unmountDrive();
            	showTrayMsg("Drive is unmounted successfully.");
            }
        });
        
        direct_settingsItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
            	if(!isMounted){
            		showTrayError("Drive is not mounted. Unable to show settings.");
            		return;
            	}
            	
            	if(frmSettings==null) {
            		try{
            			frmSettings=new FormSettings();
            		}catch(Exception ex){
            			showTrayError("Error showing settings window!");
            			return;
            		}
            	}
            	
            	frmSettings.setVisible(true);
            }
        });
        
        direct_sliceStoresItem.addActionListener(new ActionListener(){
        	 public void actionPerformed(ActionEvent e) {
             	if(!isMounted){
             		showTrayError("Drive is not mounted. Unable to show settings.");
             		return;
             	}
             	
             	if(frmSliceStores==null) {
            		try{
            			frmSliceStores=new FormSliceStores();
            		}catch(Exception ex){
            			showTrayError("Error showing slice stores window!");
            			return;
            		}
            	}
            	
             	frmSliceStores.setVisible(true);
        	 }
        });
	}
	
	/**
	 * Dismount the virtual drive when the user selects the unmount option in the system tray or exit the application
	 */
	private void unmountDrive(){
		if(!isMounted) return;

		if(directSvc!=null) directSvc.shutdown(false);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#isDriveMounted()
	 */
	@Override
	public boolean isDriveMounted(){
		return isMounted;
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#driveMounted(IVirtualDisk, PVFSSvc)
	 */
	@Override
	public void driveMounted(IVirtualDisk fs, PVFSSvc svc){
		if(fs==null || svc==null) 
			throw new NullPointerException("Object is null.");
		
		isMounted=true;
		
		this.fs=fs;
		this.directSvc=svc;
		
		proxyMenu.setEnabled(false);
		direct_unmountItem.setEnabled(true);
		direct_mountItem.setEnabled(false);
		direct_settingsItem.setEnabled(true);
		direct_sliceStoresItem.setEnabled(true);
		if(frmDevice!=null) frmDevice.setVisible(false);
		
		//cancel the bluetooth server as the user has already init the connection
		//from PC
		if(btSvr!=null && !btSvr.isDone()) btSvr.cancel();
	}
	
	@Override
	public void driveMounted(IVirtualDisk fs){
		if(fs==null)
			throw new NullPointerException("Object is null.");
		
		isMounted=true;
		
		this.fs=fs;
		
		directMenu.setEnabled(false);
		proxy_connectItem.setEnabled(false);
		proxy_disconnectItem.setEnabled(true);
		proxy_sliceStoresItem.setEnabled(true);
		if(frmDevice!=null) frmDevice.setVisible(false);
		
		//cancel the bluetooth server as the user has already init the connection
		//from PC
		if(btSvr!=null && !btSvr.isDone()) btSvr.cancel();
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#driveUnmounted()
	 */
	@Override
	public void driveUnmounted(){
		isMounted=false;
		
		//fs can be null if the user cancel the password prompt (the connection has been made)
		if(fs!=null){
			fs.unmount();
			
			if(fs.requireRestart()){
				//for work around, start a new process running and exit the current process
				//this is because dokan is unable to mount twice in the same process (even if
				//it is previously unmounted)
				try {
					//pause for 3 seconds so the ballon will stay for user to see
					Thread.sleep(1000*3);

					String path=Resources.findFile("start_pvfs.bat");
					if(path.startsWith("/") || path.startsWith("\\")) path=path.substring(1);

					Runtime.getRuntime().exec("cmd /c start /min " +  path.replace("/", "\\"));
				} catch (Exception ex) {
					ex.printStackTrace();
				}finally{
					System.exit(0);
				}
			}
		}
		
		//if the disconnect menu item is enabled, means the user is connecting via remote proxy
		if(proxy_disconnectItem.isEnabled()){
			try{
				if(!proxySvc.checkEnded(Main.getUser())){
					if(JOptionPane.showConfirmDialog(null, "Do you want to end your remote session?", "Remote connection", 
		        			JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION){
						proxySvc.end(Main.getUser());
					}
				}
			}catch(Exception ex){
				showError("Unable to connect to proxy server.");
			}
		}
		
		proxyMenu.setEnabled(true);
		directMenu.setEnabled(true);

		proxy_connectItem.setEnabled(true);
		proxy_disconnectItem.setEnabled(false);
		proxy_sliceStoresItem.setEnabled(false);

		direct_unmountItem.setEnabled(false);
		direct_mountItem.setEnabled(true);
		direct_settingsItem.setEnabled(false);
		direct_sliceStoresItem.setEnabled(false);
		
		if (frmSettings!=null){
			frmSettings.dispose();
			frmSettings=null;
		}
		if(frmSliceStores!=null){
			frmSliceStores.disposeDialogSliceStoresNewEdit();
			frmSliceStores.dispose();
			frmSliceStores=null;
		}

		directSvc=null;
		//fs=null;
		//start the bluetooth server again as the drive is unmounted
		if(btSvr==null || btSvr.isDone()) {
			btSvr=null;
			btSvr=new BTServer(Main.btUUID);
			btSvr.start();
		}
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#getDirectConnectPassword(String)
	 */
	@Override
	public String getDirectConnectPassword(String title){
		/*
		String input;
		
		do{
			input=JOptionPane.showInputDialog(null, msg, sysName, JOptionPane.PLAIN_MESSAGE);
		}while(input==null || input.trim().length()==0);
		
		return input.trim();
		*/
		if(dialogPwd==null){
			dialogPwd=new DialogPwd(null);
		}
		
		dialogPwd.setTitle(title);
		dialogPwd.setVisible(true);
		dialogPwd.toFront();
		
		//if user click cancel, null will be returned
		return dialogPwd.getPwd();
	}
	
	@Override
	public User getProxyConnectPassword(){
		if(dialogProxy==null) {
    		dialogProxy=new DialogProxy(null);
    	}

    	dialogProxy.setVisible(true);
    	dialogProxy.toFront();
    	
    	//if user click cancel, null will be returned
    	return dialogProxy.getLogin();
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showError(String)
	 */
	@Override
	public void showError(final String msg){
		JOptionPane.showMessageDialog(null, msg,
				Main.sysName, JOptionPane.ERROR_MESSAGE);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showMsg(String)
	 */
	@Override
	public void showMsg(String msg){
		JOptionPane.showMessageDialog(null, msg,
				Main.sysName, JOptionPane.INFORMATION_MESSAGE);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showTrayError(String)
	 */
	@Override
	public void showTrayError(String msg){
		trayIcon.displayMessage(Main.sysName, msg, TrayIcon.MessageType.ERROR);
	}
	
	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#showTrayMsg(String)
	 */
	@Override
	public void showTrayMsg(String msg){
		trayIcon.displayMessage(Main.sysName, msg, TrayIcon.MessageType.INFO);
	}

	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#deviceConnected(StreamConnection, RemoteDevice)
	 */
	@Override
	public void deviceConnected(StreamConnection c, RemoteDevice d) {
		try{
			DeviceConnected dc=new DeviceConnected(c, d);
	    	dc.start();
	    	System.out.println("Connected");
		}catch(Exception ex){
			showError(ex.getMessage());
		}
	}
	
	@Override
	public void proxyConnected(User usr){
		try{
			ProxyConnected pc=new ProxyConnected(usr);
			pc.start();
			System.out.println("Connected");
		}catch(Exception ex){
			showError(ex.getMessage());
		}
	}

	/**
	 * @see sg.edu.nyp.sit.pvfs.IMain#updateDirectLogonPwd(String)
	 */
	@Override
	public void updateDirectLogonPwd(String pwd) {
		if(directSvc!=null) directSvc.updateLoginPwd(pwd);
	}

}
