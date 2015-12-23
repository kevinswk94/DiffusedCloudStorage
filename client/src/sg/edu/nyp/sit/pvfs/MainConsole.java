package sg.edu.nyp.sit.pvfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;

import javax.bluetooth.RemoteDevice;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.svc.BTAgent;
import sg.edu.nyp.sit.pvfs.svc.BTServer;
import sg.edu.nyp.sit.pvfs.svc.IBTAgent;
import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

/**
 * PVFS application class that implements IMain and sg.edu.nyp.sit.svc.IBTAgent interfaces.
 * 
 * This class provides interface to the user through the use of the console. It is mainly used for developer testing.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class MainConsole implements IMain, IBTAgent {
	public static final long serialVersionUID = 4L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(MainConsole.class);
	
	private static final int MODE_SVR=1;
	private static final int MODE_CLIENT=2;
	
	private static BufferedReader in;
	
	public static void main(String args[]) throws Exception{
		in=new BufferedReader(new InputStreamReader(System.in));
		
		MainConsole mc=new MainConsole();
		Main.init(mc);
		mc.run();
	}
	
	public void run() throws Exception{
		int mode=getStartSvrOrClient();
		
		if(mode==MODE_SVR){
			(new BTServer(Main.btUUID)).run();
		}else if(mode==MODE_CLIENT){
			startClient();
		}
	}

	private int getStartSvrOrClient() throws IOException{
		String input;
		
		do{
			System.out.print("Mode [1=server, 2=client, q=quit]? ");
			input=in.readLine().trim();
		}while(input.isEmpty() || (!input.equals(MODE_SVR+"") && !input.equals(MODE_CLIENT+"") 
				&& !input.equalsIgnoreCase("q")));
		
		return input.equalsIgnoreCase("q") ? -1 : Integer.parseInt(input);
	}
	
	private Object lock=new Object();
	private List<RemoteDevice> lstDevices=null;
	private void startClient() throws Exception{
		BTAgent b=new BTAgent(this);
		b.startDiscovery();

		try { synchronized(lock){ lock.wait(); } } 
		catch (InterruptedException e) { e.printStackTrace(); }

		if(lstDevices.size()==0){
			System.out.println("No devices found. Exiting...");
			return;
		}

		int cnt=1;
		for(RemoteDevice d: lstDevices){
			System.out.print("\n"+ cnt +". "+d.getBluetoothAddress());
			try{ System.out.print("("+d.getFriendlyName(true)+")"); }
			catch(IOException ex){	System.out.print("(null)"); }

			cnt++;
		}
		
		String deviceOpt;
		int deviceSel=-1;
		do{
			System.out.print("\nChoose device to connect (1"+(cnt-1==1? "" : " - "+(cnt-1))+", q to quit): ");
			deviceOpt=in.readLine().trim();
			if(deviceOpt.isEmpty())
				deviceSel=-1;
			else if (deviceOpt.equalsIgnoreCase("q"))
				deviceSel=0;
			else{
				try{ deviceSel=Integer.parseInt(deviceOpt); } 
				catch(NumberFormatException ex){ deviceSel=-1; }
			}
		}while(deviceSel<0 || deviceSel>=cnt);

		if(deviceSel==0) return;
		
		RemoteDevice d=lstDevices.get(deviceSel-1);
		String connUrl=b.findDeviceService(d);
		
		if (connUrl==null){
	    	 System.out.println("Service not found. Exiting...");
	    	 return;
	     }
		
		deviceConnected((StreamConnection)Connector.open(connUrl), d);
	}

	@Override
	public void deviceDiscoveryCompleted(List<RemoteDevice> arrDevices) {
		lstDevices=arrDevices;
		synchronized(lock){ lock.notify(); }
	}

	@Override
	public boolean isDriveMounted() {
		//not implemented
		return false;
	}

	@Override
	public void driveMounted(IVirtualDisk fs, PVFSSvc svc) {
		//not implemented
	}

	@Override
	public void driveUnmounted() {
		//not implemented
	}

	@Override
	public String getDirectConnectPassword(String title) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void showError(String msg) {
		System.out.println(msg);
	}

	@Override
	public void showMsg(String msg) {
		System.out.println(msg);
	}

	@Override
	public void showTrayError(String msg) {
		System.out.println(msg);
	}

	@Override
	public void showTrayMsg(String msg) {
		System.out.println(msg);
	}

	@Override
	public void deviceConnected(StreamConnection c, RemoteDevice d){
		try{
			System.out.println("device connected method invoked");
			PVFSSvc svc=new PVFSSvc(c, d);
			svc.start();
			
			//mounts dokan
			int status;
			while((status=svc.getMasterStatus())==0){
				Thread.yield();
			}
			if(status==-1){
				return;
			}
			
			//TODO: any testing here, note that if this function exits, the application will stop
			//System.out.println("Invoking method.");
			//MasterTableFactory.getFileInstance().refreshDirectoryFiles("test", FileInfo.PATH_SEPARATOR, new Date(), new User("test", "test"));
			//System.out.println("Done invoking method");
			
			IMasterFileTable mt=MasterTableFactory.getFileInstance();
			
			mt.refreshDirectoryFiles("test", FileInfo.PATH_SEPARATOR, new Date(), new User("test", "test"));
			//System.out.println("Done invoking method..waiting for 60 seconds");
			
			//Thread.sleep(1000*60);
			
			//System.out.println("Invoke method again");
			//mt.refreshDirectoryFiles("test", FileInfo.PATH_SEPARATOR, new Date(), new User("test", "test"));
			System.out.println("Done invoking method");
			
			//(new TestThread1(1)).run();
			//(new TestThread(0)).run();
			//(new TestThread(0)).run();
			
			//while(true){}
			
			svc.shutdown(false);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

	class TestThread extends Thread{
		int w;
		public TestThread(int w){
			this.w=w;
		}
		public void run(){
			try {
				if(w>0)Thread.sleep(w);
				System.out.println("invoke TestThread");
				MasterTableFactory.getFileInstance().refreshDirectoryFiles("test", FileInfo.PATH_SEPARATOR, new Date(), new User("test", "test"));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	class TestThread1 extends Thread{
		int w;
		public TestThread1(int w){
			this.w=w;
		}
		public void run(){
			try {
				if(w>0)Thread.sleep(w);
				System.out.println("invoke TestThread1");
				MasterTableFactory.getFileInstance().getFileInfo("test", FileInfo.PATH_SEPARATOR, new User("test", "test"));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void updateDirectLogonPwd(String pwd) {
		//not implemented
	}

	@Override
	public void driveMounted(IVirtualDisk fs) {
		//not implemented
	}
	
	@Override
	public void proxyConnected(User usr){
		//not implemented
	}

	@Override
	public User getProxyConnectPassword() {
		//not implemented
		return null;
	}
}
