package sg.edu.nyp.sit.pvfs.svc;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.pvfs.virtualdisk.VirtualDiskFactory;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.metadata.User;

public class ProxyConnected extends Thread {
	public static final long serialVersionUID = 1L;
	
	private final char mountLetter;
	private final User usr;
	
	public ProxyConnected(User usr)throws InstantiationException {
		if(Main.isDriveMounted()){
			throw new InstantiationException ("Drive is already mounted!");
		}
		
		this.usr=usr;
		this.mountLetter=Main.getAvailableDriveLetter();
	}

	public void run(){
		try{ 
			MasterTableFactory.initProxyFileInstance();
			MasterTableFactory.initProxyNamespaceInstance();
			
			IVirtualDisk fs=VirtualDiskFactory.getInstance(usr);
			if(fs==null) throw new NullPointerException("Unable to get instance of virtual disk.");
			
			Main.setUser(usr);
			Main.driveMounted(fs);	
			Main.showTrayMsg("Drive has been mounted successfully.");
			
			//mount the drive the last because it seems to "take the thread" (for dokan)
			fs.mount(mountLetter);
		}catch(Exception ex){
			ex.printStackTrace();
			Main.showTrayError("Error occurred with mounted drive.");
		}
	}
}
