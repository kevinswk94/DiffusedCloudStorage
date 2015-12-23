package sg.edu.nyp.sit.pvfs.virtualdisk;

import sg.edu.nyp.sit.svds.metadata.User;

public abstract class IVirtualDisk {
	public static final long serialVersionUID = 2L;
	public IVirtualDisk(){
		this.usr=usr;
	}
	
	protected User usr;
	public IVirtualDisk(User usr){
		this.usr=usr;
	}
	
	public abstract int mount(char driveLetter);
	public abstract int unmount();
	
	public abstract char getDriveLetter();
	
	public abstract boolean requireRestart();
}
