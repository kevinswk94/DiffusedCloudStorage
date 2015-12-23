package sg.edu.nyp.sit.pvfs.virtualdisk;

import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.metadata.User;

public class VirtualDiskFactory {
	public static final long serialVersionUID = 2L;
	
	private static final String VIRTUALDISK_IMPL="client.virtualdisk";
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IVirtualDisk getInstance(User usr){
		try {
			Class cls=Class.forName(ClientProperties.getString(VIRTUALDISK_IMPL));
			
			return (IVirtualDisk)cls.getConstructor(new Class[] {User.class})
				.newInstance(new Object[]{usr});
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
}
