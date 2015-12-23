package sg.edu.nyp.sit.svds.client.filestore;

import java.lang.ref.WeakReference;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class FileSliceStoreFactory {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(FileSliceStoreFactory.class);
	
	private static final String AZURE_FS_PROP="client.slicestore.azure";
	private static final String S3_FS_PROP="client.slicestore.s3";
	private static final String RESTLET_FS_PROP="client.slicestore.restlet";
	private static final String SHARED_ACCESS_AZURE_FS_PROP="client.slicestore.sharedaccess.azure";
	private static final String SHARED_ACCESS_S3_FS_PROP="client.slicestore.sharedaccess.s3";
	private static final String SHARED_ACCESS_RESTLET_FS_PROP="client.slicestore.sharedaccess.restlet";
	
	private static HashMap<String, WeakReference<IFileSliceStore>> FS_INSTANCES=new HashMap<String, WeakReference<IFileSliceStore>>();

	public static IFileSliceStore getInstance(String serverId) {
		return (FS_INSTANCES.containsKey(serverId) && FS_INSTANCES.get(serverId).get()!=null ?
				 FS_INSTANCES.get(serverId).get() : createUpdateInstance(serverId));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static IFileSliceStore createUpdateInstance(String serverId){
		Class cls=null;
		
		FileSliceServerInfo fssi=IFileSliceStore.getServerMapping(serverId);
		if(fssi==null)
			return null;
		
		try{
			switch(fssi.getType()){
				case AZURE:
					cls = Class.forName(ClientProperties.getString(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS) ? 
							SHARED_ACCESS_AZURE_FS_PROP : AZURE_FS_PROP));
					break;
				case RESTLET:
					cls = Class.forName(ClientProperties.getString(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS) ? 
							SHARED_ACCESS_RESTLET_FS_PROP : RESTLET_FS_PROP));
					break;
				case S3:
					cls = Class.forName(ClientProperties.getString(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS) ? 
							SHARED_ACCESS_S3_FS_PROP : S3_FS_PROP));
					break;
				default:
					return null;
			}
			
			IFileSliceStore fss= (IFileSliceStore)cls.getConstructor(new Class[] {String.class})
				.newInstance(new Object[]{fssi.getServerId()});
			
			FS_INSTANCES.put(serverId, new WeakReference(fss));
			
			return fss;
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			return null;
		}
	}
	
	static void removeInstance(String serverId){
		FS_INSTANCES.remove(serverId);
	}
}
