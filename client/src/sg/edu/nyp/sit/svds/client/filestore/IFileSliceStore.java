package sg.edu.nyp.sit.svds.client.filestore;

import sg.edu.nyp.sit.svds.exception.*;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class IFileSliceStore {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(IFileSliceStore.class);
	public static final int AZURE_MIN_PG_SIZE=512;
	public static final int AZURE_MAX_PG_SIZE=1024*1024*4;	//4MB
	//although the max size for page blob is 1TB, use the max value of integer (rounded to multiples of 512)
	//because methods return a byte array which its size is limited by the integer size
	public static final int AZURE_PGBLOB_MAX_SIZE=Integer.MAX_VALUE-(Integer.MAX_VALUE%AZURE_MIN_PG_SIZE);
			
	protected static Map<String, FileSliceServerInfo> fileSliceStoreServerMappings
	=Collections.synchronizedMap(new HashMap<String, FileSliceServerInfo>());

	public static void updateServerMapping(String id, String host, 
			FileSliceServerInfo.Type type, FileIOMode mode,
			String keyId, String key, Properties opts){
		FileSliceServerInfo fssi=null;

		if(!fileSliceStoreServerMappings.containsKey(id)){
			fssi=new FileSliceServerInfo(id, host, type, mode);	
		}else{
			fssi=fileSliceStoreServerMappings.get(id);
			fssi.setServerHost(host);
			fssi.setMode(mode);
			
			//if the type of the fs is changed, then update the fs impl as well
			if(type!=fssi.getType())
				FileSliceStoreFactory.createUpdateInstance(id);	
			fssi.setType(type);
		}
		
		LOG.debug("FS " + id + " key updated to " + key);
		
		fssi.setKeyId(keyId);
		fssi.setKey(key);
		
		if(opts!=null)
			fssi.setAllProperties(opts);

		fileSliceStoreServerMappings.put(id, fssi);
		fssi=null;
	}
	
	public static void updateServerKey(String id, String keyId, String key){
		if(!fileSliceStoreServerMappings.containsKey(id))
			return;
		
		LOG.debug("FS " + id + " key updated to " + key);
		
		FileSliceServerInfo fssi = fileSliceStoreServerMappings.get(id);
		fssi.setKeyId(keyId);
		fssi.setKey(key);
	}

	public static FileSliceServerInfo getServerMapping(String id){
		if(!fileSliceStoreServerMappings.containsKey(id))
			return null;
		else
			return fileSliceStoreServerMappings.get(id);
	}
	
	public static void removeServerMapping(String id){
		fileSliceStoreServerMappings.remove(id);
		FileSliceStoreFactory.removeInstance(id);
	}
	
	protected String serverId;
	public IFileSliceStore(String serverId) {
		this.serverId=serverId;
	}
	
	public String getServerId(){
		return serverId;
	}

	public abstract byte[] retrieve(Object sliceName, long offset, int blkSize) throws SVDSException;
	public abstract byte[] retrieve(Object sliceName, int blkSize) throws SVDSException;
	public abstract int retrieve(Object sliceName, long offset, int len, int blkSize, byte[] data, int dataOffset) throws SVDSException;
	
	public abstract void delete(Object sliceName) throws SVDSException;
	
	public abstract void store(byte[] in, Object sliceName, SliceDigestInfo md) throws SVDSException;
	public abstract void store(byte[] in, Object sliceName, long offset, int length, SliceDigestInfo md) throws SVDSException;
	
	//for checksumming & hasing of file slices
	public abstract void storeHashes(List<byte[]> in, Object sliceName) throws SVDSException;
	public abstract List<byte[]> retrieveHashes(Object sliceName) throws SVDSException;
}
