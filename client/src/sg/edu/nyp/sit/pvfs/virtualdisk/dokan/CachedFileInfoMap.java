package sg.edu.nyp.sit.pvfs.virtualdisk.dokan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.WinError;

public class CachedFileInfoMap extends ConcurrentHashMap<String, VirtualFileInfo>  {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(CachedFileInfoMap.class);
	
	//Only store files that have complete info
	
	public VirtualFileInfo create(String namespace, String path, boolean isDirectory,
			User usr) 
		throws DokanOperationException{
		VirtualFileInfo f=get(path);
		
		if(f==null){
			f=VirtualFileInfo.create(namespace, 
					path.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR), 
					isDirectory, usr);
			
			this.putIfAbsent(path, f);
		}
		
		return f;
	}
	
	public VirtualFileInfo getFileInfo(String namespace, String path, User usr)
		throws DokanOperationException{
		String SVDSPath=path.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR);
		
		try{
			LOG.debug("get file info:" + SVDSPath);
			
			File tmp=new File(namespace+SVDSPath, usr);
			if(!tmp.exist()) {
				LOG.debug("no file");
				return null;
			}
			
			LOG.debug("file exist");
			
			VirtualFileInfo fi=new VirtualFileInfo(tmp);
			
			this.put(path, fi);
			
			return fi;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else
				throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}
	
	public void updatePaths(String existingPath, String newPath){
		VirtualFileInfo existing=this.get(existingPath);
		
		this.remove(existingPath);
		
		VirtualFileInfo f;
		String path;
		for(Map.Entry<String, VirtualFileInfo> k: this.entrySet()){
			if(!k.getKey().startsWith(existingPath))
				continue;
			
			f=k.getValue();
			path=k.getKey();
			
			this.remove(path);
			
			f.changeDirectory(existingPath.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR), 
					newPath.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR));
			
			this.put(newPath+path.substring(existingPath.length()), f);
		}
		
		this.put(newPath, existing);
	}
		
}
