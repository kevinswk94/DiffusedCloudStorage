package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.jetty.util.log.Log;

import sg.edu.nyp.sit.pvfs.virtualdisk.dokan.VirtualFS;
import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class CachedFileInfoMap extends ConcurrentHashMap<String, VirtualFile> {
	public static final long serialVersionUID = 2L;

	public VirtualFile getFile(String namespace, String path, User usr) throws Exception{
		 String SVDSPath=path.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR);
		 
		try {
			File tmp = new File(namespace+SVDSPath, usr);
			if(!tmp.exist()) return null;
			// check if there is already an entry in the map
			VirtualFile f = this.get(path);
			if (f == null) {
			   Log.debug("virtual file is not in cachedmap, create a new one");
			   f=new VirtualFile(tmp);
			   this.put(path, f);
			}
			return f;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw ex;
			}else return null;
		} catch (SVDSException ex) {
			ex.printStackTrace();
			
			return null;
		}
	 }
	 
	 public VirtualFile createFile(String namespace, String path, boolean isDirectory, User usr) throws Exception{
		 String SVDSPath=path.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR);

		 try {
			File tmp = new File(namespace+SVDSPath, usr);
			if(tmp.exist()) return null;

			if(isDirectory) tmp.createNewDirectory();
			else tmp.createNewFile();

			VirtualFile f=new VirtualFile(tmp);

			this.put(path, f);

			return f;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw ex;
			}else return null;
		}catch (SVDSException ex) {
			ex.printStackTrace();
			return null;
		}
	 }
	 
	 public void moveFile(String existingPath, String newPath) throws Exception{
		 try{
		 VirtualFile existing=this.get(existingPath);
		 if(existing==null) return;
		 
		 String newSVDSPath=newPath.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR);
		 
		existing.move(newSVDSPath);
		 
		 this.remove(existingPath);
			
		 VirtualFile f;
		 String path;
		 for(Map.Entry<String, VirtualFile> k: this.entrySet()){
			 if(!k.getKey().startsWith(existingPath))
				 continue;

			 f=k.getValue();
			 path=k.getKey();

			 this.remove(path);

			 f.changeDirectory(existingPath.replace(VirtualFS.PATH_SEPARATOR, FileInfo.PATH_SEPARATOR), 
					 newSVDSPath);

			 this.put(newPath+path.substring(existingPath.length()), f);
		 }

		 this.put(newPath, existing);
		 }catch(Exception ex){
			 ex.printStackTrace();
		 }
	 }
	 
	 public boolean deleteFile(String path) throws Exception{
		 VirtualFile f=this.get(path);
		 if(f==null) return false;
		 
		 if(!f.delete()) return false;
		 
		 this.remove(path);
		 
		 return true;
	 }
}
