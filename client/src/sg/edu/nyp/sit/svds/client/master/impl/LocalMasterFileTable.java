package sg.edu.nyp.sit.svds.client.master.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.exception.NotFoundSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class LocalMasterFileTable implements IMasterFileTable {
	public static final long serialVersionUID = 2L;
	
	private static Map<String, SortedMap<String, FileInfo>> lst_fileNamespaces=
		Collections.synchronizedMap(new HashMap<String, SortedMap<String, FileInfo>>());
	
	private String serverId="1";
	private String serverHost="D:/Projects/CloudComputing/Development/FileStore";
	private String serverKey="abc";
	
	public LocalMasterFileTable(){
		IFileSliceStore.updateServerMapping(serverId, serverHost, FileSliceServerInfo.Type.RESTLET, 
				FileIOMode.STREAM, null, serverKey, null);
	}
	
	@Override
	public void addFileInfo(FileInfo rec, User usr) throws SVDSException {
		boolean frmNewNamespace=false;
		if(!lst_fileNamespaces.containsKey(rec.getNamespace())){
			lst_fileNamespaces.put(rec.getNamespace(), Collections.synchronizedSortedMap(new TreeMap<String, FileInfo>()));
			frmNewNamespace=true;
		}else if(lst_fileNamespaces.get(rec.getNamespace()).containsKey(rec.getFullPath())){	
			//check for duplicates
			throw new IllegalArgumentException("Duplicated file name.");
		}
		
		try{		
			rec.setCreationDate(new Date());
			//update the file list in memory
			SortedMap<String, FileInfo>files = lst_fileNamespaces.get(rec.getNamespace());
			files.put(rec.getFullPath(), rec);
			lst_fileNamespaces.put(rec.getNamespace(), files);
		}catch(Exception ex){
			if(frmNewNamespace)
				lst_fileNamespaces.remove(rec.getNamespace());
			throw new SVDSException(ex.getMessage());
		}
	}

	@Override
	public void deleteFileInfo(FileInfo rec, User usr) throws SVDSException {
		SortedMap<String, FileInfo>files = lst_fileNamespaces.get(rec.getNamespace());
		if (files==null){
			//throw new NoSuchFieldException("File is not found.");
			return;
		}
			
		files.remove(rec.getFullPath());
		lst_fileNamespaces.put(rec.getNamespace(), files);
	}

	@Override
	public List<FileSliceInfo> generateFileSliceInfo(String namespace,
			int numReq, FileIOMode pref, User usr) throws SVDSException {
		List<FileSliceInfo> slices=new ArrayList<FileSliceInfo>();
		FileSliceInfo fsi;
	
		for(int i=0; i<numReq; i++){
			fsi=new FileSliceInfo(UUID.randomUUID().toString(), serverId);
			
			slices.add(fsi);
		}
		
		return slices;	
	}

	@Override
	public FileInfo getFileInfo(String namespace, String filename, User usr)
			throws SVDSException {
		SortedMap<String, FileInfo>files = lst_fileNamespaces.get(namespace);
		if (files==null){
			return null;
		}
		
		FileInfo fi = files.get(filename);
		if(fi==null){
			return null;
		}
		
		FileInfo f=new FileInfo(fi.getFullPath(), fi.getNamespace(), fi.getType());
		f.setCreationDate(fi.getCreationDate());
		f.setFileSize(fi.getFileSize());
		f.setIda(fi.getIda());
		f.setIdaVersion(fi.getIdaVersion());
		f.setLastModifiedDate(fi.getLastModifiedDate());
		f.setBlkSize(fi.getBlkSize());
		f.setKeyHash(fi.getKeyHash());
		f.setOwner(fi.getOwner());
		f.setSlices(fi.getSlices());
		
		return f;
	}
	
	@Override
	public List<String> listFiles(String namespace, String directoryPath, User usr) throws SVDSException{
		List<String> lst=new ArrayList<String>();
		SortedMap<String, FileInfo>files = lst_fileNamespaces.get(namespace);
		if (files==null){
			return lst;
		}
		
		for(FileInfo fi: files.tailMap(directoryPath).values()){
			if(fi.getFullPath().equals(directoryPath))
				continue;
			
			if(fi.getFullPath().indexOf(FileInfo.PATH_SEPARATOR, directoryPath.length())==-1)
				lst.add(fi.getFullPath());
			else
				break;
		}
		
		return lst;
	}

	@Override
	public void updateFileInfo(FileInfo rec, User usr) throws SVDSException {
		rec.setLastModifiedDate(new Date());
		//currently don't support renaming and changing of namespace yet
		SortedMap<String, FileInfo>files = lst_fileNamespaces.get(rec.getNamespace());
		files.put(rec.getFullPath(), rec);
		lst_fileNamespaces.put(rec.getNamespace(), files);
	}
	
	@Override
	public void moveFileInfo(FileInfo rec, String new_namespace, String new_path, User usr) 
		throws SVDSException{
		
		FileInfo fi=lst_fileNamespaces.get(rec.getNamespace()).get(rec.getFullPath());
		if(!rec.getNamespace().equals(new_namespace)){
			lst_fileNamespaces.get(rec.getNamespace()).remove(fi.getFullPath());
		}
		
		fi.setFullPath(new_path);
		lst_fileNamespaces.get(new_namespace).put(new_path, fi);
	}
	
	@Override
	public void lockFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(!lst_fileNamespaces.containsKey(rec.getNamespace()))
			throw new SVDSException("Existing namespace is not found.");
		
		if(!lst_fileNamespaces.get(rec.getNamespace()).containsKey(rec.getFullPath()))
			throw new SVDSException("Existing file is not found.");
		
		SortedMap<String, FileInfo>files=lst_fileNamespaces.get(rec.getNamespace());
		FileInfo fi=files.get(rec.getFullPath());
		
		if(fi.getType()!=FileInfo.Type.FILE)
			throw new SVDSException("Object is not a file.");
		
		synchronized(fi){
			if(fi.getLockBy()==null){
				fi.setLockBy(rec.getLockBy());
			}else if(!fi.getLockBy().getId().equals(rec.getLockBy().getId()))
				throw new SVDSException("File is already locked by another user");
			else
				return;
		}
		
		//increment lock cnt on associated folders
		String parent=fi.getFullPath().substring(0, fi.getFullPath().lastIndexOf(FileInfo.PATH_SEPARATOR));
		
		//if the file resides on the root folder, then no need to lock
		if(parent.length()==0)
			return;
		
		while(parent.length()>0){
			files.get(parent).incrementLock();
			parent=parent.substring(0, parent.lastIndexOf(FileInfo.PATH_SEPARATOR));
		}
	}
	
	@Override
	public void unlockFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(!lst_fileNamespaces.containsKey(rec.getNamespace()))
			throw new SVDSException("Existing namespace is not found.");
		
		if(!lst_fileNamespaces.get(rec.getNamespace()).containsKey(rec.getFullPath()))
			throw new SVDSException("Existing file is not found.");
		
		SortedMap<String, FileInfo>files=lst_fileNamespaces.get(rec.getNamespace());
		FileInfo fi=files.get(rec.getFullPath());
		
		if(fi.getType()!=FileInfo.Type.FILE)
			throw new SVDSException("Object is not a file.");
		
		synchronized(fi){
			if(fi.getLockBy()==null){
				return;
			}else if(!fi.getLockBy().getId().equals(rec.getLockBy().getId()))
				throw new SVDSException("File is not locked by the current user.");
			else{
				fi.setLockBy(null);
			}
		}

		//decrement lock cnt on associated folders
		String parent=fi.getFullPath().substring(0, fi.getFullPath().lastIndexOf(FileInfo.PATH_SEPARATOR));
		
		//if the file resides on the root folder, then no need to lock
		if(parent.length()==0)
			return;
		
		while(parent.length()>0){
			files.get(parent).decrementLock();
			parent=parent.substring(0, parent.lastIndexOf(FileInfo.PATH_SEPARATOR));
		}
	}
	
	@Override
	public List<FileInfo> refreshDirectoryFiles(String namespace, String path, 
			Date lastChkDate, User usr) throws SVDSException{
		return null;
	}
	
	@Override
	public void changeFileMode(FileInfo rec, FileIOMode mode, User usr) throws SVDSException{
		
	}
	
	@Override
	public void refreshChangeFileMode(FileInfo rec, User usr) throws SVDSException{
		
	}
	
	@Override
	public void accessFile(FileInfo rec, User usr) throws SVDSException{
		if(!lst_fileNamespaces.containsKey(rec.getNamespace()))
			throw new NotFoundSVDSException("Existing namespace is not found.");
		
		if(!lst_fileNamespaces.get(rec.getNamespace()).containsKey(rec.getFullPath()))
			throw new NotFoundSVDSException("Existing file is not found.");
		
		if(lst_fileNamespaces.get(rec.getNamespace()).get(rec.getFullPath()).getType()!=FileInfo.Type.FILE)
			throw new SVDSException("Object is not a file.");
		
		FileInfo fi=lst_fileNamespaces.get(rec.getNamespace()).get(rec.getFullPath());
		fi.setLastAccessedDate(new Date());
	}
}
