package sg.edu.nyp.sit.svds.client.master;

import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

import java.util.Date;
import java.util.List;

public interface IMasterFileTable {
	public static final long serialVersionUID = 2L;
	
	public void addFileInfo(FileInfo rec, User usr) throws SVDSException;
	public void updateFileInfo(FileInfo rec, User usr) throws SVDSException;
	public void moveFileInfo(FileInfo rec, String new_namespace, String new_path, 
			User usr) throws SVDSException;
	public void deleteFileInfo(FileInfo rec, User usr) throws SVDSException;
	public void accessFile(FileInfo rec, User usr) throws SVDSException;
	public FileInfo getFileInfo(String namespace, String filename, 
			User usr) throws SVDSException;

	public void lockFileInfo(FileInfo rec, User usr) throws SVDSException;
	public void unlockFileInfo(FileInfo rec, User usr) throws SVDSException;
	
	public void changeFileMode(FileInfo rec, FileIOMode mode, User usr) 
		throws SVDSException;
	public void refreshChangeFileMode(FileInfo rec, User usr) throws SVDSException;

	public List<String> listFiles(String namespace, String directoryPath, User usr) 
		throws SVDSException;
	public List<FileInfo> refreshDirectoryFiles(String namespace, String path, 
			Date lastChkDate, User usr) throws SVDSException;
	
	public List<FileSliceInfo> generateFileSliceInfo(String namespace, int numReq, 
			FileIOMode pref, User usr) throws SVDSException;
}
