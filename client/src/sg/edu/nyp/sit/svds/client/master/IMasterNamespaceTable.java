package sg.edu.nyp.sit.svds.client.master;

import java.util.List;

import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public interface IMasterNamespaceTable {
	public static final long serialVersionUID = 4L;
	
	public NamespaceInfo getNamespaceMemory(String namespace, User usr) throws SVDSException;
	public void refreshRestletSliceServerKey(String svrId, User usr) throws SVDSException;
	
	public String[] getSharedAccessURL(String svrId, String sliceName, User usr) throws SVDSException;
	
	public List<FileSliceServerInfo> getAvailableSliceServers(String namespace, User usr) throws SVDSException;
	public void removeSliceServer(String namespace, String svrId, User usr) throws SVDSException;
	public FileSliceServerInfo getSliceServer(String svrId, User usr) throws SVDSException;
	public void updateSliceServer(FileSliceServerInfo fssi, User usr) throws SVDSException;
	
	//applies to PVFS only 
	//namespace is the volume name
	public String getNamespace(User usr) throws SVDSException;
	public void updateNamespace(String namespace, User usr) throws SVDSException;
	
	public long getReconTimeout(User usr) throws SVDSException;
	public void updateReconTimeout(long interval, User usr) throws SVDSException;
	
	public boolean isAuthReq(User usr) throws SVDSException;
	public void updateAuthReq(boolean req, String newPwd, User usr) throws SVDSException;
}	
