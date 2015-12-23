package sg.edu.nyp.sit.pvfs.virtualdisk.dokan;

import static net.decasdev.dokan.CreationDisposition.CREATE_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.CREATE_NEW;
import static net.decasdev.dokan.CreationDisposition.OPEN_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.OPEN_EXISTING;
import static net.decasdev.dokan.CreationDisposition.TRUNCATE_EXISTING;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.pvfs.virtualdisk.Utils;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanOptions;
import net.decasdev.dokan.DokanVolumeInformation;
import net.decasdev.dokan.Win32FindData;
import net.decasdev.dokan.WinError;

public class VirtualFS extends IVirtualDisk implements DokanOperations {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(VirtualFS.class);
	
	public final static int volumeSerialNumber = 0x19831116;
	static final String sysName="SVDSFS";
	
	public static final String PATH_SEPARATOR="\\";
	
	private char driveLetter;
	private DokanDiskFreeSpace diskSpace;
	private DokanVolumeInformation volInfo;
	private CachedFileInfoMap fileInfoMap = new CachedFileInfoMap();
	
	private long nextHandleNo = 0;
	
	//workaround: onDeleteFile/directory seems to be invoked twice.
	//To prevent error from showing when the delete method is invoked 2nd time (file not found)
	//have an list to temp store the deleted files/directories.
	//To prevent the list from getting too big (as there are no maintaince on it),
	//limit the size and remove older entries when newer entries need to be added.
	//private static final int deletedFilesMaxEntries=10;
	//private String[] deletedFiles=new String[deletedFilesMaxEntries];
	//private int deletedFilesIndex=0;
	
	//workaround: when file/directory is deleted, the error "XXX is too big to be deleted, must delete perm"
	//seems to be caused by the folder /Recycled not exist.
	//To prevent the error from showing, have the methods check the path if starts with /Recycled then
	//do not throw error.
	//However this would mean the user cannot create a folder named Recycled in the root
	private static final String recycledPath=PATH_SEPARATOR+"Recycled";
	
	private IMasterNamespaceTable mt=null;
	
	public VirtualFS(User usr) throws Exception{
		super(usr);
		mt=MasterTableFactory.getNamespaceInstance();
		
		/*
		IMasterNamespaceTable mt=MasterTableFactory.getNamespaceInstance();
		NamespaceInfo ns=mt.getNamespaceMemory(namespace);
		
		diskSpace=new DokanDiskFreeSpace();
		diskSpace.freeBytesAvailable=ns.getMemoryAvailable();
		diskSpace.totalNumberOfBytes=ns.getMemoryUsed();
		diskSpace.totalNumberOfFreeBytes=ns.getMemoryAvailable();
		*/
		
		diskSpace=new DokanDiskFreeSpace();
		
		volInfo=new DokanVolumeInformation();
		volInfo.fileSystemFlags=0;
		volInfo.fileSystemName=sysName;
		volInfo.maximumComponentLength=128; //(no of max chars between \'s)
		volInfo.volumeName=mt.getNamespace(usr);
		volInfo.volumeSerialNumber=volumeSerialNumber;
		
		LOG.info("Dokan version: " + Dokan.getVersion());
		LOG.info("Dokan driver version: " + Dokan.getDriverVersion());
		
		//mount the drive immediately upon init
		//DokanOptions dokanOptions = new DokanOptions();
		//dokanOptions.driveLetter = driveLetter;
		//Dokan.mount(dokanOptions, this);
	}
	
	public int mount(char driveLetter){
		this.driveLetter=driveLetter;
		
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.driveLetter = driveLetter;
		//dokanOptions.driveLetter = 't';
		
		LOG.info(volInfo.volumeName + " virtual drive mounted at " + driveLetter);
		
		return Dokan.mount(dokanOptions, this);
	}
	
	public char getDriveLetter(){
		return driveLetter;
	}
	
	public int unmount(){
		(new Thread(){
			public void run(){
				Dokan.unmount(driveLetter);
			}
		}).start();
		
		return 1;
	}
	
	private synchronized long getNextHandle() {
		return nextHandleNo++;
	}
	
	/*
	private void addDeletedFile(String path){
		if(deletedFilesIndex>=deletedFilesMaxEntries)
			deletedFilesIndex=0;
		
		deletedFiles[deletedFilesIndex]=path;
		deletedFilesIndex++;
	}
	
	private boolean containsDeletedFile(String path){
		for(String n: deletedFiles){
			if(n==null)
				continue;
			
			if(n.equals(path))
				return true;
		}
		
		return false;
	}
	*/

	@Override
	public void onCleanup(String arg0, DokanFileInfo arg1)
			throws DokanOperationException {
		LOG.debug("[onCleanup] " + arg0);
	}

	@Override
	public void onCloseFile(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		LOG.debug("[onCloseFile] " + path);
		
		VirtualFileInfo f=fileInfoMap.get(path);
		if(f==null || f.isDirectory()) {
			//LOG.debug("is null or is directory.");
			return;
			//throw new DokanOperationException(WinError.ERROR_PATH_NOT_FOUND);
		}
		
		//LOG.debug("going to close stream");
		
		try{ f.closeStreams(); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		}
	}

	@Override
	public void onCreateDirectory(String path, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onCreateDirectory] " + path);
		
		if(path.startsWith(recycledPath)){
			return;
		}

		path = Utils.trimTailBackSlash(path);
		if (fileInfoMap.containsKey(path) || path.length() == 0)
			throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
		
		try{ fileInfoMap.create(volInfo.volumeName, path, true, usr); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
	}

	@Override
	public long onCreateFile(String path, int desiredAccess, int shareMode, 
			int creationDisposition, int flagsAndAttributes, DokanFileInfo dFileInfo) 
		throws DokanOperationException {
		LOG.debug("[onCreateFile] " + path + " creationDisposition: " + creationDisposition);
		
		if(path.startsWith(recycledPath)){
			switch (creationDisposition) {
				case CREATE_NEW:
				case CREATE_ALWAYS:
					throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
				case OPEN_ALWAYS:
				case OPEN_EXISTING:
				case TRUNCATE_EXISTING:
					long handle = getNextHandle();
					//log("onCreateFile: handle = " + handle);
					return handle;
				}
		}
		
		/*
		//Asked to create the root
		if (path.equals(PATH_SEPARATOR)) {
			switch (creationDisposition) {
				case CREATE_NEW:
				case CREATE_ALWAYS:
					throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
				case OPEN_ALWAYS:
				case OPEN_EXISTING:
				case TRUNCATE_EXISTING:
					long handle = getNextHandle();
					//log("onCreateFile: handle = " + handle);
					return handle;
			}
		//Asked to create a pre-exisitng file
		} else
		*/ 
		if (fileInfoMap.containsKey(path)) {
			switch (creationDisposition) {
				case CREATE_NEW:
					throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
				case OPEN_ALWAYS:
				case OPEN_EXISTING:
					LOG.debug("file in cache.");
					long handle = getNextHandle();
					//log("onCreateFile: handle = " + handle);
					return handle;
				case CREATE_ALWAYS:
				case TRUNCATE_EXISTING:
					try{ fileInfoMap.get(path).truncate(); }
					catch(DokanOperationException ex){
						if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
							Main.showTrayError(Main.REMOTE_ENDED_MSG);
							Main.driveUnmounted();
						}
						throw ex;
					}
					return getNextHandle();
			}
		//Asked to create a new file
		} else {
			switch (creationDisposition) {
				case CREATE_NEW:
				case CREATE_ALWAYS:
				case OPEN_ALWAYS:
					if (fileInfoMap.get(path)==null){
						try{ fileInfoMap.create(volInfo.volumeName, path, false, usr); }
						catch(DokanOperationException ex){
							if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
								Main.showTrayError(Main.REMOTE_ENDED_MSG);
								Main.driveUnmounted();
							}
							throw ex;
						}
					}
					return getNextHandle();
				case OPEN_EXISTING:
					LOG.debug("getting file info");
					try { fileInfoMap.getFileInfo(volInfo.volumeName, path, usr); }
					catch(DokanOperationException ex){
						if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
							Main.showTrayError(Main.REMOTE_ENDED_MSG);
							Main.driveUnmounted();
						}
						throw ex;
					}
					//if(fileInfoMap.getFileInfo(namespace, path, usr)==null){
					//	LOG.debug("file not found");
					//	throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
					//}
					return getNextHandle();
				case TRUNCATE_EXISTING:
					System.out.println("file not found");
					throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
			}
		}
		throw new DokanOperationException(1);
	}

	@Override
	public void onDeleteDirectory(String path, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onDeleteDirectory] " + path);

		VirtualFileInfo removed = fileInfoMap.remove(path);
		if (removed == null){
			LOG.error("file not found in cache");
			return;
			//check if found in deleted files
			/*
			if(!containsDeletedFile(path))
				throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
			else
				LOG.debug("found in deleted list");
			*/
		}
		
		try{ removed.delete(); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
		//addDeletedFile(path);
	}

	@Override
	public void onDeleteFile(String path, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onDeleteFile] " + path);

		VirtualFileInfo removed = fileInfoMap.remove(path);
		if (removed == null){
			return;
			//check if found in deleted files
			/*
			if(!containsDeletedFile(path))
				throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
			else
				LOG.debug("found in deleted list");
			*/
		}
		
		try{ removed.delete(); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
		//addDeletedFile(path);
	}

	@Override
	public synchronized Win32FindData[] onFindFiles(String path, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("<==[onFindFiles] " + path);
		
		if(path.startsWith(recycledPath)){
			return new Win32FindData[0];
		}

		VirtualFileInfo f=fileInfoMap.get(path);
		if(f==null){
			LOG.error("file not found in cache");
			throw new DokanOperationException(WinError.ERROR_PATH_NOT_FOUND);
		}
		
		List<VirtualFileInfo> files;
		try { files=f.refreshDirectory(usr); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
		
		Win32FindData[] winFiles=new Win32FindData[files.size()];
		int cnt=0;
		for(VirtualFileInfo fi: files){
			//LOG.debug(fi.getPath());
			
			winFiles[cnt]=fi.toWin32FindData();
			cnt++;
		}
		
		LOG.debug("total: " + cnt);
		LOG.debug("[onFindFiles]==>");
		
		return winFiles;
	}

	@Override
	public Win32FindData[] onFindFilesWithPattern(String arg0, String arg1,
			DokanFileInfo arg2) throws DokanOperationException {
		LOG.debug("[onFindFilesWithPattern] " + arg0);
		return null;
	}

	@Override
	public void onFlushFileBuffers(String arg0, DokanFileInfo arg1)
			throws DokanOperationException {
		LOG.debug("[onFlushFileBuffers] " + arg0);
	}

	@Override
	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0)
			throws DokanOperationException {
		LOG.debug("[onGetDiskFreeSpace]");
		
		//need to refresh the disk space from master table
		try{
			NamespaceInfo ns=mt.getNamespaceMemory(volInfo.volumeName, usr);
			
			diskSpace.freeBytesAvailable=ns.getMemoryAvailable();
			diskSpace.totalNumberOfBytes=ns.getMemoryUsed();
			diskSpace.totalNumberOfFreeBytes=ns.getMemoryAvailable();
			
			LOG.debug("used: " + ns.getMemoryUsed() + ", ava: " + ns.getMemoryAvailable());
			
			return diskSpace;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}

	@Override
	public ByHandleFileInformation onGetFileInformation(String path, DokanFileInfo dFileInfo) 
		throws DokanOperationException {
		LOG.debug("[onGetFileInformation] " + path);
		
		VirtualFileInfo fi = fileInfoMap.get(path);
		if (fi == null){
			LOG.error("file not found in cache");
			//return null;
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		
		try{ fi.refresh(usr); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
		
		return fi.toByHandleFileInformation();
	}

	@Override
	public DokanVolumeInformation onGetVolumeInformation(String arg0,
			DokanFileInfo arg1) throws DokanOperationException {
		try{
			volInfo.volumeName=mt.getNamespace(usr);
			
			return volInfo;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}

			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}

	@Override
	public void onLockFile(String path, long arg1, long arg2, DokanFileInfo arg3)
			throws DokanOperationException {
		LOG.debug("[onLockFile] " + path);
	}

	@Override
	public synchronized void onMoveFile(String existingPath, String newPath, boolean replaceExisiting,
			DokanFileInfo dFileInfo) throws DokanOperationException {
		LOG.debug("==> [onMoveFile] " + existingPath + " -> " + newPath + ", replaceExisiting = " + replaceExisiting);

		//TODO: overwrite existing files 
		
		VirtualFileInfo existing = fileInfoMap.get(existingPath);
		if (existing == null) {
			LOG.error("file not found in cache");
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		
		String SVDSPath=newPath.replace(PATH_SEPARATOR, FileInfo.PATH_SEPARATOR);
		try { existing.move(SVDSPath); }
		catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
		
		fileInfoMap.updatePaths(existingPath, newPath);

		LOG.debug("<== [onMoveFile]");
	}

	@Override
	public long onOpenDirectory(String path, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onOpenDirectory] " + path);
		
		if(path.startsWith(recycledPath)){
			return getNextHandle();
		}
		
		/*
		if (path.equals(PATH_SEPARATOR)) {
			long handle = getNextHandle();
			log("onOpenDirectory: handle = " + handle);
			return handle;
		}
		path = Utils.trimTailBackSlash(path);
		*/
		if (fileInfoMap.containsKey(path)) {
			long handle = getNextHandle();
			LOG.debug("onOpenDirectory: (cached) handle = " + handle);
			return handle;
		/*
		}else if(fileInfoMap.getFileInfo(namespace, path, usr) !=null){
			long handle = getNextHandle();
			log("onOpenDirectory: handle = " + handle);
			return handle;
		*/
		}else{
			LOG.error("file not found in cache");
			throw new DokanOperationException(WinError.ERROR_PATH_NOT_FOUND);
		}
	}

	@Override
	public int onReadFile(String path, ByteBuffer buffer, long offset, 
			DokanFileInfo dFileInfo) throws DokanOperationException {
		LOG.debug("==>[onReadFile] " + path + " offset: " + offset);

		if(path.startsWith(recycledPath)){
			return 0;
		}
		
		VirtualFileInfo fi = fileInfoMap.get(path);
		if (fi == null) {
			LOG.error("file not found in cache");
			//throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
			return 0;
		}
		
		if (fi.getFileSize() == 0) return 0;
		
		try {
			long size=fi.read(buffer, offset);
			LOG.debug("<==[onReadFile]");
			
			return (int) size;
		}catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_READ_FAULT);
		}
	}

	@Override
	public void onSetEndOfFile(String path, long length, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onSetEndOfFile] " + path);
		
		if(path.startsWith(recycledPath)){
			return;
		}
		
		VirtualFileInfo fi = fileInfoMap.get(path);
		if (fi == null){
			LOG.error("file not found in cache");
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		
		try {
			fi.closeStreams();
		}catch(DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}

	@Override
	public void onSetFileAttributes(String path, int fileAttributes, DokanFileInfo dFileInfo)
			throws DokanOperationException {
		LOG.debug("[onSetFileAttributes] " + path);
		
		if(path.startsWith(recycledPath)){
			return;
		}
		
		VirtualFileInfo fi = fileInfoMap.get(path);
		if (fi == null) throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		
		fi.setFileAttributes(fileAttributes);
	}

	@Override
	public void onSetFileTime(String path, long creationTime, long lastAccessTime,
			long lastWriteTime, DokanFileInfo dFileInfo) throws DokanOperationException {
		LOG.debug("[onSetFileTime] " + path);
		
	}

	@Override
	public void onUnlockFile(String arg0, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onUnmount(DokanFileInfo dFileInfo) throws DokanOperationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int onWriteFile(String path, ByteBuffer buffer, long offset, 
			DokanFileInfo dFileInfo) throws DokanOperationException {
		LOG.debug("[onWriteFile] " + path + " offset: " + offset);
		
		if(path.startsWith(recycledPath)){
			return (int) (offset+buffer.capacity());
		}
		
		VirtualFileInfo fi = fileInfoMap.get(path);
		if (fi == null)	{
			LOG.error("file not found in cache");
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		
		try {
			return (int) fi.write(buffer, offset);
		}catch (DokanOperationException ex){
			if(ex.errorCode==WinError.ERROR_INVALID_ACCESS){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
			throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
		}
	}

	@Override
	public boolean requireRestart() {
		//return true;
		return false;
	}
}
