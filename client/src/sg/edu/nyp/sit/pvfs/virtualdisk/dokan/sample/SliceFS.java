
package sg.edu.nyp.sit.pvfs.virtualdisk.dokan.sample;

import static net.decasdev.dokan.CreationDisposition.CREATE_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.CREATE_NEW;
import static net.decasdev.dokan.CreationDisposition.OPEN_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.OPEN_EXISTING;
import static net.decasdev.dokan.CreationDisposition.TRUNCATE_EXISTING;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;
import static net.decasdev.dokan.WinError.ERROR_ALREADY_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_DIRECTORY;
import static net.decasdev.dokan.WinError.ERROR_FILE_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_PATH_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_READ_FAULT;
import static net.decasdev.dokan.WinError.ERROR_WRITE_FAULT;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanOptions;
import net.decasdev.dokan.DokanVolumeInformation;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;
import sg.edu.nyp.sit.pvfs.virtualdisk.Utils;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class SliceFS implements DokanOperations {
	public static final long serialVersionUID = 2L;
	
	/** fileName -> SliceFileInfo */
	SliceFileInfoMap fileInfoMap = new SliceFileInfoMap();
	
	final static int volumeSerialNumber = 0x19831120;
	static final String NAMESPACE = "urn:sit.nyp.edu.sg";
	
	long nextHandleNo = 0;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;

	//Provides disk space info and volume information when requested by OS
	DokanDiskFreeSpace diskSpace;
	DokanVolumeInformation volInfo;
	
	private User usr=new User("test", "test");

	static void log(String msg) {
		System.out.println("== virtualdisk == " + msg);
	}

	/**
	 * Mounts the drive.
	 * @param args The drive letter is expected as first argument (arg[0]).
	 * If drive letter is missing, it is mounted as drive "T"
	 */
	public static void main(String[] args) throws Exception{
		char driveLetter = (args.length == 0) ? 't' : args[0].charAt(0);
		new SliceFS().mount(driveLetter);
		System.exit(0);
	}

	/**
	 * Default constructor that initializes the disk free space and volume information.
	 */
	public SliceFS() throws Exception{
		IMasterNamespaceTable mt=MasterTableFactory.getNamespaceInstance();
		NamespaceInfo ns=mt.getNamespaceMemory(NAMESPACE, usr);

		//a dummy of total 1GB space, but only 512MB available
		diskSpace=new DokanDiskFreeSpace();
		//diskSpace.freeBytesAvailable=512 * 1024 * 1024;
		//diskSpace.totalNumberOfBytes=1024 * 1024 * 1024;
		//diskSpace.totalNumberOfFreeBytes=512 * 1024 * 1024;
		diskSpace.freeBytesAvailable=ns.getMemoryAvailable();
		diskSpace.totalNumberOfBytes=ns.getMemoryUsed();
		diskSpace.totalNumberOfFreeBytes=ns.getMemoryAvailable();
		
		volInfo=new DokanVolumeInformation();
		volInfo.fileSystemFlags=0;
		volInfo.fileSystemName="SVDSFS";
		volInfo.maximumComponentLength=128; //(no of max chars between \'s)
		//volInfo.volumeName="SVDS";
		volInfo.volumeName=NAMESPACE;
		volInfo.volumeSerialNumber=volumeSerialNumber;
		
		//showVersions();
	}

//	void showVersions() {
//		int version = Dokan.getVersion();
//		System.out.println("version = " + version);
//		int driverVersion = Dokan.getDriverVersion();
//		System.out.println("driverVersion = " + driverVersion);
//	}


	/**
	 * Mounts drive.  First argument is the drive letter.
	 */
	public void mount(char driveLetter) {
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.driveLetter = driveLetter;
		Dokan.mount(dokanOptions, this);
		//log("[MemoryFS] mount at = " + driveLetter);
		//int result = Dokan.mount(dokanOptions, this);
		//log("[MemoryFS] result = " + result);
	}

	/**
	 * Retrieves the next handle.  File handles are needed when opening and closing files.
	 * @return
	 */
	synchronized long getNextHandle() {
		return nextHandleNo++;
	}

	/**
	 * Creates a file.  Note that this method maybe called multiple times even when the file has
	 * already been created previously.
	 */
	public long onCreateFile(String fileName, int desiredAccess, int shareMode, int creationDisposition,
			int flagsAndAttributes, DokanFileInfo arg5) throws DokanOperationException {
		
		log("[onCreateFile] " + fileName);

		//Asked to create the root
		if (fileName.equals("\\")) {
			switch (creationDisposition) {
			case CREATE_NEW:
			case CREATE_ALWAYS:
				throw new DokanOperationException(ERROR_ALREADY_EXISTS);
			case OPEN_ALWAYS:
			case OPEN_EXISTING:
			case TRUNCATE_EXISTING:
				long handle = getNextHandle();
				//log("onCreateFile: handle = " + handle);
				return handle;
			}
		//Asked to create a pre-exisitng file
		} else if (fileInfoMap.containsKey(fileName)) {
			switch (creationDisposition) {
			case CREATE_NEW:
				throw new DokanOperationException(ERROR_ALREADY_EXISTS);
			case OPEN_ALWAYS:
			case OPEN_EXISTING:
				long handle = getNextHandle();
				//log("onCreateFile: handle = " + handle);
				return handle;
			case CREATE_ALWAYS:
			case TRUNCATE_EXISTING:
				fileInfoMap.get(fileName).clearData();
				updateParentLastWrite(fileName);
				return getNextHandle();
			}
		//Asked to create a new file
		} else {
			switch (creationDisposition) {
			case CREATE_NEW:
			case CREATE_ALWAYS:
			case OPEN_ALWAYS:
				SliceFileInfo fi = fileInfoMap.get(fileName);
				if(fi == null) {
					fi = fileInfoMap.create(fileName, false);
				}
				return getNextHandle();
			case OPEN_EXISTING:
			case TRUNCATE_EXISTING:
				throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
			}
		}
		throw new DokanOperationException(1);
	}

	/**
	 * Handler for handling a Open Directory event.
	 */
	public long onOpenDirectory(String pathName, DokanFileInfo arg1) throws DokanOperationException {
		//log("[onOpenDirectory] " + pathName);

		if (pathName.equals("\\")) {
			long handle = getNextHandle();
			//log("onOpenDirectory: handle = " + handle);
			return handle;
		}
		pathName = Utils.trimTailBackSlash(pathName);
		if (fileInfoMap.containsKey(pathName)) {
			long handle = getNextHandle();
			//log("onOpenDirectory: handle = " + handle);
			return handle;
		}
		else
			throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
	}

	/**
	 * Creates a directory based on the filename passed in the first parameter.
	 */
	public void onCreateDirectory(String fileName, DokanFileInfo file) throws DokanOperationException {
		log("[onCreateDirectory] " + fileName);

		fileName = Utils.trimTailBackSlash(fileName);
		if (fileInfoMap.containsKey(fileName) || fileName.length() == 0)
			throw new DokanOperationException(ERROR_ALREADY_EXISTS);
		fileInfoMap.create(fileName, true);
		updateParentLastWrite(fileName);
	}

	public void onCleanup(String arg0, DokanFileInfo arg2) throws DokanOperationException {
	}

	public void onCloseFile(String arg0, DokanFileInfo arg1) throws DokanOperationException {
		
	}

	/**
	 * Reads content of a file
	 */
	public int onReadFile(String fileName, ByteBuffer buffer, long offset, DokanFileInfo arg3)
			throws DokanOperationException {
		log("[onReadFile] " + fileName);
		
		//Retrieve the SliceFileInfo object based on the filename to be read
		//If cannot be found => not on our list yet.
		SliceFileInfo fi = fileInfoMap.get(fileName);
		if (fi == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		//If the size of the file is 0, we return 0
		if (fi.getFileSize() == 0)
			return 0;
		
		//Read the content
		try {
			long size = fi.readFromFile(offset, buffer);
			return (int)size;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DokanOperationException(ERROR_READ_FAULT);
		}
	}

	/**
	 * Writes contents to the file
	 */
	public int onWriteFile(String fileName, ByteBuffer buffer, long offset, DokanFileInfo arg3)
			throws DokanOperationException {
		
		SliceFileInfo fi = fileInfoMap.get(fileName);
		if (fi == null) {
			log("[onWriteFile] file not found");
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		}
		
		try {
			return fi.writeToFile(buffer, false, offset);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	public void onSetEndOfFile(String fileName, long length, DokanFileInfo arg2)
			throws DokanOperationException {
		log("[onSetEndOfFile] " + fileName);

//		SliceFileInfo fi = fileInfoMap.get(fileName);
//		if (fi == null)
//			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
//		if (fi.getFileSize() == length)
//			return;
//		if (fi.getFileSize() < length) {
//			byte[] tmp = new byte[(int) length - fi.getFileSize()];
//			fi.m_Cache.add(tmp);
//		} else {
//			fi.m_Cache.remove((int) length, fi.getFileSize() - (int) length);
//		}

	}

	public void onFlushFileBuffers(String arg0, DokanFileInfo arg1) throws DokanOperationException {
	}

	public ByHandleFileInformation onGetFileInformation(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		//log("[onGetFileInformation] " + fileName);
		if (fileName.equals("\\")) {
			return new ByHandleFileInformation(FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_DIRECTORY,
					rootCreateTime, rootCreateTime, rootLastWrite, volumeSerialNumber, 0, 1, 1);
		}
		SliceFileInfo fi = fileInfoMap.get(fileName);
		if (fi == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		return fi.toByHandleFileInformation();

	}

	public synchronized Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		
		pathName = pathName.replace("\\", FileInfo.PATH_SEPARATOR);
		
		log("[onFindFiles] " + pathName);
		SliceFileInfoMap sfim = SliceFileInfo.updateList(pathName);
		log("[onFindFiles] sfim :" + sfim);
		if(sfim != null) {
			fileInfoMap = sfim;
		}
		
		log("[onFindFiles] sfim size:" + sfim.size());

		Collection<SliceFileInfo> fis = fileInfoMap.values();
		ArrayList<Win32FindData> files = new ArrayList<Win32FindData>();

		int count = 0;
		log("[onFindFiles] for loop");
		for (SliceFileInfo fi : fis) {
			files.add(fi.toWin32FindData());
			count++;
		}
		log("[onFindFiles] added:" + count);
		return files.toArray(new Win32FindData[0]);
	}

	public Win32FindData[] onFindFilesWithPattern(String arg0, String arg1, DokanFileInfo arg2)
			throws DokanOperationException {
		return null;
	}

	public void onSetFileAttributes(String fileName, int fileAttributes, DokanFileInfo arg2)
			throws DokanOperationException {
		log("[onSetFileAttributes] " + fileName);

		SliceFileInfo fi = fileInfoMap.get(fileName);
		if (fi == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		fi.fileAttribute = fileAttributes;

	}

	public void onSetFileTime(String fileName, long creationTime, long lastAccessTime,
			long lastWriteTime, DokanFileInfo arg4) throws DokanOperationException {
		log("[onSetFileTime] " + fileName);

		SliceFileInfo fi = fileInfoMap.get(fileName);
		if (fi == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		fi.creationTime = creationTime;
		fi.lastAccessTime = lastAccessTime;
		fi.lastWriteTime = lastWriteTime;

	}

	public void onDeleteFile(String fileName, DokanFileInfo arg1) throws DokanOperationException {
		log("[onDeleteFile] " + fileName);

		SliceFileInfo removed = fileInfoMap.remove(fileName);
		if (removed == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		
		removed.delete();
		//updateParentLastWrite(fileName);

	}

	public void onDeleteDirectory(String fileName, DokanFileInfo arg1) throws DokanOperationException {
		log("[onDeleteDirectory] " + fileName);

		SliceFileInfo removed = fileInfoMap.remove(fileName);
		if (removed == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		//updateParentLastWrite(fileName);

	}

	public void onMoveFile(String existingFileName, String newFileName, boolean replaceExisiting,
			DokanFileInfo arg3) throws DokanOperationException {
		//System.out.println("==> [onMoveFile] " + existingFileName + " -> " + newFileName + ", replaceExisiting = " + replaceExisiting);

		SliceFileInfo existing = fileInfoMap.get(existingFileName);
		if (existing == null)
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		// TODO Fix this
		if (existing.isDirectory)
			throw new DokanOperationException(ERROR_DIRECTORY);
		SliceFileInfo newFile = fileInfoMap.get(newFileName);
		if (newFile != null && !replaceExisiting)
			throw new DokanOperationException(ERROR_FILE_EXISTS);
		fileInfoMap.remove(existingFileName);
		existing.fileName = newFileName;
		fileInfoMap.put(newFileName, existing);
		updateParentLastWrite(existingFileName);
		updateParentLastWrite(newFileName);

		log("<== [onMoveFile]");
	}

	public void onLockFile(String fileName, long arg1, long arg2, DokanFileInfo arg3)
			throws DokanOperationException {
		log("[onLockFile] " + fileName);
	}

	public void onUnlockFile(String fileName, long arg1, long arg2, DokanFileInfo arg3)
			throws DokanOperationException {
		log("[onUnlockFile] " + fileName);
	}

	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0) throws DokanOperationException {
		//must return some info or else will encounter error!
		return diskSpace;
	}

	public DokanVolumeInformation onGetVolumeInformation(String arg0, DokanFileInfo arg1)
			throws DokanOperationException {
		//must return some info or else will encounter error!
		return volInfo;
	}

	public void onUnmount(DokanFileInfo arg0) throws DokanOperationException {
		log("[onUnmount]");
	}

	void updateParentLastWrite(String fileName) {
		if (fileName.length() <= 1)
			return;
		String parent = new File(fileName).getParent();
		log("[updateParentLastWrite] parent = " + parent);
		if (parent == "\\") {
			rootLastWrite = FileTimeUtils.toFileTime(new Date());
		} else {
			SliceFileInfo fi = fileInfoMap.get(parent);
			if (fi == null)
				return;
			fi.lastWriteTime = FileTimeUtils.toFileTime(new Date());
		}
	}
}
