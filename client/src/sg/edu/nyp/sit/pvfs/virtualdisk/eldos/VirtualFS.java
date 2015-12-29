package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.User;
import eldos.cbfs.CallbackFileSystem;
import eldos.cbfs.ECBFSError;
import eldos.cbfs.ICbFsDirectoryEnumerationInfo;
import eldos.cbfs.ICbFsEnumerateEvents;
import eldos.cbfs.ICbFsFileEvents;
import eldos.cbfs.ICbFsFileInfo;
import eldos.cbfs.ICbFsHandleInfo;
import eldos.cbfs.ICbFsNamedStreamsEnumerationInfo;
import eldos.cbfs.ICbFsStorageEvents;
import eldos.cbfs.ICbFsVolumeEvents;
import eldos.cbfs.ServiceStatus;
import eldos.cbfs.boolRef;
import eldos.cbfs.byteArrayRef;
import eldos.cbfs.dateRef;
import eldos.cbfs.intRef;
import eldos.cbfs.longRef;
import eldos.cbfs.stringRef;

///ICbFsStorageEvents, ICbFsVolumeEvents, ICbFsEnumerateEvents, ICbFsFileEvents
public class VirtualFS extends IVirtualDisk implements ICbFsEnumerateEvents,
		ICbFsFileEvents, ICbFsStorageEvents, ICbFsVolumeEvents {
	public static final long serialVersionUID = 4L;

	private static final Log LOG = LogFactory.getLog(VirtualFS.class);

	public static int DRIVE_ALREADY_MOUNTED_UNMOUNTED = -1;
	public static int DRIVE_MOUNT_UNMOUNT_OK = 1;
	public static int DRIVE_MOUNT_UNMOUNT_ERR = 0;

	// private static String sRegKey =
	// "8023E4D1F54EDC367D2A4F2C010E93D0052EA2D2DE403987BC119EA30D5B2A102EDBBD047EDC5A90AE73DE9A4F020203B58C1290C7E4F94617B449166F6C65D28F4C05726BC819666BC819662F2C25927B5869366BC819662BA81DCA681A";
	static String sRegKey = "F8CFADF27C7D26EE7FDC31BEC3803522E769B54E17F997353C911E23AE5259DF5D641E0390AAA319129214C1A03CF2F7BC7FB39E8E70CA828A8649165B5851DEE3A0614EF79411BEE70425B2B3F065F2F330614EF79425B2B3F035024300B5A20724F094";
	static String mGuid = "713CC6CE-B3E2-4fd9-838D-E28F558F6866";
	private static final String PRODUCT_NAME = "713CC6CE-B3E2-4fd9-838D-E28F558F6866";
	private char driveLetter;
	private boolean isDriveMounted = false;
	private CallbackFileSystem cbfs = null;

	public VirtualFS() {

	}

	public VirtualFS(User usr) throws ECBFSError {
		super(usr);
		cbfs = new CallbackFileSystem(this);
		cbfs.setRegistrationKey(sRegKey);
		cbfs.initialize(PRODUCT_NAME);
		cbfs.setFileCacheEnabled(false);
		cbfs.setSerializeCallbacks(true);
		// cbfs.setThreadPoolSize(1);
		cbfs.setProcessRestrictionsEnabled(false);

		if (!isDriverInstalled())
			throw new NullPointerException("Driver is not installed.");
	}

	private boolean isDriverInstalled() {
		boolRef installed = new boolRef(false);
		longRef versionHigh = new longRef();
		longRef versionLow = new longRef();
		ServiceStatus status = new ServiceStatus();

		try {
			cbfs.getModuleStatus(mGuid, CallbackFileSystem.CBFS_MODULE_DRIVER,
					installed, versionHigh, versionLow, status);

			return installed.getValue();
		} catch (ECBFSError ex) {
			ex.printStackTrace();
			return false;
		}
	}

	@Override
	public int mount(char driveLetter) {
		if (isDriveMounted)
			return DRIVE_ALREADY_MOUNTED_UNMOUNTED;

		System.out.println("");
		System.out.println("Drive mounted!!");
		System.out.println("");
		
		if (mt == null)
			mt = MasterTableFactory.getNamespaceInstance();

		this.driveLetter = driveLetter;
		/*
		 * cbfs = new CallbackFileSystem(new Handler());
		 * cbfs.initialize(PRODUCT_NAME);
		 * cbfs.setRegistrationKey(DEFAULT_REGISTRATION_KEY);
		 * cbfs.setSerializeCallbacks(true); //cbfs.setThreadPoolSize(1);
		 * cbfs.setProcessRestrictionsEnabled(false); cbfs.createStorage();
		 * //cbfs.disableMetaDataCache(true);
		 * cbfs.addMountingPoint(DEFAULT_MOUNT_POINT); cbfs.mountMedia(0);
		 */
		try {
			cbfs.createStorage();

			cbfs.addMountingPoint(driveLetter + ":");

			cbfs.mountMedia(1000 * 5);

			isDriveMounted = true;

			return DRIVE_MOUNT_UNMOUNT_OK;
		} catch (ECBFSError ex) {
			ex.printStackTrace();
			return DRIVE_MOUNT_UNMOUNT_ERR;
		}
	}

	@Override
	public int unmount() {
		if (!isDriveMounted)
			return DRIVE_ALREADY_MOUNTED_UNMOUNTED;

		System.out.println("");
		System.out.println("Drive unmounted!!");
		System.out.println("");
		
		// have to run the unmount in a thread as the virtual drive may be
		// unmounted
		// in event of error within the callback methods, if run directly, the
		// virtual
		// drive will not unmount properly and may cause the entire app to crash
		(new Thread() {
			public void run() {
				try {
					cbfs.unmountMedia(true);

					// the mounting point must be deleted else when deleteing
					// storage,
					// access denied exception will be thrown.
					cbfs.deleteMountingPoint(0);

					cbfs.deleteStorage(true);

					isDriveMounted = false;
				} catch (ECBFSError ex) {
					ex.printStackTrace();
				}
			}
		}).start();

		return DRIVE_MOUNT_UNMOUNT_OK;

		/*
		 * try{ cbfs.unmountMedia(true);
		 * 
		 * //the mounting point must be deleted else when deleteing storage,
		 * //access denied exception will be thrown.
		 * cbfs.deleteMountingPoint(0);
		 * 
		 * cbfs.deleteStorage(true);
		 * 
		 * isDriveMounted=false;
		 * 
		 * return DRIVE_MOUNT_UNMOUNT_OK; } catch (ECBFSError ex) {
		 * ex.printStackTrace(); return DRIVE_MOUNT_UNMOUNT_ERR; }
		 */
	}

	@Override
	public char getDriveLetter() {
		return driveLetter;
	}

	@Override
	public boolean requireRestart() {
		return false;
	}

	// ---------------------------ELDOS CALLBACKS---------------------------
	public final static int volumeSerialNumber = 0x19831116;
	public static final String PATH_SEPARATOR = "\\";
	private IMasterNamespaceTable mt = null;
	private String volumeLabel = null;

	private CachedFileInfoMap map = null;

	@Override
	public void onGetVolumeID(CallbackFileSystem sender, intRef volumeID)
			throws Exception {
		LOG.debug("==cbfs==[onGetVolumeID]");

		volumeID.setValue(volumeSerialNumber);
	}

	@Override
	public void onGetVolumeLabel(CallbackFileSystem sender,
			stringRef volumeLabel) throws Exception {
		LOG.debug("==cbfs==[onGetVolumeLabel]");

		try {
			this.volumeLabel = mt.getNamespace(usr);
			volumeLabel.setValue(this.volumeLabel);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		}
	}

	@Override
	public void onGetVolumeSize(CallbackFileSystem sender,
			longRef totalAllocationUnits, longRef availableAllocationUnits)
			throws Exception {
		LOG.debug("==cbfs==[onGetVolumeSize]");

		try {
			if (volumeLabel == null)
				volumeLabel = mt.getNamespace(usr);
			NamespaceInfo ns = mt.getNamespaceMemory(volumeLabel, usr);

			totalAllocationUnits.setValue(ns.getTotalMemory()
					/ sender.getSectorSize());
			availableAllocationUnits.setValue(ns.getMemoryAvailable()
					/ sender.getSectorSize());
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		}
	}

	@Override
	public void onSetVolumeLabel(CallbackFileSystem sender, String volumeLabel)
			throws Exception {
		LOG.debug("==cbfs==[onSetVolumeLabel]");

		mt.updateNamespace(volumeLabel, usr);
		this.volumeLabel = volumeLabel;
	}

	@Override
	public void onMount(CallbackFileSystem sender) throws Exception {
		LOG.debug("==cbfs==[onMount]");

		map = new CachedFileInfoMap();
	}

	@Override
	public void onStorageEjected(CallbackFileSystem sender) throws Exception {
		LOG.debug("==cbfs==[onStorageEjected]");
	}

	@Override
	public void onUnmount(CallbackFileSystem sender) throws Exception {
		LOG.debug("==cbfs==[onUnmount]");

		map.clear();
		map = null;
		volumeLabel = null;
	}

	@Override
	public void onCanFileBeDeleted(CallbackFileSystem callbackFileSystem,
			ICbFsFileInfo fileInfo, ICbFsHandleInfo handleInfo,
			boolRef canBeDeleted) throws Exception {
		LOG.debug("==cbfs==[onCanFileBeDeleted] " + fileInfo.getFileName());

		// only don't allow deletion if it's root folder
		canBeDeleted
				.setValue(fileInfo.getFileName().equals(PATH_SEPARATOR) ? false
						: true);
	}

	@Override
	public void onCloseFile(CallbackFileSystem sender, ICbFsFileInfo fileInfo,
			ICbFsHandleInfo handleInfo) throws Exception {
		LOG.debug("==cbfs==[onCloseFile] " + fileInfo.getFileName());

		VirtualFile f = map.get(fileInfo.getFileName());
		if (f != null) {
			try {
				f.close();
			} catch (RejectedSVDSException ex) {
				if (ex.getOrigin() == RejectedSVDSException.PROXY) {
					Main.showTrayError(Main.REMOTE_ENDED_MSG);
					Main.driveUnmounted();
					throw ex;
				}
			}
		}
	}

	@Override
	public void onCreateFile(CallbackFileSystem sender, String fileName,
			long desiredAccess, long fileAttributes, long shareMode,
			ICbFsFileInfo fileInfo, ICbFsHandleInfo handleInfo /*
																 * byteArrayRef
																 * fileHandleContext
																 */)
			throws Exception {
		LOG.debug("==cbfs==[onCreateFile] " + fileName + ", access:"
				+ desiredAccess + ", attr:" + fileAttributes + ", mode:"
				+ shareMode);

		try {
			map.createFile(
					volumeLabel,
					fileName,
					((fileAttributes & FileAttributes.DIRECTORY.value()) != 0 ? true
							: false), usr);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		}
	}

	@Override
	public void onDeleteFile(CallbackFileSystem sender, ICbFsFileInfo fileInfo)
			throws Exception {
		LOG.debug("==cbfs==[onDeleteFile] " + fileInfo.getFileName());

		try {
			map.deleteFile(fileInfo.getFileName());
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
	}

	@Override
	public void onFlushFile(CallbackFileSystem sender, ICbFsFileInfo fileInfo)
			throws Exception {
		LOG.debug("==cbfs==[onFlushFile] " + fileInfo.getFileName());
		System.out.println("onFlushFile");
	}

	@Override
	public void onGetFileInfo(CallbackFileSystem sender, String fileName,
			boolRef fileExists, dateRef creationTime, dateRef lastAccessTime,
			dateRef lastWriteTime, longRef endOfFile, longRef allocationSize,
			longRef fileId, longRef fileAttributes, stringRef shortFileName,
			stringRef realFileName) throws Exception {
		LOG.debug("==cbfs==[onGetFileInfo] " + fileName);

		VirtualFile f = null;
		try {
			f = map.getFile(volumeLabel, fileName, usr);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				throw ex;
			}
		}

		if (f == null) {
			LOG.debug("file does not exist");
			fileExists.setValue(false);
			return;
		}

		fileExists.setValue(true);

		creationTime.setValue(f.getCreationTime());
		lastAccessTime.setValue(f.getLastAccessedTime());
		lastWriteTime.setValue(f.getLastModifiedTime());
		endOfFile.setValue(f.getFileSize());
		allocationSize.setValue(f.getFileSize());
		fileAttributes.setValue(f.getAttrs().value());
	}

	/*
	 * @Override public void onGetFileNameByFileId(CallbackFileSystem sender,
	 * long fileId, stringRef filePath, longRef filePathLength) throws Exception
	 * { LOG.debug("==cbfs==[onGetFileNameByFileId] " +fileId);
	 * 
	 * //filePath should be of type stringRef }
	 */

	@Override
	public void onOpenFile(CallbackFileSystem sender, String fileName,
			long desiredAccess, long fileAttributes, long shareMode,
			ICbFsFileInfo fileInfo, ICbFsHandleInfo handleInfo)
			throws Exception {
		LOG.debug("==cbfs==[onOpenFile] " + fileName + ", access:"
				+ desiredAccess + ", mode:" + shareMode);

		// to cater for directories where the information has not been retrieved
		// as get file info method is never called
		if (!map.contains(fileName) && shareMode == 3) {
			try {
				map.getFile(volumeLabel, fileName, usr);
			} catch (RejectedSVDSException ex) {
				if (ex.getOrigin() == RejectedSVDSException.PROXY) {
					Main.showTrayError(Main.REMOTE_ENDED_MSG);
					Main.driveUnmounted();
					throw ex;
				}
			}
		}
	}

	@Override
	public void onReadFile(CallbackFileSystem sender, ICbFsFileInfo fileInfo,
			long position, byteArrayRef buffer, int bytesToRead,
			intRef bytesRead) throws Exception {
		LOG.debug("==cbfs==[onReadFile] " + fileInfo.getFileName() + ", pos:"
				+ position + ", len:" + bytesToRead);

		VirtualFile f = map.get(fileInfo.getFileName());
		if (f == null) {
			LOG.debug("cannot find f of :" + fileInfo.getFileName() + " in map");
			bytesRead.setValue(0);
			return;
		}

		if (buffer.getValue() == null || buffer.getValue().length == 0) {
			buffer.setValue(new byte[bytesToRead]);
		}
		sender.resetTimeout(0);
		try {
			bytesRead
					.setValue(f.read(buffer.getValue(), position, bytesToRead));
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
	}

	@Override
	public void onRenameOrMove(CallbackFileSystem sender,
			ICbFsFileInfo fileInfo, String newFileName) throws Exception {
		LOG.debug("==cbfs==[onRenameOrMove] " + fileInfo.getFileName() + " to "
				+ newFileName);

		try {
			map.moveFile(fileInfo.getFileName(), newFileName);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
	}

	@Override
	public void onSetAllocationSize(CallbackFileSystem sender,
			ICbFsFileInfo fileInfo, long allocationSize) throws Exception {
		LOG.debug("==cbfs==[onSetAllocationSize] " + fileInfo.getFileName()
				+ ", size:" + allocationSize);
	}

	@Override
	public void onSetEndOfFile(CallbackFileSystem sender,
			ICbFsFileInfo fileInfo, long endOfFile) throws Exception {
		LOG.debug("==cbfs==[onSetEndOfFile] " + fileInfo.getFileName()
				+ ", size:" + endOfFile);

		VirtualFile f = map.get(fileInfo.getFileName());
		if (f == null)
			return;
		f.setEndOfFile(endOfFile);
	}

	@Override
	public void onSetFileAttributes(CallbackFileSystem sender,
			ICbFsFileInfo fileInfo, ICbFsHandleInfo handleInfo,
			Date creationTime, Date lastAccessTime, Date lastWriteTime,
			long fileAttributes) throws Exception {
		LOG.debug("==cbfs==[onSetFileAttributes] " + fileInfo.getFileName());
		// TODO: set file attributes
		// this is probably an indication that a file has been written and the
		// time stamp is updated
		VirtualFile f = map.get(fileInfo.getFileName());
		if (f == null)
			return;
		f.SetFileAttributes(fileInfo, creationTime, lastAccessTime,
				lastWriteTime, fileAttributes);
	}

	@Override
	public void onWriteFile(CallbackFileSystem sender, ICbFsFileInfo fileInfo,
			long position, byteArrayRef buffer, int bytesToWrite,
			intRef bytesWritten) throws Exception {
		LOG.debug("==cbfs==[onWriteFile] " + fileInfo.getFileName() + ", pos:"
				+ position + ", len:" + bytesToWrite);

		VirtualFile f = map.get(fileInfo.getFileName());
		if (f == null) {
			bytesWritten.setValue(0);
			return;
		}
		sender.resetTimeout(0);
		try {
			bytesWritten.setValue(f.write(buffer.getValue(), position,
					bytesToWrite));
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}
	}

	/*
	 * @Override public void onCloseDirectoryEnumeration(CallbackFileSystem
	 * sender, CbFsFileInfo directoryInfo, CbFsDirectoryEnumerationInfo
	 * enumerationInfo ) throws Exception {
	 * LOG.debug("==cbfs==[onCloseEnumeration] " +directoryInfo.getFileName());
	 * 
	 * enumerationInfo.setValue(null); }
	 */

	@Override
	public void onEnumerateDirectory(CallbackFileSystem sender,
			ICbFsFileInfo directoryInfo, ICbFsHandleInfo handleInfo,
			ICbFsDirectoryEnumerationInfo enumerationContext, String mask,
			int index, boolean restart, boolRef fileFound,
			stringRef longFileName, stringRef shortLongFileName,
			dateRef creationTime, dateRef lastAccessTime,
			dateRef lastWriteTime, longRef endOfFile, longRef allocationSize,
			longRef fileId, longRef fileAttributes) throws Exception {
		LOG.debug("==cbfs==[onEnumerateDirectory] "
				+ directoryInfo.getFileName() + ", mask:" + mask + ", restart:"
				+ restart);

		VirtualFile parent = map.get(directoryInfo.getFileName());
		if (parent == null) {
			try {
				parent = map.getFile(volumeLabel, directoryInfo.getFileName(),
						usr);
			} catch (RejectedSVDSException ex) {
				if (ex.getOrigin() == RejectedSVDSException.PROXY) {
					Main.showTrayError(Main.REMOTE_ENDED_MSG);
					Main.driveUnmounted();
					throw ex;
				}
			}
		}

		boolean isExactMatch = !mask.equals("*");
		boolean resetEnumeration = false;
		EnumInfo pInfo = null;
		VirtualSubFile sf = null;

		if ((restart || enumerationContext.getUserContext() == null || ((byte[]) enumerationContext
				.getUserContext()).length == 0) && !isExactMatch) {
			resetEnumeration = true;
		}

		if ((restart && enumerationContext.getUserContext() != null)) {
			enumerationContext.setUserContext(null);
		}

		if (enumerationContext.getUserContext() == null
				|| ((byte[]) enumerationContext.getUserContext()).length == 0) {
			pInfo = new EnumInfo();
		} else {
			byte[] byteArr = (byte[]) enumerationContext.getUserContext();
			pInfo = EnumInfo.toObject(byteArr);
		}

		if (resetEnumeration)
			pInfo.index = 0;
		if (pInfo.index == 0) {
			try {
				parent.refreshDirectoryFiles(usr);
			} catch (RejectedSVDSException ex) {
				if (ex.getOrigin() == RejectedSVDSException.PROXY) {
					Main.showTrayError(Main.REMOTE_ENDED_MSG);
					Main.driveUnmounted();
				}
				throw ex;
			}
		}

		if (!pInfo.isExactMatch) {
			sf = (isExactMatch ? parent.getSubFile(mask) : parent
					.getSubFile(pInfo.index));
		}

		pInfo.index++;
		pInfo.isExactMatch = isExactMatch;
		enumerationContext.setUserContext(pInfo.toBytes());

		if (sf == null) {
			fileFound.setValue(false);
			return;
		}

		fileFound.setValue(true);

		longFileName.setValue(sf.getName());
		creationTime.setValue(sf.getCreationTime());
		lastAccessTime.setValue(sf.getLastAccessedTime());
		lastWriteTime.setValue(sf.getLastModifiedTime());
		endOfFile.setValue(sf.getFileSize());
		allocationSize.setValue(sf.getFileSize());
		fileAttributes.setValue(sf.getAttrs().value());

		LOG.debug("file: " + sf.getName());
	}

	@Override
	public void onIsDirectoryEmpty(CallbackFileSystem callbackFileSystem,
			ICbFsFileInfo fileInfo, String fileName, boolRef isEmpty)
			throws Exception {
		LOG.debug("==cbfs==[onIsDirectoryEmpty] " + fileName);

		VirtualFile f = map.get(fileName);
		if (f == null) {
			isEmpty.setValue(true);
			return;
		}

		try {
			f.refreshDirectoryFiles(usr);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}
			throw ex;
		}

		isEmpty.setValue(!f.hasSubFiles());

		LOG.debug(isEmpty.getValue());
	}

	// ---------------------------ELDOS CALLBACKS---------------------------
	@Override
	public void onCleanupFile(CallbackFileSystem arg0, ICbFsFileInfo arg1,
			ICbFsHandleInfo arg2) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onGetFileNameByFileId(CallbackFileSystem arg0, long arg1,
			String arg2) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSetValidDataLength(CallbackFileSystem arg0,
			ICbFsFileInfo arg1, long arg2) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void onCloseDirectoryEnumeration(CallbackFileSystem sender,
			ICbFsFileInfo fileInfo, ICbFsDirectoryEnumerationInfo dirInfo)
			throws Exception {
		if (dirInfo.getUserContext() == null)
			return;
	}

	@Override
	public void onCloseNamedStreamsEnumeration(CallbackFileSystem arg0,
			ICbFsFileInfo arg1, ICbFsNamedStreamsEnumerationInfo arg2)
			throws Exception {
		// TODO Auto-generated method stub

	}
}
