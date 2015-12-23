package sg.edu.nyp.sit.svds.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Meta data for each file, including directory.
 * 
 * Because both file and directory type uses the same class, there are some properties that are exclusive
 * to the individual type. For example, all methods that has to do with slices are exclusively for objects
 * that are of file type.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class FileInfo implements Comparable<FileInfo> {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Names for the file properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum PropName{
		/**
		 * Size of the file
		 */
		SIZE ("size"),
		/**
		 * Owner of the file
		 */
		OWNER ("owner"),
		/**
		 * Type of the file
		 */
		TYPE ("type"),
		/**
		 * File creation date time.
		 */
		CREATION ("creation"),
		/**
		 * File last modified date time.
		 */
		LASTMOD ("lastModified"),
		/**
		 * File last accessed date time.
		 */
		LASTACC ("lastAccessed"),
		/** 
		 * IDA version that the file is using. The IDA information includes the matrix and other related information
		 * necessary to split and combine the sliced data. Does not applies to directories.
		 */
		IDA_VERSION ("idaVersion"),
		/**
		 * Full file path
		 */
		PATH ("fileName"),
		/**
		 * Date time the specified directory was checked for changes. Only applies to directories.
		 */
		DIR_LASTCHECK ("lastCheck"),
		/**
		 * Number of files in the specified directory.
		 */
		COUNT ("fileCount"),
		/**
		 * Block size that is used to compute the block hashes when checksum verification is required.
		 * Does not applies to directories.
		 */
		SLICE_BLKSIZE ("blkSize"),
		/**
		 * Key hash that is used to compute the block hashes when checksum verification is required.
		 * Does not applies to directories.
		 */
		SLICE_KEYHASH ("keyHash"),
		/**
		 * Flag that the file is in change mode.
		 */
		CHGMODE ("fileMode");
		
		private String name;
		PropName(String name){ this.name=name; }
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	/**
	 * Type of the file.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Type{
		DIRECTORY (1),
		FILE (0);
		
		private int type;
		Type(int type){this.type=type;}
		/**
		 * Gets the type of the file as a integer.
		 * @return Integer value representing the type of file.
		 */
		public int value(){return type;}
		/**
		 * Gets the enum value given the integer representation of the type of file.
		 * 
		 * @param type Integer representation of the type of file
		 * @return Enum value or null if it does not correspond to any valid type of file.
		 */
		public static Type valueOf(int type){
			switch(type){
				case 1: return DIRECTORY;
				case 0: return FILE;
				default: return null;
			}
		}
	}
	
	/**
	 * Status code for the file status - LOCKED.
	 */
	public final static int FILE_STATUS_LOCKED = 423;
	/**
	 * Indicator to denote end of file.
	 */
	public final static int EOF=-1;
	/**
	 * Separator used in the file path.
	 */
	public final static String PATH_SEPARATOR="/";

	private List<FileSliceInfo> slices=null;	
	private Date creationDate=null;
	private Date lastModifiedDate=null;
	private Date lastAccessedDate=null;
	// total file size, and in case of directory totalFileSize = 0
	private long fileSize=0;  		
	private User owner=null;  						
	// filename or directory name 
	private String filename=null; 			
	private Type type=null; 				
	// namespace this file is under
	private String namespace=null; 
	
	//for file type locking
	private User lockBy=null;
	private Date lockOn=null;
	//for directory type locking
	private int lockCnt=0;
	
	//directory path+file/directory name (NOT INCLUDING NAMESPACE), starts with /
	private String fullPath=null;
	
	//info for IDA
	private int idaVersion=0;
	private IdaInfo ida=null;
	
	//indicator if the object only contains the IDA info
	public boolean isEmpty=true;
	//used by the master server end in slice recovery to indicate that if there
	//is recovery, it should stop as the file is already deleted
	public boolean isDeleted=false;
	
	private FileIOMode chgMode=FileIOMode.NONE;
	
	//a unique no that determines if the request to the master server is the same
	public String msgSeq="";
	
	private String keyHash=null;
	private int blkSize=0;
	
	/**
	 * Instantiate a file info object using the path and specifying the type of file.
	 *  
	 * @param fullPath Path including the name of the file, must starts with {@link #PATH_SEPARATOR}.
	 * @param namespace Namespace that the file resides.
	 * @param type Type of the file.
	 */
	public FileInfo(String fullPath, String namespace, Type type){
		slices=Collections.synchronizedList(new ArrayList<FileSliceInfo>());
		setFullPath(fullPath);
		this.namespace=namespace;
		this.type=type;
	}
	
	/**
	 * Sets the slices the file is splitted into.
	 * The list is synchornized through the {@link java.util.Collections#synchronizedList(List)} method to 
	 * prevent concurrent problems.
	 * Does not apply to directories.
	 * 
	 * @param slices List of {@link FileSliceInfo} objects. Or set to null to clear the slices.
	 */
	public void setSlices(List<FileSliceInfo> slices) {
		if(slices==null){
			this.slices.clear();
			return;
		}
		
		this.slices = Collections.synchronizedList(slices);
	}
	/**
	 * Gets a synchronized list of file slices.
	 * Does not apply to directories.
	 * 
	 * @return Synchronized list of file slices.
	 */
	public List<FileSliceInfo> getSlices() {
		return this.slices;
	}
	/**
	 * Gets a specified file slice from the list of file slices using the sequence number.
	 * Does not apply to directories.
	 * 
	 * @param seq Sequence number of the specified file slice to retrieve.
	 * @return Specified file slice.
	 */
	public FileSliceInfo getSlice(int seq){
		for(FileSliceInfo fsi: slices)
			if(fsi.getSliceSeq()==seq)
				return fsi;
		
		return null;
	}
	/**
	 * Adds a file slice into the list. If it exist (determine by the slice name property in {@link FileSliceInfo} class}),
	 * then it is updated with the new object.
	 * Does not apply to directories.
	 * 
	 * @param fsi FileSliceInfo object to be added or updated into the list of file slices.
	 */
	public void addSlice(FileSliceInfo fsi){
		int index=-1;
		boolean isFound=false;
		for(FileSliceInfo i: slices){
			index++;
			if(i.getSliceName().equals(fsi.getSliceName())){
				slices.set(index, fsi);
				isFound=true;
				break;
			}
		}
		
		if(!isFound)
			slices.add(fsi);
	}
	/**
	 * Removes a specified file slice from the list.
	 * Does not apply to directories.
	 * 
	 * @param fsi FileSliceInfo object to be removed from the list.
	 */
	public void removeSlice(FileSliceInfo fsi){
		slices.remove(fsi);
	}
	/**
	 * Removes a specified file slice from the list using the sequence number.
	 * Does not apply to directories.
	 * 
	 * @param sliceSeq Sequence number of the file slice to be removed.
	 */
	public void removeSlice(int sliceSeq){
		int index=-1;
		
		synchronized(slices) {
			for(FileSliceInfo i:slices){
				index++;
				if(i.getSliceSeq()==sliceSeq)
					break;
			}
	
			if(index!=-1)
				slices.remove(index);
		}
	}
	
	/**
	 * Sets the size of the file. Does not apply to directories.
	 * Calling this method will also invoke {@link IdaInfo#setDataOffset(long)} method.
	 * 
	 * @param totalFileSize The size of file.
	 */
	public void setFileSize(long totalFileSize) {
		this.fileSize = totalFileSize;
		if(this.ida!=null) this.ida.setDataSize(totalFileSize);
	}
	/**
	 * Gets the size of the file. Does not apply to directories.
	 * 
	 * @return The size of file.
	 */
	public long getFileSize() {
		return this.fileSize;
	}
	
	/**
	 * Sets the owner of the file.
	 * 
	 * @param owner Owner of the file.
	 */
	public void setOwner(User owner) {
		this.owner = owner;
	}
	/**
	 * Gets the owner of the file.
	 * 
	 * @return Owner of the file.
	 */
	public User getOwner() {
		return this.owner;
	}
	
	/**
	 * Sets the full path of the file.
	 * 
	 * @param fullPath The new full path of the file, including the file name. Must starts with {@link #PATH_SEPARATOR}.
	 */
	public void setFullPath(String fullPath) {
		this.fullPath = fullPath;
		//cater for root directory
		if(fullPath.equals(FileInfo.PATH_SEPARATOR))
			this.filename=fullPath;
		else
			this.filename=fullPath.substring(fullPath.lastIndexOf(FileInfo.PATH_SEPARATOR)+1);
	}
	/**
	 * Gets the full path of the file.
	 * 
	 * @return Full path of the file.
	 */
	public String getFullPath() {
		return this.fullPath;
	}
	
	/**
	 * Sets or changes the file name. 
	 * If the object is referring to the root path (where the file name equals to {@link #PATH_SEPARATOR}, 
	 * then nothing is changed.
	 * 
	 * @param filename The new file name.
	 */
	public void setFilename(String filename) {
		//cannot change the name of root directory
		if(this.filename.equals(FileInfo.PATH_SEPARATOR))
			return;
		
		this.filename = filename;
		
		if(this.fullPath.lastIndexOf(FileInfo.PATH_SEPARATOR)>0)
			this.fullPath=this.fullPath.substring(0, this.fullPath.lastIndexOf(FileInfo.PATH_SEPARATOR)+1) 
				+ filename;
		else
			this.fullPath=FileInfo.PATH_SEPARATOR+filename;
	}
	/**
	 * Gets the file name.
	 * 
	 * @return The file name.
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * Sets the type of the file object.
	 * 
	 * @param type Type of the file.
	 */
	public void setType(Type type) {
		this.type = type;
	}
	/**
	 * Gets the type of the file object.
	 * 
	 * @return Type of the file.
	 */
	public Type getType() {
		return this.type;
	}
	
	/**
	 * Sets the namespace that the current file object is residing.
	 * 
	 * @param namespace Namespace of the file.
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	/**
	 * Gets the namespace the current file object is residing.
	 * 
	 * @return Namespace of the file.
	 */
	public String getNamespace() {
		return this.namespace;
	}

	/**
	 * Sets the creation date time of the file.
	 * 
	 * @param creationDate Creation date time of the file.
	 */
	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
	/**
	 * Gets the creation date time of the file.
	 * 
	 * @return Creation date time of the file.
	 */
	public Date getCreationDate() {
		return creationDate;
	}

	/**
	 * Sets the last modified date time of the file.
	 * If it is a directory, the last accessed date will be set too.
	 * 
	 * @param lastModifiedDate Last modified date time of the file.
	 */
	public synchronized void setLastModifiedDate(Date lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
		
		if(this.type!=null && this.type==Type.DIRECTORY)
			this.lastAccessedDate=lastModifiedDate;
	}
	/**
	 * Gets the last modified date time of the file.
	 * 
	 * @return Last modified date time of the file.
	 */
	public synchronized Date getLastModifiedDate() {
		return lastModifiedDate;
	}

	/**
	 * Sets the last accessed date time of the file.
	 * 
	 * @param lastAccessedDate Last accessed date time of the file.
	 */
	public synchronized void setLastAccessedDate(Date lastAccessedDate) {
		this.lastAccessedDate = lastAccessedDate;
	}
	/**
	 * Gets the last accessed date time of the file.
	 * 
	 * @return Last accessed date time of the file.
	 */
	public synchronized Date getLastAccessedDate() {
		return lastAccessedDate;
	}

	/**
	 * Sets the IDA version used by the file for splitting and combining data.
	 * Does not apply to directories.
	 * 
	 * @param version IDA version used by the file.
	 */
	public void setIdaVersion(int version) {
		this.idaVersion = version;
	}
	/**
	 * Gets the IDA version used by the file.
	 * 
	 * @return IDA version used by the file.
	 */
	public int getIdaVersion() {
		return idaVersion;
	}

	/**
	 * Sets the IDA info object used by the file for splitting and combining data.
	 * The IDA version identifies the IDA info object.
	 * 
	 * @param ida IDA info object used by the file.
	 */
	public void setIda(IdaInfo ida) {
		this.ida = ida;
	}
	/**
	 * Gets the IDA info object used by the file.
	 * 
	 * @return IDA info object used by the file.
	 */
	public IdaInfo getIda() {
		return ida;
	}

	/**
	 * Sets the file change mode.
	 * The purpose of the change mode is to move slices that resides in slice stores that does not
	 * support given mode to those that support it.
	 * 
	 * @param mode Mode of the file to change to.
	 */
	public void setChgMode(FileIOMode mode) {
		this.chgMode = mode;
	}
	/**
	 * Gets the file change mode.
	 * 
	 * @return Mode of the file to change to.
	 */
	public FileIOMode getChgMode(){
		return chgMode;
	}
	/**
	 * To determine if the file is in the mist of mode change.
	 * 
	 * @return True of the file is in the mist of mode change or False if it is not.
	 */
	public boolean isChgMode() {
		return !(chgMode==FileIOMode.NONE);
	}

	/**
	 * Sets the key hash used in the computation of block hashes of the file slices.
	 * 
	 * @param keyHash Key hashed used in the computation of block hashes.
	 */
	public void setKeyHash(String keyHash) {
		this.keyHash = keyHash;
	}
	/**
	 * Gets the key hash used in the computation of the block hashes of the file slices.
	 * 
	 * @return Key hash used in the computation of the block hashes.
	 */
	public String getKeyHash() {
		return keyHash;
	}

	/**
	 * Sets the block size (in bytes) used in the computation of block hashes of the file slices.
	 * 
	 * @param blkSize Block size used in the computation of block hashes or 0 if checksum verification is not required for the file slices.
	 */
	public void setBlkSize(int blkSize) {
		this.blkSize = blkSize;
	}
	/**
	 * Gets the block size (in bytes) used in the computation of block hashes of the file slices.
	 * 
	 * @return Block size used in the computation of block hashes or 0 if checksum verification is not required for the file slices.
	 */
	public int getBlkSize() {
		return blkSize;
	}

	/**
	 * Sets the user who is currently locking the file for write related operations.
	 * This means that the file can be locked by the system for operations such as file recovery or file change mode etc.
	 * Lock has not effect on file read related operations.
	 * 
	 * @param lockBy User that is locking the file.
	 */
	public synchronized void setLockBy(User lockBy) {
		this.lockBy = lockBy;
		this.lockOn = new Date();
	}
	/**
	 * Gets the user who is currently locking the file for write related operations.
	 * 
	 * @return User that is locking the file.
	 */
	public synchronized User getLockBy() {
		return lockBy;
	}
	/**
	 * Gets the date time the file is being locked.
	 * 
	 * @return Date time the file is being locked.
	 */
	public synchronized Date getLockOn() {
		return lockOn;
	}
	/**
	 * Sets the date time the file is being locked to the current date time.
	 */
	public synchronized void refreshLock(){
		this.lockOn=new Date();
	}

	/**
	 * Increment the number of file locks held on this directory.
	 * 
	 * The directory must keep track of how many files within itself are opened for write 
	 * such that if a user attempts to delete the directory, an exception will be thrown.  
	 */
	public synchronized void incrementLock() {
		this.lockCnt ++;
	}
	/**
	 * Decrement the number of file locks held on this directory.
	 * 
	 * When a user has finished writting to a file within the directory, the lock are release.
	 * Only when all the file locks of the directory is release, the directory may be moved or deleted.
	 */
	public synchronized void decrementLock() {
		if(this.lockCnt>0)
			this.lockCnt --;
	}
	/**
	 * Gets the number of file locks held on this directory.
	 * 
	 * @return Number of locks
	 */
	public synchronized int getLockCnt() {
		return lockCnt;
	}
	
	/**
	 * Checks if the file requires checksum verification on the file slices.
	 * 
	 * @return True if the file requires checksum verification or False if it does not.
	 */
	public boolean verifyChecksum(){
		//if blkSize is set to zero, means the checksum feature is to be turned off
		return !(blkSize==0);
	}
	
	/**
	 * Overrides method to compare current object with another using full path.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(FileInfo o) {
		return this.fullPath.compareTo(o.getFullPath());

	}
	
	/**
	 *  Overrides method to return the full path of the file object
	 *  
	 *  @see java.lang.Object#toString()
	 */
	@Override
	public String toString(){
		return this.fullPath;
	}
}
