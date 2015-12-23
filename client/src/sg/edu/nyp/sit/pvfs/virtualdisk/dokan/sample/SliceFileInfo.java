package sg.edu.nyp.sit.pvfs.virtualdisk.dokan.sample;

import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.virtualdisk.Utils;
import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.client.SVDSInputStream;
import sg.edu.nyp.sit.svds.client.SVDSOutputStream;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class SliceFileInfo {
	public static final long serialVersionUID = 1L;
	
	static long nextFileIndex = 2;

	public static final String OWNER_NAME = "owner_vdisk";
	public static final User USER = new User(OWNER_NAME, "");
	//public static final String ROOT_PATH = NAMESPACE + FileInfo.PATH_SEPARATOR + OWNER_NAME;
	private static SliceFileInfoMap c_Map = new SliceFileInfoMap();
	

	String fileName;
	final boolean isDirectory;
	//TByteArrayList m_Cache = null;
	private byte[] m_baCache = null;
	int fileAttribute = FILE_ATTRIBUTE_NORMAL;
	long creationTime = 0;
	long lastAccessTime = 0;
	long lastWriteTime = 0;
	final long fileIndex;
	long m_nFileSize = -1;
	private static Date dateCheck = new Date(0);

	SliceFileInfo(String fileName, boolean isDirectory) {
		
		this.fileName = fileName;
		this.isDirectory = isDirectory;
		fileIndex = getNextFileIndex();
		if (isDirectory)
			fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;
		long fileTime = FileTimeUtils.toFileTime(new Date());
		creationTime = fileTime;
		lastAccessTime = fileTime;
		lastWriteTime = fileTime;
	}
	
	SliceFileInfo(String fileName, boolean isDirectory, long size, long dtCreate,
			long dtLastMod, long dtLastAcc){
		this.fileName = fileName;
		this.isDirectory = isDirectory;
		fileIndex = getNextFileIndex();
		if (isDirectory) fileAttribute |= FILE_ATTRIBUTE_DIRECTORY;
		creationTime = FileTimeUtils.toFileTime(new Date(dtCreate));
		lastAccessTime = FileTimeUtils.toFileTime(new Date(dtLastMod));
		lastWriteTime = FileTimeUtils.toFileTime(new Date(dtLastAcc));
		m_nFileSize=size;
	}

	Win32FindData toWin32FindData() {
		
		//int fileSize = getFileSize();
		String strFileName = FilenameUtils.getName(fileName);
		String strShortName = Utils.toShortName(fileName); 

		Win32FindData w32fd = new Win32FindData(fileAttribute, creationTime, lastAccessTime,
				lastWriteTime, m_nFileSize, 0, 0,
				strFileName, strShortName);
		
		return w32fd;

	}

	ByHandleFileInformation toByHandleFileInformation() {
		return new ByHandleFileInformation(fileAttribute, creationTime,
				lastAccessTime, lastWriteTime, MemoryFS.volumeSerialNumber,
				//getFileSize(), 1, fileIndex);
				m_nFileSize, 1, fileIndex);
	}
	
	long getFileSize()
	{
		return m_nFileSize;
		/*
		//Cache
		if(m_nFileSize != -1 && m_nFileSize != 0) {
			return m_nFileSize;
		}
		
		File file;
		try {
			file = new File(Util.toSvdsName(SliceFS.NAMESPACE, OWNER_NAME, this.fileName), SliceFileInfo.USER);
			m_nFileSize = (int)file.getFileSize();
		} catch (SVDSException e) {
			e.printStackTrace();
			m_nFileSize = -1;
		}
		
		return m_nFileSize;
		*/
	}

	static long getNextFileIndex() {
		return nextFileIndex++;
	}

	/**
	 * Update the list of files by reading from the slice master server
	 * @param path
	 * @return
	 */
	public static synchronized SliceFileInfoMap updateList(String path) {
		System.out.println("Get directory files");
		LogFactory.getLog(SliceFileInfo.class).debug("SliceFileInfoMap::updateList");
		try {
			IMasterFileTable mt = MasterTableFactory.getFileInstance();
			//List<FileInfo> list = mt.refreshDirectoryFiles(NAMESPACE, path, new Date(0));
			List<FileInfo> list = mt.refreshDirectoryFiles(SliceFS.NAMESPACE, path, dateCheck, SliceFileInfo.USER);

			//If list returns null, means no update
			if(list == null) {
				LogFactory.getLog(SliceFileInfo.class).debug("refreshDirectoryFiles returned null");
				return c_Map;
			}
			//LogFactory.getLog(SliceFileInfo.class).debug("list is not null");
			
			//Got updates, so instantiates a new map
			if(c_Map == null)
				c_Map = new SliceFileInfoMap();
			
			SliceFileInfoMap mapTemp = new SliceFileInfoMap();
			String filename;
			SliceFileInfo sfi;
			//LogFactory.getLog(SliceFileInfo.class).debug("SliceFileInfoMap::updateList");
			for(FileInfo fi : list)
			{
				filename = "\\" + fi.getFilename();
				sfi = c_Map.get(filename);
				if(sfi == null)
					sfi = new SliceFileInfo(filename, fi.getType()==FileInfo.Type.DIRECTORY, 
							fi.getFileSize(), fi.getCreationDate().getTime(), 
							fi.getLastModifiedDate().getTime(), fi.getLastAccessedDate().getTime());
				mapTemp.putIfAbsent(filename, sfi);
			}
			c_Map = mapTemp;
			
			return c_Map;
		} catch (SVDSException ex) {
			ex.printStackTrace();
			return new SliceFileInfoMap();
		}
	}

	public void delete() {
		try {
			File f = new File(Util.toSvdsName(SliceFS.NAMESPACE, OWNER_NAME, fileName), SliceFileInfo.USER);
			f.deleteFile();
			clearCache();
		} catch (SVDSException ex) {
			ex.printStackTrace();
		}
	}

	public long readFromFile(long offset, ByteBuffer out)
	{
		//If available in cache read from Cache
		long sizeToRead;
		if(m_baCache != null) {
			sizeToRead = readFromCache(offset, out);
			if(sizeToRead != -1)
				return sizeToRead;
		}
		
		//read from server
		try {
			File file = new File(Util.toSvdsName(SliceFS.NAMESPACE, OWNER_NAME, this.fileName), SliceFileInfo.USER);
			SVDSInputStream in = new SVDSInputStream(file, false);

			sizeToRead = Math.min(out.capacity(), file.getFileSize()-offset);
			if(sizeToRead <= 0)
				return 0;
			
			byte[] ba = new byte[(int)sizeToRead];
			in.seek(offset);
			in.read(ba);
			out.put(ba, 0, (int)sizeToRead);
			in.close();
			
			//Write to cache
			writeToCache(ba, (int)offset);
			
			return sizeToRead;
		} catch (Exception ex) {
			ex.printStackTrace();
			return 0;
		}
	}
	
	/**
	 * Reads data from internal cache
	 * @param offset The offset to start reading the data
	 * @param buffer the buffer for data to be written to (returned)
	 * @return
	 */
	private long readFromCache(long offset, ByteBuffer buffer)
	{
		//If request > what we have in cache, return -1
		int dataSizeRequested = buffer.capacity() + (int)offset;
		if(dataSizeRequested > m_baCache.length)
			return -1;

		try {
			int dataAvailableInCache = m_baCache.length - (int)offset;
			int sizeToCopy = Math.min(buffer.capacity(), dataAvailableInCache);
			//if size to read is nothing (due to not enough capacity or end of data)
			if (sizeToCopy <= 0)
				return 0;
			//write to buffer to be returned
			buffer.put(m_baCache, (int)offset, sizeToCopy);
			return sizeToCopy;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * Write the content to the file
	 * @param buffer
	 * @param streaming
	 * @param offset
	 * @return
	 */
	public int writeToFile(ByteBuffer buffer, boolean streaming, long offset)
	{
		
		System.out.println("[writeToFile] offset:"+offset+", bytes to write: " + buffer.capacity());
		//Write to remote server
		try {
			//Convert to the SVDS Name
			String strSvdsName = Util.toSvdsName(SliceFS.NAMESPACE, OWNER_NAME, this.fileName);

			File fileOut = new File(strSvdsName, SliceFileInfo.USER);
			
			int sizeWritten;
			byte[] ba;
			if(buffer.hasArray() == false) {
				sizeWritten = buffer.capacity();
				ba = new byte[sizeWritten];
				buffer.get(ba);
			}
			else {
				ba = buffer.array();
				sizeWritten = ba.length;
			}
			
			//Write to master
			SVDSOutputStream out = new SVDSOutputStream(fileOut, false);

			//Move output steam to offset
			out.seek(offset);
			out.write(ba);
			out.close();
			
			//Write to cache
			writeToCache(ba, (int)offset);

			return sizeWritten;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			clearCache();
			return 0;
		}
	}
	
	private void clearCache()
	{
		m_baCache = null;
		m_nFileSize = -1;
	}
	
	public void clearData()
	{
		delete();
		createNewFile();
	}

	/**
	 * Write to cache
	 * @param buffer buffer containing data to write to cache
	 * @param capacity data size to write
	 * @param offset Offset of the CACHE to write to
	 * @return Number of bytes written
	 */
	private synchronized int writeToCache(byte[] buffer, int offset)
	{
		int totalsize = buffer.length + offset;
		
		//For empty cache buffer
		if(m_baCache == null) {
			m_baCache = new byte[totalsize];
		}
		else if(m_baCache.length < totalsize) {
			//Cache buffer already contain data
			//Ensure that it is big enough to hold all data
			int targetSize = m_baCache.length*2;
			
			System.out.println("[writeToCache] targetSize:" + targetSize);
			targetSize = totalsize > targetSize ? totalsize : targetSize;
			byte[] baTemp = new byte[targetSize];
			System.out.println("[writeToCache] new Size:" + targetSize);
			System.arraycopy(m_baCache, 0, baTemp, 0, m_baCache.length);
			m_baCache = baTemp;
		}
		
		//public static void arraycopy(Object src, int srcPos, Object dest, int destPos, int length)
		System.arraycopy(buffer, 0, m_baCache, offset, buffer.length);
		return buffer.length;
	}

	public void createNewFile() {

		
		if(this.fileName == null || this.fileName.equals("")) {
			return;
		}
		
		String svdsFileName = Util.toSvdsName(SliceFS.NAMESPACE, OWNER_NAME, this.fileName);
		try {
			File fileNew = new File(svdsFileName, SliceFileInfo.USER);
			fileNew.createNewFile();
			Field f=fileNew.getClass().getDeclaredField("fInfo");
			f.setAccessible(true);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}

	}
	

}
