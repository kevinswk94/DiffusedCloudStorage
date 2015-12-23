package sg.edu.nyp.sit.pvfs.virtualdisk.dokan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.virtualdisk.Utils;
import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.client.SVDSInputStream;
import sg.edu.nyp.sit.svds.client.SVDSOutputStream;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.FileAttribute;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;
import net.decasdev.dokan.WinError;

public class VirtualFileInfo {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(VirtualFileInfo.class);
	
	private static long nextFileIndex=0;
	private static boolean isStreaming=false;
	
	private int fileAttribute = FileAttribute.FILE_ATTRIBUTE_NORMAL;
	
	private File f=null;
	
	private SVDSInputStream in=null;
	private long lastReadOffset=0;
	
	private SVDSOutputStream out=null;
	private long lastWriteOffset=0;

	private Date lastChk=null;
	private String virtualFullPath=null;
	private long size=0;
	private long fileIndex=0;
	private boolean isDirectory=false;
	private ByHandleFileInformation hfi=null;

	//sub files only contains brief info
	private List<VirtualFileInfo> subFiles=null;
	@SuppressWarnings("unused")
	private FileInfo fi=null;
	private Win32FindData wfi=null;
	
	public VirtualFileInfo(File f){
		this.f=f;
		this.virtualFullPath=f.getFullPath().replace(FileInfo.PATH_SEPARATOR, 
				VirtualFS.PATH_SEPARATOR);
		this.size=f.getFileSize();
		fileIndex=getNextFileIndex();
		
		if(f.isDirectory()) {
			fileAttribute|=FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
			lastChk=new Date(0);
			isDirectory=true;
		}
		
		long tmp=FileTimeUtils.toFileTime(f.getCreationDate());
		
		hfi=new ByHandleFileInformation(fileAttribute, tmp,
				(f.getLastAccessedDate()==null?tmp:FileTimeUtils.toFileTime(f.getLastAccessedDate())), 
				(f.getLastModifiedDate()==null?tmp:FileTimeUtils.toFileTime(f.getLastModifiedDate())),
				VirtualFS.volumeSerialNumber,
				size, 1, fileIndex);
	}
	
	private VirtualFileInfo(FileInfo fi){
		this.fi=fi;
		this.virtualFullPath=fi.getFullPath().replace(FileInfo.PATH_SEPARATOR, 
				VirtualFS.PATH_SEPARATOR);
		this.size=fi.getFileSize();
		
		if(fi.getType()==FileInfo.Type.DIRECTORY){
			fileAttribute|=FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
			isDirectory=true;
		}

		long tmp=FileTimeUtils.toFileTime(fi.getCreationDate());
		
		wfi=new Win32FindData(fileAttribute, tmp,
				(fi.getLastAccessedDate()==null?tmp:FileTimeUtils.toFileTime(fi.getLastAccessedDate())), 
				(fi.getLastModifiedDate()==null?tmp:FileTimeUtils.toFileTime(fi.getLastModifiedDate())),
				fi.getFileSize(), 0, 0,
				FilenameUtils.getName(virtualFullPath), Utils.toShortName(virtualFullPath));
	}
	
	private static long getNextFileIndex() {
		return nextFileIndex++;
	}
	
	public static VirtualFileInfo create(String namespace, String path, boolean isDirectory,
			User usr)
		throws DokanOperationException{
		
		try{
			File f=new File(namespace+path, usr);
			if(f.exist()) throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
			
			if(isDirectory) f.createNewDirectory();
			else f.createNewFile();
			
			return new VirtualFileInfo(f);
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			}else throw new DokanOperationException(WinError.ERROR_CANNOT_MAKE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_CANNOT_MAKE);
		}
	}
	
	public List<VirtualFileInfo> refreshDirectory(User usr) throws DokanOperationException{
		if(f.isFile()) {
			throw new DokanOperationException(WinError.ERROR_DIRECTORY);
		}
		
		try{
			List<FileInfo> files=(MasterTableFactory.getFileInstance()).refreshDirectoryFiles(
					f.getNamespace(), f.getFullPath(), lastChk, usr);
			if(subFiles==null) subFiles=new ArrayList<VirtualFileInfo>();
			
			//no changes, return 
			if(files==null){
				return subFiles;
			}

			subFiles.clear();
			for(FileInfo fInfo: files){
				subFiles.add(new VirtualFileInfo(fInfo));
			}
			
			return subFiles;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}
	
	public void refresh(User usr) throws DokanOperationException{
		FileInfo fi;
		
		try{
			fi=(MasterTableFactory.getFileInstance()).getFileInfo(this.f.getNamespace(), 
					this.f.getFullPath(), usr);
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else
				throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
		//currently only updates the creation, last modified, last access and size
		FileInfo cfi=f.getFileInfo();
		cfi.setCreationDate(fi.getCreationDate());
		cfi.setLastAccessedDate(fi.getLastAccessedDate());
		cfi.setLastModifiedDate(fi.getLastModifiedDate());
		cfi.setFileSize(fi.getFileSize());
		
		this.size=fi.getFileSize();
		long tmp=FileTimeUtils.toFileTime(fi.getCreationDate());
		hfi.creationTime=tmp;
		hfi.lastWriteTime=(fi.getLastModifiedDate()==null?tmp:FileTimeUtils.toFileTime(fi.getLastModifiedDate()));
		hfi.lastAccessTime=(fi.getLastAccessedDate()==null?tmp:FileTimeUtils.toFileTime(fi.getLastAccessedDate()));
		hfi.fileSize=fi.getFileSize();
	}
	
	public void truncate() throws DokanOperationException{
		boolean isDirectory=f.isDirectory();
		delete();
		
		try{
			if(isDirectory) f.createNewDirectory();
			else {
				//LOG.debug("truncate: " + f.getFullPath());
				f.createNewFile();
			}
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			}else throw new DokanOperationException(WinError.ERROR_CANNOT_MAKE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_CANNOT_MAKE);
		}
	}
	
	public void delete() throws DokanOperationException{
		try{
			if(f.isDirectory()) f.deleteDirectory();
			else f.deleteFile();
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else
				throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}
	
	public long getFileSize(){
		return size;
	}
	
	public String getPath(){
		return virtualFullPath;
	}
	
	public Date getLastChk(){
		return lastChk;
	}
	
	public boolean isDirectory(){
		return isDirectory;
	}
	
	public void setFileAttributes(int attr){
		fileAttribute=attr;
		
		if(hfi!=null) hfi.fileAttributes=attr;
		if(wfi!=null) wfi.fileAttributes=attr;
	}
	
	public long read(ByteBuffer out, long offset) throws DokanOperationException{
		try{
			if(in==null)  in=new SVDSInputStream(f, isStreaming);
			
			if(offset!=lastReadOffset) in.seek(offset);

			long sizeToRead=Math.min(out.capacity(), f.getFileSize()-offset);
			if(sizeToRead<=0) return 0;
			
			byte[] tmp=new byte[(int) sizeToRead];
			
			in.read(tmp);
			out.put(tmp);
			
			lastReadOffset+=sizeToRead;
			
			return sizeToRead;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else
				throw new DokanOperationException(WinError.ERROR_READ_FAULT);
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_READ_FAULT);
		}
	}
	
	public long write(ByteBuffer in, long offset) throws DokanOperationException{
		try{
			if(out==null) out=new SVDSOutputStream(f, isStreaming);
			
			if(offset!=lastWriteOffset) out.seek(offset);
			
			long sizeToWrite=0;
			byte[] tmp;
			if(in.hasArray()){
				tmp=in.array();
				sizeToWrite=tmp.length;
			}else{
				sizeToWrite=in.capacity();
				tmp=new byte[(int) sizeToWrite];
				in.get(tmp);
			}
			
			out.write(tmp);
			
			return sizeToWrite;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY)
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			else throw new DokanOperationException(WinError.ERROR_READ_FAULT);
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_READ_FAULT);
		}
	}
	
	public void move(String newPath) throws DokanOperationException{
		try{
			//(MasterTableFactory.getFileInstance()).moveFileInfo(f.fInfo, f.getNamespace(), 
			//		newPath, usr);
			
			//f.fInfo.setFullPath(newPath);
			f.moveTo(newPath);
			
			//no need to change the sub files path because when the directory is accessed 
			//next time, the sub files will be refreshed
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			}else throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}catch(SVDSException ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}
	
	public void changeDirectory(String oriDirPath, String newDirPath){
		f.getFileInfo().setFullPath(newDirPath+f.getFileInfo().getFullPath().substring(oriDirPath.length()));
	}
	
	public void closeStreams() throws DokanOperationException{
		try{ 
			//LOG.debug("out:" + (out==null? "null":"non null"));
			if(out!=null) {
				out.close();
				//LOG.debug("close stream");
				out=null;
			}
			
			if(in!=null){
				in.close();
				in=null;
			}
		}
		catch(IOException ex){
			if(ex.getMessage().equals(RejectedSVDSException.PROXY+"")){
				throw new DokanOperationException(WinError.ERROR_INVALID_ACCESS);
			}else{
				LOG.error(ex);
				ex.printStackTrace();
			}
		}
	}
	
	public ByHandleFileInformation toByHandleFileInformation() {
		return hfi;
	}
	
	public Win32FindData toWin32FindData(){
		return wfi;
	}
}
