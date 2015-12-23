package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import java.util.Date;

import sg.edu.nyp.sit.svds.metadata.FileInfo;

public class VirtualSubFile {
	public static final long serialVersionUID = 1L;
	
	private FileInfo fi=null;
	
	private FileAttributes attrs;
	
	public VirtualSubFile(FileInfo fi){
		this.fi=fi;
		
		if(fi.getType()==FileInfo.Type.DIRECTORY){
			attrs=FileAttributes.DIRECTORY;
		}else{
			attrs=FileAttributes.NORMAL;
		}
	}
	
	public FileAttributes getAttrs() {
		return attrs;
	}
	
	public Date getCreationTime(){
		return fi.getCreationDate();
	}
	
	public Date getLastAccessedTime(){
		return fi.getLastAccessedDate();
	}
	
	public Date getLastModifiedTime(){
		return fi.getLastModifiedDate();
	}
	
	public long getFileSize(){
		return fi.getFileSize();
	}

	public String getName(){
		return fi.getFilename();
	}
}
