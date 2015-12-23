package sg.edu.nyp.sit.pvfs.virtualdisk.eldos.sample;

import java.nio.ByteBuffer;
import java.util.Date;

import sg.edu.nyp.sit.pvfs.virtualdisk.eldos.FileAttributes;

public class VirtualFile {
	private String name=null;
	private ByteBuffer buffer=null;
	private FileAttributes attrs;
	private Date creationTime=null;
	private Date lastAccessedTime=null;
	private Date lastWriteTime=null;
	private long endOfFile=0L;
	private DiskEnumerationContext context=null;
	
	public VirtualFile(String name){
		this.name=name;
		creationTime=new Date();
		lastAccessedTime=new Date();
		lastWriteTime=new Date();
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setAttrs(FileAttributes attrs) {
		this.attrs = attrs;
	}

	public FileAttributes getAttrs() {
		return attrs;
	}

	public void setCreationTime(Date creationTime) {
		this.creationTime = creationTime;
	}

	public Date getCreationTime() {
		return creationTime;
	}

	public void setLastAccessedTime(Date lastAccessedTime) {
		this.lastAccessedTime = lastAccessedTime;
	}

	public Date getLastAccessedTime() {
		return lastAccessedTime;
	}

	public void setLastWriteTime(Date lastWriteTime) {
		this.lastWriteTime = lastWriteTime;
	}

	public Date getLastWriteTime() {
		return lastWriteTime;
	}

	public void setEndOfFile(long endOfFile) {
		this.endOfFile = endOfFile;
	}

	public long getEndOfFile() {
		return endOfFile;
	}

	public long getAllocationSize() {
		return (buffer==null? 0 : buffer.capacity());
	}

	public void setContext(DiskEnumerationContext context) {
		this.context = context;
	}

	public DiskEnumerationContext getContext() {
		return context;
	}
	
	public void remove(){
		if(context!=null)
			context.removeFile(this);
	}
	
	public int write(byte[] data, long position, int bytesToWrite){
		try{
		if(buffer==null){
			buffer=ByteBuffer.wrap(new byte[(int) (position+bytesToWrite)]);
		}else if(buffer.capacity()<position+bytesToWrite){
			byte[] currArr=buffer.array();
			byte[] newArr=new byte[(int) (position+bytesToWrite)];
			System.arraycopy(currArr, 0, newArr, 0, currArr.length);
			buffer=ByteBuffer.wrap(newArr);
			currArr=null;
		}
		
		endOfFile=position+bytesToWrite;
		
		buffer.position((int) position);
		buffer.put(data, 0, bytesToWrite);
		
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		return bytesToWrite;
	}
	
	public int read(byte[] data, long position, int bytesToRead){
		if(buffer==null) return 0;
		
		if(position>=buffer.capacity())
			return 0;
		
		buffer.position((int) position);
		
		int bytesRead=(int) (buffer.capacity()-position>bytesToRead?bytesToRead:
			buffer.capacity()-position);

		buffer.get(data, 0, bytesRead);
		
		return bytesRead;
	}
}
