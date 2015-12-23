package sg.edu.nyp.sit.svds.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sg.edu.nyp.sit.svds.FileSliceSegmentsComparator;

/**
 * Meta data for each slice and/or segment information of the file object.
 * 
 * A slice can contains 0 to many segments. Because the same class is used for both slice and segment objects,
 * some properties, constructors or even methods are exclusive to the type of objects.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class FileSliceInfo {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Names for the slice and/or segment properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author victoria
	 * @version %I% %G%
	 */
	public enum PropName{
		/**
		 * Total number of slices
		 */
		COUNT ("sliceCount"),
		/**
		 * Name of the slice/segment
		 */
		NAME ("sliceName"),
		/**
		 * Sequence of the slice within the file object
		 */
		SEQ ("sliceSeq"),
		/**
		 * Aggregated checksum of the slice
		 */
		CHECKSUM ("sliceChecksum"),
		/**
		 * Size of the slice
		 */
		LEN ("sliceLength"),
		/**
		 * ID of the slice store server where the slice/segment is stored
		 */
		SVR ("sliceServerId"),
		/**
		 * Total number of segments in the slice
		 */
		SEG_CNT ("sliceSegCount"),
		/**
		 * The starting offset of the segment within the slice
		 */
		SEG_OFFSET ("sliceSegOffset"),
		/**
		 * Prefix for identifying the slice's segment(s)
		 */
		SEG ("sliceSeg"),
		/**
		 * If the slice is undergoing or require recovery (merging the segments back to the slice)
		 */
		SEG_RECOVERY ("sliceRecovery"),
		/**
		 * Size of the segment
		 */
		SEG_LEN ("sliceSegLength");
		
		private String name;
		PropName(String name){ this.name=name; }
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	//A status code to indicate the checksum calculated from the slice or segment does not match the one that is given
	public final static int SLICE_STATUS_EXPECTATION_FAILED = 417;
	
	private String sliceChecksum=null;
	private String sliceName=null;
	private String serverId=null;
	//used by client end to notify if the server is doing recovery for the current slice
	private boolean sliceRecovery=false;
	
	private int sliceSeq=-1;
	
	//used when a slice is incomplete, has segments
	private long offset=0;
	private long length=0;
	//a object to synchronize the length and offset variable if the setter and getter methods are called by different threads
	private Object lengthLock = new Object();
	private List<FileSliceInfo> segments=null;
	//value in ticks to determine when the segment is created
	private long timestamp=0;
	
	//a flag to determine in file recovery, the segment has been merge back to the slice and should be removed
	public boolean isRemoved=false;
	
	//private List<byte[]> blkHashes=null;
	
	/**
	 * Constructor for the slice object
	 * 
	 * @param sliceName Name of the slice
	 * @param serverId ID of the slice store server that stores the slice
	 */
	public FileSliceInfo(String sliceName, String serverId){
		this.sliceName=sliceName;
		this.serverId=serverId;
	}
	
	/**
	 * Constructor for the slice object
	 * 
	 * @param sliceName Name of the slice
	 * @param serverId ID of the slice store server that stores the slice
	 * @param length Size of slice data in bytes
	 * @param sliceChecksum Aggregated checksum of the slice data 
	 * @param sliceSeq Sequence of the slice within the file object
	 */
	public FileSliceInfo(String sliceName, String serverId, long length, String sliceChecksum, int sliceSeq){
		this.sliceName=sliceName;
		this.serverId=serverId;
		this.sliceChecksum=sliceChecksum;
		this.sliceSeq=sliceSeq;
		this.length=length;
	}

	/**
	 * Constructor for the segment obejct
	 * 
	 * @param sliceName Name of the segment
	 * @param serverId ID of the slice store server that stores the segment
	 * @param offset The starting offset of the segment within the slice
	 * @param length Size of the segment data in bytes
	 * @param timestamp The segment creation date time in ticks
	 */
	public FileSliceInfo(String sliceName, String serverId, long offset, long length, long timestamp){
		this.sliceName=sliceName;
		this.serverId=serverId;
		this.offset = offset;
		this.length=length;
		this.timestamp=timestamp;
	}
	
	/**
	 * Sets the aggregate checksum of the slice
	 * 
	 * @param sliceChecksum Aggregated checksum of the slice data
	 */
	public void setSliceChecksum(String sliceChecksum) {
		this.sliceChecksum = sliceChecksum;
	}
	/**
	 * Gets the aggregate checksum of the slice
	 * 
	 * @return Aggregate checksum of the slice data
	 */
	public String getSliceChecksum() {
		return this.sliceChecksum;
	}
	
	/**
	 * Sets the slice/segment name
	 * 
	 * @param sliceName Name of the slice/segment
	 */
	public void setSliceName(String sliceName) {
		this.sliceName = sliceName;
	}
	/**
	 * Gets the slice/segment name
	 * 
	 * @return Name of the slice/segment
	 */
	public String getSliceName() {
		return this.sliceName;
	}
	
	/**
	 * Sets the ID of the slice store server that stores the slice/segment
	 * 
	 * @param serverId ID of the slice store server that stores the slice/segment
	 */
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	/**
	 * Gets the ID of the slice store server that stores the slice/segment
	 * 
	 * @return ID of the slice store server that stores the slice/segment
	 */
	public String getServerId() {
		return this.serverId;
	}
	
	/**
	 * Sets the sequence of the slice within the file object
	 * 
	 * @param sliceSeq Sequence of the slice within the file object
	 */
	public void setSliceSeq(int sliceSeq) {
		this.sliceSeq = sliceSeq;
	}
	/**
	 * Gets the sequence of the slice within the file object
	 * 
	 * @return Sequence of the slice within the file object
	 */
	public int getSliceSeq() {
		return sliceSeq;
	}

	/**
	 * Sets the starting offset of the segment within the slice
	 * @param offset The starting offset of the segment within the slice
	 */
	public void setOffset(long offset) {
		this.offset = offset;
	}
	/**
	 * Gets the starting offset of the segment within the slice
	 * @return The starting offset of the segment within the slice
	 */
	public long getOffset() {
		return offset;
	}
	
	/**
	 * Sets the size of the slice/segment data
	 *  
	 * @param length Size of the slice/segment data in bytes
	 */
	public void setLength(long length) {
		//only need to sync the length prop, so no need to lock up the entire file slice info
		synchronized(lengthLock){
			this.length = length;
		}
	}
	/**
	 * Increases the size of the slice/segment based on the offset. If the current length is less than the offset and length, then the size will be set to offset plus length.
	 * 
	 * @param len Size of the data to increase
	 * @param offset Offset to start the increment
	 */
	public void incrementLength(long len, long offset){
		synchronized(lengthLock){
			if(this.length<offset+len)
				this.length=offset+len;
		}
	}
	/**
	 * Decreases the size of the slice/segment
	 * 
	 * @param len Size of the data to decrease
	 */
	public  void decrementLength(long len){
		synchronized(lengthLock){
			this.length-=len;
		}
	}
	/**
	 * Gets the size of the slice/segment data
	 * 
	 * @return Size of the slice/segment data in bytes
	 */
	public long getLength() {
		synchronized(lengthLock){
			return length;
		}
	}
	
	/**
	 * Gets the creation date time of the segment in ticks
	 * 
	 * @return The creation date time of the segment in ticks
	 */
	public long getTimestamp(){
		return timestamp;
	}

	/*
	public void setBlkHashes(List<byte[]> blkHashes) {
		this.blkHashes = blkHashes;
	}

	public List<byte[]> getBlkHashes() {
		return blkHashes;
	}
	*/
	
	/**
	 * Gets the last segment within the slice object in the list.
	 * 
	 * @return Last segment object within the slice object in the list or the current slice object is no segments exist
	 */
	public FileSliceInfo getLastSegment(){
		return (segments==null || segments.size()==0 ? this : 
			segments.get(segments.size()-1));
	}
	/**
	 * Adds a new segment object to the current slice object. The segments are sorted according to the creation date time.
	 * 
	 * @param seg The new segment object to add
	 */
	public void addSegment(FileSliceInfo seg){
		if(segments==null)
			segments=Collections.synchronizedList(new ArrayList<FileSliceInfo>());
		
		segments.add(seg);
		
		//if there are more than 1 elements in the list, sort it according to its
		//offset
		if(segments.size()>1)
			Collections.sort(segments, new FileSliceSegmentsComparator());
	}
	/**
	 * Removes a segment object from the current slice object
	 * 
	 * @param seg The segment object to remove
	 */
	public void removeSegment(FileSliceInfo seg){
		if(segments==null)
			return;
		
		segments.remove(seg);
	}
	/**
	 * Removes a segment object from the current slice object
	 * 
	 * @param segName The name of the segment to remove
	 */
	public void removeSegment(String segName){
		if(segments==null)
			return;
		
		FileSliceInfo seg=null;;
		for(FileSliceInfo s: segments){
			if(s.getSliceName().equals(segName)){
				seg=s;
				break;
			}
		}
		if(seg!=null)
			segments.remove(seg);
	}
	/**
	 * Checks if the current slice object contains the segment
	 * 
	 * @param segName Name of the segment object to search
	 * @return True if the segment with the given name is found or false if none is found or the slice object does not contains any segments
	 */
	public boolean containsSegment(String segName){
		if(segments==null)
			return false;
		
		for(FileSliceInfo s: segments){
			if(s.getSliceName().equals(segName))
				return true;
		}
		
		return false;
	}
	/**
	 * Gets the list of all segments in the current slice object
	 * 
	 * @return A list of all segments in the current slice object or null if none exist
	 */
	public List<FileSliceInfo> getSegments(){
		if(segments==null)
			return null;
		
		return segments;
	}
	/**
	 * Removes all existing segments object from the current slice object
	 */
	public void clearSegments(){
		if(segments==null)
			return;
		
		segments.clear();
		segments=null;
	}
	/**
	 * Checks if the current slice object has segments
	 * 
	 * @return True if the current slice object has segments or false otherwise
	 */
	public boolean hasSegments(){
		if(segments==null || segments.size()==0)
			return false;
		else 
			return true;
	}
	/**
	 * Return the number of segments objects in the current slice object
	 * 
	 * @return Number of segments objects in the current slice object
	 */
	public int getSegmentCount(){
		return (segments==null? 0 : segments.size());
	}
	/**
	 * Checks if the current slice object has segments and also if file recovery has been completed (by checking if the isRemoved flag is set to true for all segments)

	 * @return True if the current slice object does not have any existing segment objects in the list or any existing segments object has been marked for removal, false otherwise
	 */
	public boolean isAllSegmentsRemoved(){
		if(!hasSegments())
			return true;
		
		for(FileSliceInfo fsi: segments){
			if(!fsi.isRemoved)
				return false;
		}
		
		return true;
	}

	/**
	 * Sets the flag to indicate that the slice is in the midst of recovery
	 * 
	 * @param sliceRecovery Flag to indicate if the slice is in the midst of recovery
	 */
	public void setSliceRecovery(boolean sliceRecovery) {
		this.sliceRecovery = sliceRecovery;
	}
	/**
	 * @return True if the slice is in the midst of recovery, false otherwise.
	 */
	public boolean isSliceRecovery() {
		return sliceRecovery;
	}
}
