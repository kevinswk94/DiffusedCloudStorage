package sg.edu.nyp.sit.svds.metadata;

import java.util.List;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;

/**
 * Metadata for slice digest
 * 
 * @author Victoria Chin
 * @version %I% %G%
 * @see sg.edu.nyp.sit.svds.SliceDigest
 */
public class SliceDigestInfo {
	public static final long serialVersionUID = 1L;
	
	private final int blkSize; //in bytes
	private final String keyHash;
	private String checksum=null;
	private SliceDigest md=null;
	
	/**
	 * Creates a slice digest info object using block size
	 * 
	 * @param blkSize Size of each block used in the calculation of block hash
	 */
	public SliceDigestInfo(int blkSize){
		this.blkSize=blkSize;
		this.keyHash=null;
	}
	
	/**
	 * Creates a slice digest info object using block size and salt value
	 * 
	 * @param blkSize Size of each block used in the calculation of block hash
	 * @param keyHash Salt value used in the calculation of block hash
	 */
	public SliceDigestInfo(int blkSize, String keyHash){
		this.blkSize=blkSize;
		this.keyHash=keyHash;
	}
	
	/**
	 * Gets the block size used in the calculation of block hash
	 * 
	 * @return Size of each block used in the calculation of block hash
	 */
	public int getBlkSize() {
		return blkSize;
	}
	
	/**
	 * Gets the salt value used in the calculation of block hash
	 * @return Salt value used in the calculation of block hash
	 */
	public String getKey(){
		return keyHash;
	}

	/**
	 * Sets the aggregated checksum of the file slice that is calculated from the block hashes
	 * 
	 * @param checksum Aggregated checksum of the file slice in string
	 */
	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}
	/**
	 * Sets the aggregated checksum of the file slice that is calculated from the block hashes
	 * 
	 * @param checksum Aggregated checksum of the file slice in byte array
	 */
	public void setChecksum(byte[] checksum) {
		this.checksum = Resources.convertToHex(checksum);
	}
	/**
	 * Gets the aggregated checksum of the file size that is calculated from the block hashes
	 *  
	 * @return Aggregated checksum of the file slice in string
	 */
	public String getChecksum() {
		return checksum;
	}
	
	/**
	 * Sets the slice digest object that this slice digest info object is associated with
	 * 
	 * @param md Slice digest object to associate with this slice digest info object
	 */
	public void setSliceDigest(SliceDigest md){
		this.md=md;
	}
	/**
	 * Gets the slice digest object that his slice digest info object is associated with
	 * 
	 * @return Slice digest object that his slice digest info object is associated with
	 */
	public SliceDigest getSliceDigest(){
		return md;
	}
	
	/**
	 * Gets all the block hashes for the file slice. The slice digest info object must be associated with a slice digest object first.
	 * 
	 * @return The list of block hashes
	 */
	public List<byte[]> getBlkHashes() {
		return md.getBlkHashes();
	}
	/**
	 * Gets a portion of the block hashes for the file slice from the offset within the file slice and the length + offset.
	 *   
	 * @param offset
	 * @param length 
	 * @return List of block hashes (byte array) from the offset to offset+length.
	 */
	public List<byte[]> getBlkHashes(long offset, int length){
		return md.getBlkHashes(offset, length);
	}
}
