package sg.edu.nyp.sit.svds;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.util.*;

import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

/**
 * To compute the block digest of a file slice.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class SliceDigest {
	public static final long serialVersionUID = 1L;
	
	private long offset=0L;
	private int length=0;
	
	private SliceDigestInfo info=null;
	private MessageDigest md=null;
	private byte[] sliceChecksum=null;
	private Map<Long, byte[]> blkHashes=null;
	
	/**
	 * Instantiate a blank SliceDigest object with SliceDigestInfo containing block size and key hash.
	 * 
	 * @param info SliceDigestInfo object containing block size and key hash.
	 * @throws SVDSException If the hash algorithm provided {@link Resources#HASH_ALGO} is invalid
	 */
	public SliceDigest(SliceDigestInfo info) throws SVDSException{
		this.info=info;
		this.blkHashes=new HashMap<Long, byte[]>();
		init();
	}
	
	/**
	 * Instantiate a SliceDigest object with given block hashes and SliceDigestInfo containing block size and key hash.
	 * 
	 * @param info SliceDigestInfo object containing block size and key hash.
	 * @param blkHashes List of block hashes (byte array).
	 * @throws SVDSException If the hash algorithm provided {@link Resources#HASH_ALGO} is invalid.
	 */
	public SliceDigest(SliceDigestInfo info, List<byte[]> blkHashes) throws SVDSException{
		this.info=info;
		init();
		setBlkHashes(blkHashes, 0);
	}
	
	private void init() throws SVDSException{
		info.setSliceDigest(this);
		try{ 
			md=MessageDigest.getInstance(Resources.HASH_ALGO);
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
		this.sliceChecksum = new byte[Resources.HASH_BIN_LEN];
	}
	
	/**
	 * Determine the offset so that the next update will generate the hash from the specific offset.
	 * 
	 * @param offset 
	 * @throws SVDSException If the value of the offset is not aligned by the block size.
	 */
	public void setOffset(long offset) throws SVDSException {
		if(offset%info.getBlkSize()>0)
			throw new SVDSException("Offset cannot be in the middle of a block.");
		
		//if hash computation has not complete for current block, just reset the message disget
		if(length!=0){
			//throw new SVDSException("Hash computation has not complete for current block.");
			md.reset();
		}
		
		this.offset = offset;
		length=0;
	}
	
	/**
	 * Determine the offset so that the next update will generate the hash from the specific offset.
	 * 
	 * @param offset
	 * @param data Extra data to be hashed so that the current block hash can be completed
	 * @throws SVDSException If the value of the offset is not aligned by the block size and the length of the data (byte array) does not match the remaining block size.
	 */
	public void setOffset(long offset, byte[] data) throws SVDSException{
		if(offset%info.getBlkSize()>0 && data!=null && data.length!=(offset%info.getBlkSize()))
			throw new SVDSException("Data length does not match start size of block.");
		
		//if data is null, means that user acknowledge that it's a force set of offset
		md.reset();
		this.offset=offset-(offset%info.getBlkSize());
		this.length=0;
		
		this.update(data);
	}
	
	/**
	 * Gets the current offset.
	 * 
	 * @return The current offset.
	 */
	public long getOffset() {
		return offset;
	}

	/**
	 * Populate the block hashes. The offset used is 0.
	 * 
	 * @param blkHashes List of block hashes (byte array).
	 */
	public void setBlkHashes(List<byte[]> blkHashes)  {
		try{ setBlkHashes(blkHashes, 0); }catch(SVDSException ex){ex.printStackTrace();}
	}
	
	/**
	 * Populate the block hashes starting at the specified offset.
	 * 
	 * @param blkHashes List of block hashes (byte array).
	 * @param offset The offset for the given block hashes. 
	 * @throws SVDSException
	 */
	public void setBlkHashes(List<byte[]> blkHashes, long offset) throws SVDSException{
		if(blkHashes==null && this.blkHashes!=null)
			this.blkHashes.clear();
		
		if(offset%info.getBlkSize()>0)
			throw new SVDSException("Offset cannot be in the middle of a block.");
		
		if(blkHashes==null)
			return;
		
		if(this.blkHashes==null)
			this.blkHashes=new HashMap<Long, byte[]>();
		
		for(byte[] h : blkHashes){
			if(this.blkHashes.containsKey(offset))
				aggregateBlkHash(this.blkHashes.get(offset));
			
			this.blkHashes.put(offset, h);
			aggregateBlkHash(h);
			
			offset+=info.getBlkSize();
		}
	}

	/**
	 * Gets the current list of block hashes.
	 * 
	 * @return List of block hashes (byte array).
	 */
	public List<byte[]> getBlkHashes() {
		/*
		for(Map.Entry<Long, byte[]> e: blkHashes.entrySet()){
			System.out.println(e.getKey()+"="+Resources.convertToHex(e.getValue()));
		}
		*/
		return (new ArrayList<byte[]>(blkHashes.values()));
	}
	
	/**
	 * Gets the block hash that the offset is in.
	 * 
	 * @param offset
	 * @return The hash of the block at given offset.
	 */
	public byte[] getBlkHashes(long offset){
		if(offset%info.getBlkSize()>0) 
			offset=offset-(offset%info.getBlkSize());
		
		if(!blkHashes.containsKey(offset))
			return null;
		
		return blkHashes.get(offset);
	}
	
	/**
	 * Gets the list of block hashes between offset and offset+length.
	 * 
	 * @param offset
	 * @param length
	 * @return List of block hashes (byte array) from the offset to offset+length.
	 */
	public List<byte[]> getBlkHashes(long offset, int length){
		long end=offset+length;

		if(offset%info.getBlkSize()>0) 
			offset=offset-(offset%info.getBlkSize());

		List<byte[]> hashes=new ArrayList<byte[]>();
		do{
			if(blkHashes.containsKey(offset))
				hashes.add(blkHashes.get(offset));
			
			offset+=info.getBlkSize();
		}while(offset<end);
		
		return hashes;
	}

	/**
	 * Sets or remove the aggregated hash for the slice. The aggregated hashes is combination of all the block hashes performed by the XOR operation.
	 * 
	 * @param sliceChecksum To remove the slice checksum, pass in a null value.
	 */
	public void setSliceChecksum(byte[] sliceChecksum) {
		if(sliceChecksum==null)
			Arrays.fill(this.sliceChecksum, (byte)0);
		else
			this.sliceChecksum = sliceChecksum;
	}

	/**
	 * Gets the aggregated hash for the current list of block hashes.
	 * 
	 * @return Aggregated hash in byte array.
	 */
	public byte[] getSliceChecksum() {
		return sliceChecksum;
	}
	
	/**
	 * Updates the block hashes using the given data.
	 * 
	 * @param data Byte array containing the data.
	 */
	public void update(byte[] data){
		if(data==null)
			return;
		
		for(byte b: data)
			update(b);
	}
	
	/**
	 * Updates the block hashes using the given data starting from the offset to offset+len.
	 * 
	 * @param data Byte array containing the data
	 * @param offset Position within the byte array to start.
	 * @param len Length of data after the offset to use to update the block hashes.
	 */
	public void update(byte[] data, int offset, int len){
		if(data==null)
			return;
		//System.out.println("[SD] Slice 1st byte from offset " + offset + ": " + data[offset]);
		//System.out.println("[SD] Slice last byte at pos " + (offset + len -1) + ": " + data[offset+len-1]);
		
		for(int i=offset; i<(offset+len); i++)
			update(data[i]);
	}
	
	/**
	 * Updates the block hashes using the specified data.
	 * 
	 * @param data
	 */
	public void update(byte data){
		md.update(data);
		
		offset++;
		length++;
		
		if(length==info.getBlkSize()){
			finalizeBlkHash();
			
			//reset the length
			length=0;
		}
	}
	
	/**
	 * Completes the current block hash computation by performing final operations such as padding.
	 */
	public void finalizeDigest(){
		finalizeBlkHash();
	}
	
	private void finalizeBlkHash(){
		if(length==0)
			return;
		
		//if there's a key then append the key to the content of the blk
		if(info.getKey()!=null)
			md.update(info.getKey().getBytes());
		
		//complete hash for block
		byte[] hash=md.digest();
		
		//if map contains a prev value of hash at the same offset,
		//remove from the slice checksum first
		if(blkHashes.containsKey(offset-length)){
			aggregateBlkHash(blkHashes.get(offset-length));
		}
		
		//update the blk hash in the list
		blkHashes.put(offset-length, hash);
		
		//update the slice checksum
		aggregateBlkHash(hash);
	}
	
	private void aggregateBlkHash(byte[] hash){
		//combine all the current block hashes using the XOR operation
		for(int i=0; i<sliceChecksum.length; i++)
			sliceChecksum[i]=(byte) (sliceChecksum[i]^hash[i]);
	}
	
	/**
	 * Gets the number of bytes remaining to complete the current block.
	 * 
	 * @return Number of bytes.
	 */
	public int getCurrRemainingBlkLength(){
		if(length==0)
			return 0;
		
		return info.getBlkSize()-length;
	}

	/**
	 * Gets the block size that hashes are computed for.
	 * 
	 * @return The block size.
	 */
	public int getBlkSize() {
		return info.getBlkSize();
	}
	
	/**
	 * Gets the key hash that is used in the block hashes computation.
	 * 
	 * @return The key hash or null if none is provided initially.
	 */
	public String getKey(){
		return info.getKey();
	}
	
	/**
	 * Gets the SliceDigestInfo object the current SliceDigest is associated with.
	 * @return
	 */
	public SliceDigestInfo getSliceDigestInfo(){
		return info;
	}
	
	/**
	 * Resets the current SliceDigest object. All existing block hashes are removed. The offset is reset back to 0.
	 */
	public void reset(){
		md.reset();
		length=0;
		offset=0;
		blkHashes.clear();
		this.sliceChecksum = null;
		this.sliceChecksum = new byte[Resources.HASH_BIN_LEN];
	}
	
	/**
	 * Static method compute an aggregated hash based on given block hashes, offset and length (from the offset).
	 * 
	 * @param f The file containing the block hashes.
	 * @param offset The offset to start the computation of the aggregated hash.
	 * @param blkSize The block size that was used to compute the block hashes initially. Used to determine how many block hashes to retrieve from the file based on the length.
	 * @param length The amount of data used for the computation of aggregated hash. Starting from the offset.
	 * @return
	 */
	public static byte[] combineBlkHashes(java.io.File f, long offset, int blkSize, long length){
		if(!f.exists())
			return null;
		
		if(offset>(f.length()/Resources.HASH_BIN_LEN*blkSize))
			return new byte[Resources.HASH_BIN_LEN];
		
		RandomAccessFile fChk=null;
		try{
			fChk=new RandomAccessFile(f, "r");
			
			if(offset!=0){
				fChk.seek((offset/blkSize)*Resources.HASH_BIN_LEN);
			}
			
			int end;
			if(length>0)
				end=(int)Math.ceil((double)(offset+length)/blkSize);
			else
				end=(int)fChk.length()/Resources.HASH_BIN_LEN;
			
			byte[] combinedHash=new byte[Resources.HASH_BIN_LEN];
			byte[] hash=new byte[Resources.HASH_BIN_LEN];
			//System.out.println("[FS] "+f.getName()+" Hash: start " + (offset/blkSize) + " end " + end);
			for(int cnt=(int)(offset/blkSize); cnt<end; cnt++){
				if(fChk.read(hash)!=-1){
					//String strTmp="";
					for(int i=0; i<hash.length; i++){
						combinedHash[i]=(byte) (combinedHash[i]^hash[i]);
						//strTmp+=combinedHash[i] + " ";
					}
					//System.out.println("[FS] "+f.getName()+" Combined hash: "+strTmp);
				}else
					break;
			}
			
			return combinedHash;
		}catch(Exception ex){
			ex.printStackTrace();
			return new byte[Resources.HASH_BIN_LEN];
		}finally{
			if(fChk!=null)
				try{fChk.close();}catch(Exception ex){ex.printStackTrace();}
		}
	}
	
	/**
	 * Static method to compute an aggregated hash given the list of block hashes.
	 * 
	 * @param hashes The list of block hashes to compute the aggregated hash.
	 * @return The aggregated hash in byte array.
	 */
	public static byte[] combineBlkHashes(List<byte[]> hashes){
		byte[] combinedHash=new byte[Resources.HASH_BIN_LEN];
		
		for(byte[] hash: hashes){
			for(int i=0; i<hash.length; i++)
				combinedHash[i]=(byte) (combinedHash[i]^hash[i]);
		}
		
		return combinedHash;
	}
	
	/**
	 * Write the current list of block hashes to the given file.
	 * 
	 * The block hashes are written into the file according to the offset they are generated. For example, if the hash generated for the block
	 * at offset 2048 is ABCDEFGH, then the value ABCDEFGH will be written to the file at the position 16 (assuming the block size is 1024 and the
	 * length of a block hash is 8).
	 * 
	 * @param f The file to update the block hashes into
	 * @throws IOException If error occurs during the write to the file.
	 */
	public void updBlkHashes(java.io.File f) throws Exception{
		RandomAccessFile out=new RandomAccessFile(f, "rw");
		
		try{
			long pos;
			for(Map.Entry<Long, byte[]> e: blkHashes.entrySet()){
				pos=(e.getKey()/info.getBlkSize())*Resources.HASH_BIN_LEN;
				
				//seek only when the previous write is not continuous
				if(pos!=out.getFilePointer())
					out.seek(pos);
				
				out.write(e.getValue());
			}
		}catch(IOException ex){
			throw ex;
		}finally{
			out.close();
			out=null;
		}
	}
}
