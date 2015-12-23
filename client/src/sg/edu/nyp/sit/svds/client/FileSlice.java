package sg.edu.nyp.sit.svds.client;

import java.io.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.filestore.FileSliceStoreFactory;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.*;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class FileSlice implements Comparable<FileSlice> {
	public static final long serialVersionUID = 3L;
	
	private static final Log LOG = LogFactory.getLog(FileSlice.class);
	
	private IMasterNamespaceTable mns=null;
	
	private FileSliceInfo fsInfo=null;
	
	private IFileSliceStore fss=null;
	private User usr=null;
	
	public enum Codes{
		OK, //indicate the read or write is successful
		PROCESSING, //indicate the read or write is running
		ERR, //indicate the read or write encounter generic error
		CORRUPT, //indicate that the read is successful, but the checksum of the read is different
					//from the one found in the fsInfo object
		UNAUTHORIZED //indicate the slice store key is not correct for processing
	}
	
	/*
	public final static int ERR_OK=0; //indicate the read or write is successful
	public final static int ERR_EMPTY=1; //indicate the read or write has not been started
	public final static int ERR_PROCESSING=2; //indicate the read or write is running
	public final static int ERR_ERR=-1; //indicate the read or write encounter generic error
	//indicate that the read is successful, but the checksum of the read is different
	//from the one found in the fsInfo object
	public final static int ERR_CORRUPT=-2;
	//indicate the slice store key is not correct for processing
	public final static int ERR_UNAUTHORIZED=-3;
	*/
	
	private Codes lastReadStatus=null;
	private long lastReadTime=0;
	private Codes lastWriteStatus=null;

	//input stream for file slice
	public InputStream in=null;
	
	//to store the list of access URLs used by the file slice, the key is the slice name
	private Hashtable<String, Object> accessUrls=new Hashtable<String, Object>();
	private String currSliceName=null;
	
	public FileSlice(FileSliceInfo fs, User usr) throws SVDSException{
		if(fs==null)
			throw new SVDSException("File slice info cannot be null");
		
		this.fsInfo=fs;
		this.usr=usr;
		
		fss = FileSliceStoreFactory.getInstance(fsInfo.getServerId());
		mns=MasterTableFactory.getNamespaceInstance();
		
		if(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS)){
			accessUrls.put(fs.getSliceName(), 
					mns.getSharedAccessURL(fs.getServerId(), fs.getSliceName(), usr));
		}else accessUrls.put(fs.getSliceName(), fs.getSliceName());

		currSliceName=fs.getSliceName();
	}
	
	FileSliceInfo getFileSliceInfo(){
		return this.fsInfo;
	}
	void setFileSliceInfo(FileSliceInfo fsInfo){
		try{
			if(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS)){
				accessUrls.clear();
				
				accessUrls.put(fsInfo.getSliceName(), 
						mns.getSharedAccessURL(fsInfo.getServerId(), fsInfo.getSliceName(), usr));
			}else accessUrls.put(fsInfo.getSliceName(), fsInfo.getSliceName());
		}catch(SVDSException ex){
			LOG.error(ex);
		}
		
		this.fsInfo=null;
		fss=null;
		
		this.fsInfo=fsInfo;
		currSliceName=fsInfo.getSliceName();
		fss = FileSliceStoreFactory.getInstance(fsInfo.getServerId());
	}
	
	public void setSliceChecksum(String sliceChecksum){
		fsInfo.setSliceChecksum(sliceChecksum);
	}
	
	public String getSliceChecksum(){
		return fsInfo.getSliceChecksum();
	}
	
	public void setSliceSeq(int sliceSeq){
		fsInfo.setSliceSeq(sliceSeq);
	}
	
	public int getSliceSeq(){
		return fsInfo.getSliceSeq();
	}

	public List<byte[]> getBlkHashes() throws SVDSException{
		try{
			return fss.retrieveHashes(accessUrls.get(currSliceName));
		}catch (UnauthorizedSharedAccessSVDSException ex){
			if(!refreshSharedAccessURLs(fsInfo.getServerId(), fsInfo.getSliceName())){
				throw new UnauthorizedSharedAccessSVDSException("Unable to get shared access URL");
			}
			
			return getBlkHashes();
		}catch(UnauthorizedSVDSException ex){
			if(!refreshRestletSliceServerKey(fsInfo.getServerId())){
				throw new UnauthorizedSVDSException("Unable to refresh slice server key.");
			}
			
			return getBlkHashes();
		}
	}
	
	/*
	public void setBlkHashes(List<byte[]> sliceHashes){
		fsInfo.setBlkHashes(sliceHashes);
	}
	*/
	
	public void resetReadStatus(){
		lastReadStatus=null;
		lastReadTime=0;
	}
	
	public void resetWriteStatus(){
		lastWriteStatus=null;
	}
	
	public void flagLastWriteError(Codes status){
		if(status!=null && status != Codes.OK &&
				status!=Codes.PROCESSING)
			lastWriteStatus=status;	
	}
	
	public void flagLastReadError(Codes status){
		if(status!=null && status != Codes.OK &&
				status!=Codes.PROCESSING)
			lastReadStatus=status;	
	}
	
	public boolean isLastWriteError(){
		return (lastWriteStatus==Codes.ERR || lastWriteStatus==Codes.CORRUPT
				|| lastWriteStatus==Codes.UNAUTHORIZED);
	}
	public Codes getLastWriteError(){
		return lastWriteStatus;
	}
	
	public boolean isLastReadError(){
		return (lastReadStatus==Codes.ERR || lastReadStatus==Codes.CORRUPT
				|| lastReadStatus==Codes.UNAUTHORIZED);
	}
	public Codes getLastReadError(){
		return lastReadStatus;
	}
	
	private boolean refreshRestletSliceServerKey(String serverId){
		try{
			mns.refreshRestletSliceServerKey(serverId, usr);
			return true;
		}catch(SVDSException ex){
			return false;
		}
	}
	
	private boolean refreshSharedAccessURLs(String svrId, String sliceName){
		try{
			accessUrls.put(fsInfo.getSliceName(), 
					mns.getSharedAccessURL(svrId, sliceName, usr));
			return true;
		}catch(SVDSException ex){
			return false;
		}
	}
	
	//Access the physical file server to retrieve the file 
	//slice. Because client might not know the size of the entire slice
	//may be hard for client to init the byte array to pass in.
	//Therefore, the method will return a byte array instead
	public byte[] read(int blkSize) throws SVDSException{
		if(!isComplete())
			throw new SVDSException("Slice is not complete.");
		
		try{
			//checkSliceServer(fsInfo.getServerId());
			return fss.retrieve(accessUrls.get(currSliceName), blkSize);
		}catch (UnauthorizedSharedAccessSVDSException ex){
			LOG.debug("Wrong shared access URL at read(int blkSize) method");
			if(!refreshSharedAccessURLs(fsInfo.getServerId(), fsInfo.getSliceName())){
				throw new UnauthorizedSharedAccessSVDSException("Unable to get shared access URL");
			}
			
			return read(blkSize);
		}catch(UnauthorizedSVDSException ex){
			LOG.debug("Wrong signature generated at read(int blkSize) method");
			if(!refreshRestletSliceServerKey(fsInfo.getServerId())){
				throw ex;
			}
			
			return read(blkSize);
		}catch(SVDSException ex){
			LOG.error(ex);
			throw ex;
		}finally{
			lastReadTime=(new Date()).getTime();
		}
	}
	
	//client might not know the size of the slice so hard for them to init array to
	//pass in. Therefore, the method will return a byte array instead
	public byte[] read(long offset, int blkSize) throws SVDSException{
		if(offset<0) throw new SVDSException("Offset cannot be less than 0.");
		
		if(!isComplete())
			throw new SVDSException("Slice is not complete.");
		
		try{
			return fss.retrieve(accessUrls.get(currSliceName), offset, blkSize);
		}catch (UnauthorizedSharedAccessSVDSException ex){
			LOG.debug("Wrong shared access URL at read(long offset,int blkSize) method");
			if(!refreshSharedAccessURLs(fsInfo.getServerId(), fsInfo.getSliceName())){
				throw new UnauthorizedSharedAccessSVDSException("Unable to get shared access URL");
			}
			
			return read(offset, blkSize);
		}catch(UnauthorizedSVDSException ex){
			LOG.debug("Wrong signature generated at read(long offset,int blkSize) method");
			if(!refreshRestletSliceServerKey(fsInfo.getServerId())){
				throw ex;
			}
			
			return read(offset, blkSize);
		}catch (SVDSException ex){
			LOG.error(ex);
			throw ex;
		}finally{
			lastReadTime=(new Date()).getTime();
		}
	}
	
	public int read(long offset, int len, int blkSize, byte[] out, int outOffset) throws SVDSException{
		if(offset<0 || len<0) throw new SVDSException("Offset/length cannot be less than 0.");
		
		if(!isComplete())
			throw new SVDSException("Slice is not complete.");
		
		try{
			return fss.retrieve(accessUrls.get(currSliceName), offset, len, blkSize, out, outOffset);
		}catch (UnauthorizedSharedAccessSVDSException ex){
			LOG.debug("Wrong shared access URL at read(long offset,int len,int blkSize) method");
			if(!refreshSharedAccessURLs(fsInfo.getServerId(), fsInfo.getSliceName())){
				throw new UnauthorizedSharedAccessSVDSException("Unable to get shared access URL");
			}
			
			return read(offset, len, blkSize, out, outOffset);
		}catch(UnauthorizedSVDSException ex){
			LOG.debug("Wrong signature generated at read(long offset,int len,int blkSize) method");
			if(!refreshRestletSliceServerKey(fsInfo.getServerId())){
				//lastReadStatus=ERR_UNAUTHORIZED;
				throw ex;
			}
			
			return read(offset, len, blkSize, out, outOffset);
		}catch (SVDSException ex){
			ex.printStackTrace();
			LOG.error(ex);
			//lastReadStatus=ERR_ERR;
			throw ex;
		}finally{
			lastReadTime=(new Date()).getTime();
		}
	}
	
	public long getLastReadTime(){
		return lastReadTime;
	}
	
	public Codes write(byte[] in, SliceDigestInfo md){
		try{
			fss.store(in, accessUrls.get(currSliceName), md);
			return Codes.OK;
		}catch (UnauthorizedSharedAccessSVDSException ex){
			if(!refreshSharedAccessURLs(fsInfo.getServerId(), fsInfo.getSliceName())){
				return Codes.UNAUTHORIZED;
			}
			
			return write(in, md);
		}catch(UnauthorizedSVDSException ex){
			if(!refreshRestletSliceServerKey(fsInfo.getServerId())){
				return Codes.UNAUTHORIZED;
			}
		
			return write(in, md);
		}catch(SVDSException ex){
			ex.printStackTrace();
			LOG.error(ex);
			return Codes.ERR;
		}
	}
	
	public Codes write(FileSliceInfo seg, byte[] in, long offset, int length, SliceDigestInfo md){
		IFileSliceStore fss;
		
		try{
			//if seg is null, then use the main slice
			if(seg==null){
				seg=fsInfo;
				fss=this.fss;
			}else{
				fss=FileSliceStoreFactory.getInstance(seg.getServerId());
				
				if(!accessUrls.containsKey(seg.getSliceName())){
					if(ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS))
						accessUrls.put(seg.getSliceName(), mns.getSharedAccessURL(seg.getServerId(), seg.getSliceName(), usr));
					else accessUrls.put(seg.getSliceName(), seg.getSliceName());
				}
			}
			
			//System.out.println("slice: " + seg.getSliceName() + " offset: " + offset + " len: " + length 
			//		+ " checksum: " + md.getChecksum() + "\nData: " + Resources.concatByteArray(in, 0, length));
			 
			fss.store(in, accessUrls.get(seg.getSliceName()), offset, length, md);
			
			return Codes.OK;
		}catch (UnauthorizedSharedAccessSVDSException ex){
			if(!refreshSharedAccessURLs(seg.getServerId(), seg.getSliceName())){
				return Codes.UNAUTHORIZED;
			}
			
			return write(seg, in, offset, length, md);
		}catch(UnauthorizedSVDSException ex){
			if(!refreshRestletSliceServerKey(seg.getServerId())){
				return Codes.UNAUTHORIZED;
			}
			
			return write(seg, in, offset, length, md);
		}catch(CorruptedSVDSException ex){
			LOG.error(ex);
			//lastWriteStatus=ERR_CORRUPT;
			return Codes.CORRUPT;
		}catch(SVDSException ex){
			LOG.error(ex);
			//lastWriteStatus=ERR_ERR;
			return Codes.ERR;
		}finally{
			fss=null;
		}
	}
	
	public Codes write(byte[] in, long offset, int length, SliceDigestInfo md){
		return write(null, in, offset, length, md);
	}
	
	//Access the physical file server to delete the file slice
	public void delete(){
		//if there are multiple segments, delete them as well
		//deleteSlice(fsInfo.getServerId(), fsInfo.getSliceName());
		delete(fsInfo);
		
		if(fsInfo.hasSegments()){
			for(FileSliceInfo fsi: fsInfo.getSegments())
				//deleteSlice(fsi.getServerId(), fsi.getSliceName());
				delete(fsi);
		}	
	}
	
	public void delete(FileSliceInfo seg){
		IFileSliceStore fss;
		
		try{
			fss=FileSliceStoreFactory.getInstance(seg.getServerId());
			fss.delete(accessUrls.containsKey(seg.getSliceName()) ? accessUrls.get(seg.getSliceName())
					: (ClientProperties.getBool(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS) ?
						mns.getSharedAccessURL(seg.getServerId(), seg.getSliceName(), usr) : seg.getSliceName()));
		}catch (UnauthorizedSharedAccessSVDSException ex){
			if(!refreshSharedAccessURLs(seg.getServerId(), seg.getSliceName())){
				return;
			}
			
			delete();
		}catch(UnauthorizedSVDSException ex){
			if(!refreshRestletSliceServerKey(seg.getServerId())){
				return;
			}
			
			delete();
		}catch(SVDSException ex){
			LOG.error(ex);
		}finally{
			fss=null;
		}
	}
	
	/*
	private void deleteSlice(String serverId, String sliceName){	
		try{
			checkSliceServer(serverId);
			fss.delete(sliceName);
		}catch(UnauthorizedSVDSException ex){
			if(!refreshSliceServerKey(serverId)){
				return;
			}
			
			delete();
		}catch(SVDSException ex){
			LOG.error(ex);
		}
	}
	*/
	
	/*
	public boolean updateBlkHashes(){
		try{
			fss.storeHashes(fsInfo.getBlkHashes(), fsInfo.getServerId(), fsInfo.getSliceName());
			return true;
		}catch(SVDSException ex){
			return false;
		}
	}
	*/

	public boolean isComplete(){
		return (!fsInfo.hasSegments() && !fsInfo.isSliceRecovery());
	}
	
	public boolean supportMode(FileIOMode mode){
		FileIOMode fsMode=IFileSliceStore.getServerMapping(fsInfo.getServerId()).getMode();

		return (fsMode==FileIOMode.STREAM || fsMode==mode);
	}

	//when sorting a list of file slices, sort according to the seq nos
	@Override
	public int compareTo(FileSlice o) {
		return (this.getSliceSeq() < o.getSliceSeq() ? -1 :
            (this.getSliceSeq() == o.getSliceSeq() ? 0 : 1));

	}
}
