package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.net.URLEncoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSharedAccessSVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.RestletFileSliceServerQueryPropName;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class RestletSharedAccessSliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(RestletSharedAccessSliceStore.class);

	private FileSliceServerInfo fs=null;
	
	public RestletSharedAccessSliceStore(String serverId) {
		super(serverId);
		fs=getServerMapping(serverId);
	}
	
	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize) throws SVDSException {
		try{
			String query=((String[]) sliceName)[0]
			                  +(offset==-1L?"":"&"+RestletFileSliceServerQueryPropName.Slice.OFFSET.value()+"="+offset)
			                  +(blkSize<=0?"":"&"+RestletFileSliceServerQueryPropName.Slice.FILE_BLKSIZE.value()+"="+blkSize);
			
			return RestletSliceStoreImpl.retrieve(fs, query);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		try{
			String query=((String[]) sliceName)[0]
			  				+(blkSize<=0?"":"&"+RestletFileSliceServerQueryPropName.Slice.FILE_BLKSIZE.value()+"="+blkSize);
			
			return RestletSliceStoreImpl.retrieve(fs, query);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public int retrieve(Object sliceName, long offset, int len, int blkSize,
			byte[] data, int dataOffset) throws SVDSException {
		if(data==null || data.length<(len+dataOffset+(blkSize==0?0:Resources.HASH_BIN_LEN)))
			throw new SVDSException("Array index out of bounds.");
		
		try{
			String query=((String[]) sliceName)[0]
				+ (offset==-1L ? "" : "&"+RestletFileSliceServerQueryPropName.Slice.OFFSET.value()+"="+offset)
				+ (len==-1 ? "" :"&"+RestletFileSliceServerQueryPropName.Slice.LENGTH.value()+"="+len)
				+ (blkSize<=0 ? "" :"&"+RestletFileSliceServerQueryPropName.Slice.FILE_BLKSIZE.value()+"="+blkSize);
			
			return RestletSliceStoreImpl.retrieve(fs, data, dataOffset, query);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void delete(Object sliceName) throws SVDSException {
		try{
			RestletSliceStoreImpl.delete(fs, ((String[]) sliceName)[0]);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, SliceDigestInfo md)
			throws SVDSException {
		try{
			String query=((String[]) sliceName)[0]
                + (md==null?"":"&"+RestletFileSliceServerQueryPropName.Slice.CHECKSUM.value()+"="+URLEncoder.encode(md.getChecksum(), "UTF-8")
				+ "&"+RestletFileSliceServerQueryPropName.Slice.FILE_KEYHASH.value()+"="+URLEncoder.encode(md.getKey(), "UTF-8")
				+ "&"+RestletFileSliceServerQueryPropName.Slice.FILE_BLKSIZE.value()+"="+md.getBlkSize());
			
			RestletSliceStoreImpl.store(fs, in, in.length, query);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, long offset, int length,
			SliceDigestInfo md) throws SVDSException {
		try{
			String query=((String[]) sliceName)[0]
			    + "&"+RestletFileSliceServerQueryPropName.Slice.OFFSET.value()+"="+offset
			    + (md==null?"":"&"+RestletFileSliceServerQueryPropName.Slice.CHECKSUM.value()+"="+URLEncoder.encode(md.getChecksum(), "UTF-8")
				+ "&"+RestletFileSliceServerQueryPropName.Slice.FILE_KEYHASH.value()+"="+URLEncoder.encode(md.getKey(), "UTF-8")
				+ "&"+RestletFileSliceServerQueryPropName.Slice.FILE_BLKSIZE.value()+"="+md.getBlkSize());
			
			RestletSliceStoreImpl.store(fs, in, length, query);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void storeHashes(List<byte[]> in, Object sliceName)
			throws SVDSException {
		try{
			RestletSliceStoreImpl.storeHashes(fs, in, ((String[])sliceName)[0]);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException {
		try{
			return RestletSliceStoreImpl.retrieveHashes(fs, ((String[])sliceName)[0]);
		}catch(UnauthorizedSVDSException ex){
			throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
}
