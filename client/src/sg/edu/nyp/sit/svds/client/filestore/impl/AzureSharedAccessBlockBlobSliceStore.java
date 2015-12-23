package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSharedAccessSVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.blob.client.*;

public class AzureSharedAccessBlockBlobSliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(AzureSharedAccessBlockBlobSliceStore.class);
	
	//private int defRetryLimit=5;
	//private int defRetryInterval=1000*5; //in milliseconds
	
	private String containerName=null;
	private static String SEPARATOR="/";
	
	private enum AccessURLIndex{
		BLOB_END_POINT(0),
		SLICE_NAME(1),
		SLICE_SIGNATURE(2),
		SLICE_CHK_SIGNATURE(3);
		
		private final int i;
		AccessURLIndex(int i){ this.i=i; }
		public int index(){ return i;}
	}
	
	public AzureSharedAccessBlockBlobSliceStore(String serverId) throws SVDSException {
		super(serverId.toLowerCase());
		
		//container names must be lowercase, at least 3-63 chars long
		containerName=serverId.toLowerCase();
	}

	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize)
			throws SVDSException {
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}

	@Override
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		try {
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			CloudBlockBlob blkBlob = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			
			CloudBlockBlob blkBlobChk=null;
			if(blkSize > 0){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				blkBlobChk = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			return AzureBlockBlobImpl.retrieve(blkBlob, blkBlobChk, blkSize);
		}catch(StorageException ex){
			if(ex.getHttpStatusCode()==HttpURLConnection.HTTP_FORBIDDEN)
				throw new UnauthorizedSharedAccessSVDSException(ex);
			else throw new SVDSException(ex);
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public int retrieve(Object sliceName, long offset, int len, int blkSize,
			byte[] data, int dataOffset) throws SVDSException {
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}

	@Override
	public void delete(Object sliceName) throws SVDSException {
		try {
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			CloudBlockBlob blkBlob = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);

			client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			CloudBlockBlob blkBlobChk = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");

			AzureBlockBlobImpl.delete(blkBlob, blkBlobChk);
		}catch(StorageException ex){
			if(ex.getHttpStatusCode()==HttpURLConnection.HTTP_FORBIDDEN)
				throw new UnauthorizedSharedAccessSVDSException(ex);
			else throw new SVDSException(ex);
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
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			CloudBlockBlob blkBlob = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			
			CloudBlockBlob blkBlobChk=null;
			if(md!=null){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				blkBlobChk = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			AzureBlockBlobImpl.store(in, blkBlob, blkBlobChk, md);
		}catch(IOException ex){
			if(ex.getCause() instanceof StorageException){
				StorageException se=(StorageException)ex.getCause();
				if(se.getHttpStatusCode()==HttpURLConnection.HTTP_FORBIDDEN)
					throw new UnauthorizedSharedAccessSVDSException(se);
			}else throw new SVDSException(ex);
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			//ex.printStackTrace();
			throw new SVDSException(ex);
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, long offset, int length,
			SliceDigestInfo md) throws SVDSException {
		throw new NotSupportedSVDSException("Partial write is not supported for this slice store.");
	}

	@Override
	public void storeHashes(List<byte[]> in, Object sliceName)
			throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;

			CloudBlobClient client=new CloudBlobClient(new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]), 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			CloudBlockBlob blkBlob = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			
			AzureBlockBlobImpl.storeHashes(in, blkBlob);
		}catch(IOException ex){
			if(ex.getCause() instanceof StorageException){
				StorageException se=(StorageException)ex.getCause();
				if(se.getHttpStatusCode()==HttpURLConnection.HTTP_FORBIDDEN)
					throw new UnauthorizedSharedAccessSVDSException(se);
			}else throw new SVDSException(ex);
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;

			CloudBlobClient client=new CloudBlobClient(new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]), 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			CloudBlockBlob blkBlob = client.getBlockBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			
			return AzureBlockBlobImpl.retrieveHashes(blkBlob);
		}catch(StorageException ex){
			if(ex.getHttpStatusCode()==HttpURLConnection.HTTP_FORBIDDEN)
				throw new UnauthorizedSharedAccessSVDSException(ex);
			else throw new SVDSException(ex);
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
}
