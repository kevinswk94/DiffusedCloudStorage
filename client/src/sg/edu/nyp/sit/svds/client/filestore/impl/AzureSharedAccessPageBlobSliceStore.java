package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSharedAccessSVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class AzureSharedAccessPageBlobSliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(AzureSharedAccessPageBlobSliceStore.class);
	
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
	
	public AzureSharedAccessPageBlobSliceStore(String serverId) {
		super(serverId.toLowerCase());
		
		//container names must be lowercase, at least 3-63 chars long
		containerName=serverId.toLowerCase();
	}

	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize)
			throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			CloudPageBlob pgBlobChk=null;
			if(blkSize>0){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				
				pgBlobChk = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			return AzurePageBlobImpl.retrieve(pgBlob, pgBlobChk, offset, blkSize);
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
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			CloudPageBlob pgBlobChk=null;
			if(blkSize>0){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				
				pgBlobChk = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			return AzurePageBlobImpl.retrieve(pgBlob, pgBlobChk, blkSize);
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
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			CloudPageBlob pgBlobChk=null;
			if(blkSize>0){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				
				pgBlobChk = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			return AzurePageBlobImpl.retrieve(pgBlob, pgBlobChk, offset, len, blkSize, data, dataOffset);
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
	public void delete(Object sliceName) throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			
			client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			CloudPageBlob pgBlobChk=client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			
			AzurePageBlobImpl.delete(pgBlob, pgBlobChk);
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
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			CloudPageBlob pgBlobChk=null;
			if(md!=null){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				
				pgBlobChk = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			AzurePageBlobImpl.store(in, pgBlob, pgBlobChk, md);
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
	public void store(byte[] in, Object sliceName, long offset, int length,
			SliceDigestInfo md) throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]);
			CloudPageBlob pgBlobChk=null;
			if(md!=null){
				client=new CloudBlobClient(endpoint, 
						new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
				
				pgBlobChk = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			}
			
			AzurePageBlobImpl.store(in, pgBlob, pgBlobChk, offset, length, md);
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
	public void storeHashes(List<byte[]> in, Object sliceName)
			throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			
			AzurePageBlobImpl.storeHashes(in, pgBlob);
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
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException {
		try{
			String[] urls=(String[]) sliceName;
			URI endpoint=new URI(urls[AccessURLIndex.BLOB_END_POINT.index()]);
			CloudBlobClient client=new CloudBlobClient(endpoint, 
					new StorageCredentialsSharedAccessSignature(urls[AccessURLIndex.SLICE_CHK_SIGNATURE.index()]));
			
			CloudPageBlob pgBlob = client.getPageBlobReference(containerName+SEPARATOR+urls[AccessURLIndex.SLICE_NAME.index()]+".chk");
			
			return AzurePageBlobImpl.retrieveHashes(pgBlob);
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

}