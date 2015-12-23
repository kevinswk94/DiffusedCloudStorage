package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.blob.client.*;

import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class AzurePageBlobSliceStore extends IFileSliceStore  {
	public static final long serialVersionUID = 3L;

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(AzurePageBlobSliceStore.class);

	private CloudBlobClient blobStorage=null;
	private CloudBlobContainer blobContainer=null;

	private int defRetryLimit=5;
	private int defRetryInterval=1000*5; //in milliseconds

	public AzurePageBlobSliceStore(String serverId) throws SVDSException {
		super(serverId.toLowerCase());

		FileSliceServerInfo fssi=IFileSliceStore.getServerMapping(serverId);
		//container names must be lowercase, at least 3-63 chars long
		String containerName=serverId.toLowerCase();
		Properties props=fssi.getAllProperties();
		if(props==null) props=new Properties();

		if(fssi.getKeyId()==null || fssi.getKey()==null)
			throw new SVDSException("Missing storage information.");

		try{
			CloudStorageAccount cloudAccount=new CloudStorageAccount(
					new StorageCredentialsAccountAndKey(fssi.getKeyId(), fssi.getKey()));

			blobStorage=cloudAccount.createCloudBlobClient();
			blobContainer=blobStorage.getContainerReference(containerName);
			if(!blobContainer.exists())
				throw new SVDSException("Storage container " + containerName + " does not exist.");

			blobStorage.setRetryPolicyFactory(new RetryLinearRetry(
					(props.containsKey(FileSliceServerInfo.AzurePropName.RETRY_INTERVAL.value())? 
							Integer.parseInt(props.get(FileSliceServerInfo.AzurePropName.RETRY_INTERVAL.value()).toString())
							: defRetryInterval),
							(props.containsKey(FileSliceServerInfo.AzurePropName.RETRY_CNT.value())? Integer.parseInt(props.get(FileSliceServerInfo.AzurePropName.RETRY_CNT.value()).toString())
									: defRetryLimit)));
		}catch(Exception ex){
			ex.printStackTrace();
			throw new SVDSException(ex);
		}
	}

	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize)
	throws SVDSException {
		try{
			return AzurePageBlobImpl.retrieve(blobContainer.getPageBlobReference((String) sliceName), 
					(blkSize > 0 ? blobContainer.getPageBlobReference(sliceName+".chk") : null), 
					offset, blkSize);
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		try{
			return AzurePageBlobImpl.retrieve(blobContainer.getPageBlobReference((String) sliceName), 
					(blkSize > 0 ? blobContainer.getPageBlobReference(sliceName+".chk") : null), 
					blkSize);
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
			return AzurePageBlobImpl.retrieve(blobContainer.getPageBlobReference((String) sliceName), 
					(blkSize > 0 ? blobContainer.getPageBlobReference(sliceName+".chk") : null), 
					offset, len, blkSize, data, dataOffset);
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void delete(Object sliceName) throws SVDSException {
		try {
			AzurePageBlobImpl.delete(blobContainer.getPageBlobReference((String) sliceName), 
					blobContainer.getPageBlobReference(sliceName+".chk"));
		}catch (SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, SliceDigestInfo md)
	throws SVDSException {
		try{
			AzurePageBlobImpl.store(in, blobContainer.getPageBlobReference((String) sliceName), 
					(md!=null ? blobContainer.getPageBlobReference(sliceName+".chk") : null), md);
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
			AzurePageBlobImpl.store(in, blobContainer.getPageBlobReference((String) sliceName), 
					(md!=null ? blobContainer.getPageBlobReference(sliceName+".chk") : null), 
					offset, length, md);
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
			AzurePageBlobImpl.storeHashes(in, blobContainer.getPageBlobReference(sliceName+".chk"));
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException("Unable to create blk hash file.");
		}
	}

	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException {
		try{
			return AzurePageBlobImpl.retrieveHashes(blobContainer.getPageBlobReference(sliceName+".chk"));
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

	/*
	public void clearBlobs() throws Exception{
		for(ListBlobItem b: blobContainer.listBlobs()){
			blobContainer.getPageBlobReference(b.getUri().toString()).deleteIfExists();
		}
	}
	 */
}
