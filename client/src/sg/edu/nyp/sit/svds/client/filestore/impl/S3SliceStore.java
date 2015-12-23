package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class S3SliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 3L;
	
	private static final Log LOG = LogFactory.getLog(S3SliceStore.class);
	//static final String awsAccessKeyId = "AKIAIXXU5TKBYF4LKW6A";
    //static final String awsSecretAccessKey = "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";   
    
    public enum Region{
		AP_Singapore ("s3-ap-southeast-1.amazonaws.com"),
		AP_Tokyo ("s3-ap-northeast-1.amazonaws.com"),
		EU_Ireland ("s3-eu-west-1.amazonaws.com"),
		US_Standard ("s3.amazonaws.com"),
		US_West ("s3-us-west-1.amazonaws.com");

		private String url;
		Region(String url){
			this.url=url;
		}
		public String url(){return url;}
	}
    
    //NOTE: bucket must already been created during registration of the slice store to the master server
    private AmazonS3Client s3=null;
    private String bucketName=null;
    
	public S3SliceStore(String serverId) throws SVDSException{
		super(serverId);
		
		FileSliceServerInfo fssi=IFileSliceStore.getServerMapping(serverId);
		Properties props=fssi.getAllProperties();
		
		if(fssi.getKeyId()==null || fssi.getKey()==null || props==null
				|| !props.containsKey(FileSliceServerInfo.S3PropName.CONTAINER.value()))
			throw new SVDSException("Missing storage information.");
		
		try{
			LOG.debug("accesskey:"+fssi.getKeyId());
			LOG.debug("secretkey:"+fssi.getKey());
		
			BasicAWSCredentials s3Acct=new BasicAWSCredentials(fssi.getKeyId(), fssi.getKey());
			
			ClientConfiguration s3Config=new ClientConfiguration();
			s3Config=s3Config.withProtocol(com.amazonaws.Protocol.HTTP);
			if(props.containsKey(FileSliceServerInfo.S3PropName.RETRY_CNT.value())){
				s3Config=s3Config.withMaxErrorRetry(
						Integer.parseInt(props.get(FileSliceServerInfo.S3PropName.RETRY_CNT.value()).toString()));
			}
			
			bucketName=props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString().toLowerCase();
			s3=new AmazonS3Client(s3Acct, s3Config);
			s3.setEndpoint(Region.valueOf(fssi.getServerHost()).url());
					
			if(!s3.doesBucketExist(bucketName))
				throw new SVDSException("Container "+bucketName+" does not exist.");
		}catch(SVDSException ex){
			throw ex;
		}catch(AmazonServiceException ex){
			//System.out.println("Service exception");
			ex.printStackTrace();
			throw new SVDSException(ex);
		}catch(AmazonClientException ex){
			//System.out.println("client exception");
			ex.printStackTrace();
			throw new SVDSException(ex);
		}
	}
	
	@Override
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		try{
			S3Object data=s3.getObject(bucketName, (String) sliceName);
			if(data==null)
				return null;
			
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			InputStream in=data.getObjectContent();
			int len;
			byte[] tmp=new byte[Resources.DEF_BUFFER_SIZE];
			while((len=in.read(tmp))!=-1){
				out.write(tmp, 0, len);
			}
			in.close();
			tmp=null;
			if(out.size()==0)
				return null;
			
			if(blkSize>0){
				List<byte[]> hashes=retrieveHashes(sliceName);
				if(hashes==null || hashes.size()==0)
					throw new SVDSException("Unable to retrieve slice checksum");
				
				byte[] combinedHash=SliceDigest.combineBlkHashes(hashes);
				
				byte[] all=new byte[out.size()+combinedHash.length];
				System.arraycopy(combinedHash, 0, all, 0, combinedHash.length);
				System.arraycopy(out.toByteArray(), 0, all, combinedHash.length, out.size());
				
				return all;
			}else
				return out.toByteArray();
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize) throws SVDSException{
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}
	
	@Override
	public int retrieve(Object sliceName, long offset, int len, int blkSize, byte[] data, int dataOffset) 
		throws SVDSException{
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}

	@Override
	public void delete(Object sliceName) throws SVDSException {
		 try {
			s3.deleteObject(bucketName, (String) sliceName);
			
			//deletes the checksum file, if any
			s3.deleteObject(bucketName, sliceName+".chk");
		} catch (Exception ex) {
			throw new SVDSException(ex);
		} 
	}

	@Override
	public void store(byte[] in, Object sliceName, SliceDigestInfo md) throws SVDSException{
		try{
			//put the slice data
			ObjectMetadata meta=new ObjectMetadata();
			meta.setContentLength(in.length);
			s3.putObject(bucketName, (String) sliceName, new ByteArrayInputStream(in), meta);
			meta=null;
			
			//put the checksums if available
			if(md!=null){
				ByteArrayOutputStream out=new ByteArrayOutputStream();
				for(byte[] h: md.getBlkHashes()){
					out.write(h);
				}
				
				meta=new ObjectMetadata();
				meta.setContentLength(out.size());
				s3.putObject(bucketName, sliceName+".chk", new ByteArrayInputStream(out.toByteArray()), meta);
			}
		}catch(Exception ex){
			ex.printStackTrace();
			throw new SVDSException(ex);
		}
	}
	
	@Override
	public void store(byte[] in, Object sliceName, long offset, int length, SliceDigestInfo md)
			throws SVDSException{
		throw new NotSupportedSVDSException("Partial write is not supported for this slice store.");
	}

	@Override
	public void storeHashes(List<byte[]> in, Object sliceName) throws SVDSException{
		try{
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			for(byte[] d: in){
				out.write(d, 0, d.length);
			}
			
			s3.putObject(bucketName, sliceName+".chk", new ByteArrayInputStream(out.toByteArray()), null);
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException{
		try{
			S3Object chksum=s3.getObject(bucketName, sliceName+".chk");
			if(chksum==null)
				return null;
			
			List<byte[]> hashes=new ArrayList<byte[]>();
			byte[] d=new byte[Resources.HASH_BIN_LEN];
			InputStream in= new BufferedInputStream(chksum.getObjectContent());
			while((in.read(d))!=-1){
				//must copy to a new array as list will only keep the reference
				hashes.add(Arrays.copyOf(d, d.length));
			}
			in.close();
			
			return hashes;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
}
