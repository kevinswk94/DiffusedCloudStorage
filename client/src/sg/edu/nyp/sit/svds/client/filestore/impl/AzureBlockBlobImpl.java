package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class AzureBlockBlobImpl {
	public static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(AzureBlockBlobImpl.class);

	public static byte[] retrieve(CloudBlockBlob blkBlob, CloudBlockBlob blkBlobChk, int blkSize) throws Exception {
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		blkBlob.download(out);

		if(blkSize>0){
			List<byte[]> hashes=retrieveHashes(blkBlobChk);
			if(hashes==null || hashes.size()==0)
				throw new SVDSException("Unable to retrieve slice checksum");

			byte[] combinedHash=SliceDigest.combineBlkHashes(hashes);

			byte[] all=new byte[out.size()+combinedHash.length];			
			System.arraycopy(combinedHash, 0, all, 0, combinedHash.length);
			System.arraycopy(out.toByteArray(), 0, all, combinedHash.length, out.size());

			return all;
		}else
			return out.toByteArray();
	}

	public static void delete(CloudBlockBlob blkBlob, CloudBlockBlob blkBlobChk) throws Exception {
		blkBlob.delete();

		//deletes the checksum file, if any
		blkBlobChk.deleteIfExists();
	}

	public static void store(byte[] in, CloudBlockBlob blkBlob, CloudBlockBlob blkBlobChk, SliceDigestInfo md)
		throws Exception {
		store(in, blkBlob);

		if(md!=null){
			storeHashes(md.getBlkHashes(), blkBlobChk);
		}
	}

	private static void store(byte[] in, CloudBlockBlob blkBlob) throws Exception{
		OutputStream out=blkBlob.openOutputStream();
		out.write(in);
		out.close();
		//blob.upload(new ByteArrayInputStream(in), in.length);

		blkBlob.getProperties().setContentType("application/octet-stream");
		blkBlob.uploadProperties();
	}

	public static void storeHashes(List<byte[]> in, CloudBlockBlob blkBlob)
	throws Exception {
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		for(byte[] d: in){
			out.write(d, 0, d.length);
		}

		store(out.toByteArray(), blkBlob);
	}

	public static List<byte[]> retrieveHashes(CloudBlockBlob blkBlob) throws Exception {
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		blkBlob.download(out);

		List<byte[]> hashes=new ArrayList<byte[]>();
		byte[] d=out.toByteArray();
		for(int i=0; i<d.length; i+=Resources.HASH_BIN_LEN){
			hashes.add(Arrays.copyOfRange(d, i, i+Resources.HASH_BIN_LEN));
		}

		return hashes;
	}
}
