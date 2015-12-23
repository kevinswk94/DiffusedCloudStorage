package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSharedAccessSVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class S3SharedAccessSliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(S3SharedAccessSliceStore.class);
	
	private enum AccessURLIndex{
		SLICE_SIGNATURE_GET(0),
		SLICE_CHK_SIGNATURE_GET(1),
		SLICE_SIGNATURE_PUT(2),
		SLICE_CHK_SIGNATURE_PUT(3),
		SLICE_SIGNATURE_DEL(4),
		SLICE_CHK_SIGNATURE_DEL(5);
		
		private final int i;
		AccessURLIndex(int i){ this.i=i; }
		public int index(){ return i;}
	}
	
	public S3SharedAccessSliceStore(String serverId) {
		super(serverId);
	}

	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize)
			throws SVDSException {
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}

	@Override
	public byte[] retrieve(Object sliceName, int blkSize) throws SVDSException {
		String[] urls=(String[])sliceName;
		
		try{
			ByteArrayOutputStream out=retrieve(urls[AccessURLIndex.SLICE_SIGNATURE_GET.index()]);
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
			ex.printStackTrace();
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
	private ByteArrayOutputStream retrieve(String url) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			fsConn=(HttpURLConnection)(new URL(url)).openConnection();
			fsConn.setRequestMethod("GET");
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
				default:
					throw new SVDSException(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}
			
			InputStream in= fsConn.getInputStream();
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			int len;
			byte[] tmp=new byte[4096];
			while((len=in.read(tmp))!=-1){
				out.write(tmp, 0, len);
			}
			in.close();
			tmp=null;
			
			return out;
		}catch(Exception ex){
			throw ex;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public int retrieve(Object sliceName, long offset, int len, int blkSize,
			byte[] data, int dataOffset) throws SVDSException {
		throw new NotSupportedSVDSException("Partial read is not supported for this slice store.");
	}

	@Override
	public void delete(Object sliceName) throws SVDSException {
		String[] urls=(String[])sliceName;
		
		try{
			delete(urls[AccessURLIndex.SLICE_SIGNATURE_DEL.index()]);
			delete(urls[AccessURLIndex.SLICE_CHK_SIGNATURE_DEL.index()]);
		}catch(SVDSException ex){
			ex.printStackTrace();
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
	private void delete(String url) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			fsConn=(HttpURLConnection)(new URL(url)).openConnection();
			fsConn.setRequestMethod("DELETE");
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
				case HttpURLConnection.HTTP_NO_CONTENT:
					break;
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
				default:
					throw new SVDSException(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}
		}catch(Exception ex){
			throw ex;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, SliceDigestInfo md)
			throws SVDSException {
		String[] urls=(String[])sliceName;
		
		try{
			store(in, urls[AccessURLIndex.SLICE_SIGNATURE_PUT.index()]);
			
			//put the checksums if available
			if(md!=null){
				ByteArrayOutputStream out=new ByteArrayOutputStream();
				for(byte[] h: md.getBlkHashes()){
					out.write(h);
				}
				
				store(out.toByteArray(), urls[AccessURLIndex.SLICE_CHK_SIGNATURE_PUT.index()]);
			}
		}catch(SVDSException ex){
			ex.printStackTrace();
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}
	
	private void store(byte[] in, String url) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			fsConn=(HttpURLConnection)(new URL(url)).openConnection();
			fsConn.setRequestMethod("PUT");
			fsConn.setDoOutput(true);
			
			OutputStream out=fsConn.getOutputStream();
			out.write(in);
			out.flush();
			out.close();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new UnauthorizedSharedAccessSVDSException("Invalid shared access URL");
				default:
					throw new SVDSException(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}
		}catch(Exception ex){
			throw ex;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
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
		String[] urls=(String[])sliceName;
		
		try{
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			for(byte[] d: in){
				out.write(d, 0, d.length);
			}
			
			store(out.toByteArray(), urls[AccessURLIndex.SLICE_CHK_SIGNATURE_PUT.index()]);
		}catch(SVDSException ex){
			ex.printStackTrace();
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}		
	}

	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException {
		String[] urls=(String[])sliceName;
		
		try{
			ByteArrayOutputStream out=retrieve(urls[AccessURLIndex.SLICE_CHK_SIGNATURE_GET.index()]);
			if(out.size()==0) return null;
			
			List<byte[]> hashes=new ArrayList<byte[]>();
			byte[] d=new byte[Resources.HASH_BIN_LEN];
			ByteArrayInputStream in=new ByteArrayInputStream(out.toByteArray());
			while((in.read(d))!=-1){
				//must copy to a new array as list will only keep the reference
				hashes.add(Arrays.copyOf(d, d.length));
			}
			in.close();
			
			return hashes;
		}catch(SVDSException ex){
			ex.printStackTrace();
			throw ex;
		}catch(Exception ex){
			throw new SVDSException(ex);
		}		
	}

}
