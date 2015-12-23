package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

//NOTE: DOES NOT SUPPORT HASHING AND CHECKSUMMING OF SLICE CONTEN!!!!

public class LocalFileSliceStore extends IFileSliceStore {
	public static final long serialVersionUID = 2L;
	
	public LocalFileSliceStore(String serverId){
		super(serverId);
	}
	
	@Override
	public void delete(Object sliceName) throws SVDSException {
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			File f=new File(getServerMapping(serverId).getServerHost()+"/"+sliceName);
			if(f.exists())
				f.delete();
		}catch (Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}

	@Override
	public void store(byte[] in, Object sliceName, SliceDigestInfo md) throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			File f=new File(getServerMapping(serverId).getServerHost()+"/"+sliceName);
			if(!f.exists())
				f.createNewFile();
			
			FileOutputStream out=new FileOutputStream(f);

			out.write(in);
			out.flush();
			out.close();
			out=null;
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
	
	@Override
	public void store(byte[] in, Object sliceName, long offset, int length, SliceDigestInfo md) throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			RandomAccessFile f=new RandomAccessFile(getServerMapping(serverId).getServerHost()+"/"+sliceName, "rw");
			
			f.seek(offset);
			
			f.write(in);
			
			f.close();
			
			f=null;
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
	
	@Override
	public byte[] retrieve(Object sliceName, int blkSize) 
			throws SVDSException {
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			File f=new File(getServerMapping(serverId).getServerHost()+"/"+sliceName);
			if(!f.exists())
				throw new Exception("File slice does not exist.");
			
			FileInputStream in=new FileInputStream(f);
			ByteArrayOutputStream out=new ByteArrayOutputStream();
			byte[] tmp=new byte[512];
			int len;
			while((len=in.read(tmp))!=-1){
				out.write(tmp, 0, len);
			}
			in.close();
			tmp=null;
			
			return out.toByteArray();
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
	
	@Override
	public byte[] retrieve(Object sliceName, long offset, int blkSize) throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		ByteArrayOutputStream outTmp=null;
		
		try{
			RandomAccessFile f=new RandomAccessFile(getServerMapping(serverId).getServerHost()+"/"+sliceName, "r");
			f.seek(offset);
			
			outTmp=new ByteArrayOutputStream();
			byte[] data=new byte[512];
			int len;
			while((len=f.read(data))!=-1){
				outTmp.write(data, 0, len);
			}
			outTmp.flush();
			
			return outTmp.toByteArray();
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}finally{
			outTmp=null;
		}
	}
	
	@Override
	public int retrieve(Object sliceName, long offset, int len, int blkSize, byte[] data, int dataOffset) 
		throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			RandomAccessFile f=new RandomAccessFile(getServerMapping(serverId).getServerHost()+"/"+sliceName, "r");
			f.seek(offset);
			
			byte[] tmp=new byte[512];
			int size, index=dataOffset;
			while((size=f.read(data))!=-1){
				System.arraycopy(tmp, 0, data, index, size);
				index+=size;
			}
			
			return index-dataOffset;
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
	
	@Override
	public void storeHashes(List<byte[]> in, Object sliceName) throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			RandomAccessFile f=new RandomAccessFile(getServerMapping(serverId).getServerHost()+"/"+sliceName+".chk", "rwd");
			
			//truncate the file
			f.setLength(0);
	
			for(byte[] tmp: in){
				f.write(tmp);
			}
			
			f.close();
			f=null;
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
	
	@Override
	public List<byte[]> retrieveHashes(Object sliceName) throws SVDSException{
		if(getServerMapping(serverId)==null)
			throw new SVDSException("File slice store server not found.");
		
		try{
			File f= new File(getServerMapping(serverId).getServerHost()+"/"+sliceName+".chk");
			if(!f.exists())
				return null;
			
			FileInputStream in=new FileInputStream(f);
			List<byte[]> hashes=new ArrayList<byte[]>();
			byte[] tmp=new byte[Resources.HASH_BIN_LEN];
			while(in.read(tmp)!=-1)
				hashes.add(tmp);
			in.close();
			in=null;
			f=null;
			
			return hashes;
		}catch(Exception ex){
			throw new SVDSException(ex.getMessage());
		}
	}
}
