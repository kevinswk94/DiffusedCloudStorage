package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.exception.CorruptedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

import com.microsoft.windowsazure.services.blob.client.*;

public class AzurePageBlobImpl {
	public static final long serialVersionUID = 2L;

	private static final Log LOG = LogFactory.getLog(AzurePageBlobImpl.class);

	public static byte[] retrieve(CloudPageBlob pgBlob, CloudPageBlob pgBlobChk, long offset, int blkSize) 
	throws Exception{
		ByteArrayOutputStream out=new ByteArrayOutputStream();

		if(!pgBlob.exists()) return null;

		//get list of pages with valid data
		List<PageRange> pges=pgBlob.downloadPageRanges();

		//if checksum is required then fill it with 0 first
		if(blkSize>0) for(int i=0;i<Resources.HASH_BIN_LEN; i++) out.write(0);

		//check if the 1st page range is bigger than offset, if yes, pad with
		//0 until the 1st page range start offset
		if(offset<pges.get(0).getStartOffset()){
			for(int i=0; i<(pges.get(0).getStartOffset()-offset); i++) out.write(0);

			offset=pges.get(0).getStartOffset();
		}

		byte[] tmp=new byte[0];
		for(PageRange r: pges) {
			if(offset>r.getEndOffset()) continue;

			if(offset>=r.getStartOffset() && offset<=r.getEndOffset()){
				r.setStartOffset((int)offset);
			}

			if(r.getEndOffset()-r.getStartOffset()+1>tmp.length){
				tmp=null;
				tmp=new byte[(int) (r.getEndOffset()-r.getStartOffset()+1)];
			}
			pgBlob.downloadRange(r.getStartOffset(), (int)(r.getEndOffset()-r.getStartOffset()+1), tmp, 0);
			out.write(tmp, 0, (int)(r.getEndOffset()-r.getStartOffset()+1));
		}
		tmp=null;

		//System.out.println("Get data okie..now to get checksum");

		if(out.size()<=(blkSize>0?Resources.HASH_BIN_LEN:0)){
			LOG.debug("no data found!");
			return null;
		}

		byte[] data=out.toByteArray();
		out=null;

		if(blkSize>0){
			List<byte[]> hashes=retrieveHashes(pgBlobChk, offset, blkSize, -1);
			if(hashes==null || hashes.size()==0)
				throw new SVDSException("Unable to retrieve slice checksum");

			byte[] combinedHash=SliceDigest.combineBlkHashes(hashes);
			System.arraycopy(combinedHash, 0, data, 0, combinedHash.length);
		}

		return data;
	}

	public static byte[] retrieve(CloudPageBlob pgBlob, CloudPageBlob pgBlobChk, int blkSize) throws Exception {
		ByteArrayOutputStream out=new ByteArrayOutputStream();

		if(!pgBlob.exists()) return null;

		//get list of pages with valid data
		List<PageRange> pges=pgBlob.downloadPageRanges();

		//if checksum is required then fill it with 0 first
		if(blkSize>0) for(int i=0;i<Resources.HASH_BIN_LEN; i++) out.write(0);

		byte[] tmp=new byte[0];
		for(PageRange r: pges) {
			if(r.getEndOffset()-r.getStartOffset()+1>tmp.length){
				tmp=null;
				tmp=new byte[(int) (r.getEndOffset()-r.getStartOffset()+1)];
			}
			pgBlob.downloadRange(r.getStartOffset(), (int)(r.getEndOffset()-r.getStartOffset()+1), tmp, 0);
			out.write(tmp, 0, (int)(r.getEndOffset()-r.getStartOffset()+1));
		}
		tmp=null;

		if(out.size()<=(blkSize>0?Resources.HASH_BIN_LEN:0)){
			LOG.debug("no data found!");
			return null;
		}

		//System.out.println("Get data okie..now to get checksum");

		byte[] data=out.toByteArray();
		out=null;

		if(blkSize>0){
			List<byte[]> hashes=retrieveHashes(pgBlobChk);
			if(hashes==null || hashes.size()==0)
				throw new SVDSException("Unable to retrieve slice checksum");

			byte[] combinedHash=SliceDigest.combineBlkHashes(hashes);
			System.arraycopy(combinedHash, 0, data, 0, combinedHash.length);
		}

		return data;
	}

	public static int retrieve(CloudPageBlob pgBlob, CloudPageBlob pgBlobChk, long offset, int len, int blkSize,
			byte[] data, int dataOffset) throws Exception {
		int dataIndex=dataOffset;

		if(!pgBlob.exists()) return 0;

		//get list of pages with valid data
		List<PageRange> pges=pgBlob.downloadPageRanges();

		//check if the offset is bigger than the available data, if yes return 0
		if(offset>pges.get(pges.size()-1).getEndOffset()+1)
			return (blkSize>0?Resources.HASH_BIN_LEN:0);

		//if checksum is required then set the offset index
		if(blkSize>0) dataIndex+=Resources.HASH_BIN_LEN;

		//check if the 1st page range is bigger than offset, if yes, increase the offset
		//within the byte array
		if(offset<pges.get(0).getStartOffset()){
			dataIndex+=pges.get(0).getStartOffset()-offset;
			len-=pges.get(0).getStartOffset()-offset;

			offset=pges.get(0).getStartOffset();
		}

		for(PageRange r: pges) {
			if(offset>r.getEndOffset()) continue;

			if(offset>=r.getStartOffset() && offset<=r.getEndOffset()){
				r.setStartOffset((int)offset);
			}

			if((r.getEndOffset()-r.getStartOffset()+1)>=len) r.setEndOffset(r.getStartOffset()+len-1);

			pgBlob.downloadRange(r.getStartOffset(), (int)(r.getEndOffset()-r.getStartOffset()+1), data, dataIndex);
			dataIndex+=(int)(r.getEndOffset()-r.getStartOffset()+1);

			if((r.getEndOffset()-r.getStartOffset()+1)>=len) break;

			len-=(r.getEndOffset()-r.getStartOffset()+1);
		}

		if(dataIndex==dataOffset || (blkSize>0 && dataIndex==dataOffset+Resources.HASH_BIN_LEN)){
			LOG.debug("no data found!");
			return 0;
		}

		//System.out.println("Get data okie..now to get checksum");

		if(blkSize>0){
			List<byte[]> hashes=retrieveHashes(pgBlobChk, offset, blkSize, len);
			if(hashes==null || hashes.size()==0)
				throw new SVDSException("Unable to retrieve slice checksum");

			byte[] combinedHash=SliceDigest.combineBlkHashes(hashes);
			System.arraycopy(combinedHash, 0, data, dataOffset, combinedHash.length);
		}

		return dataIndex-dataOffset;
	}

	public static void delete(CloudPageBlob pgBlob, CloudPageBlob pgBlobChk) throws Exception {
		pgBlob.delete();

		//deletes the checksum file, if any
		pgBlobChk.deleteIfExists();
	}

	public static void store(byte[] in, CloudPageBlob pgBlob, CloudPageBlob pgBlobChk, SliceDigestInfo md)
	throws Exception {
		writeBlob(in, pgBlob, 0, -1);

		//create the blk hash file
		if(md!=null){
			try{
				storeHashes(md.getBlkHashes(), pgBlobChk);
			}catch(Exception ex){
				//if write hash file, delete the slice such that it never exist;
				try {
					pgBlob.deleteIfExists();
				} catch (Exception e) { e.printStackTrace(); }

				ex.printStackTrace();
				throw ex;
			}
		}
	}

	public static void store(byte[] in, CloudPageBlob pgBlob, CloudPageBlob pgBlobChk, long offset, int length,
			SliceDigestInfo md) throws Exception {
		if(offset%IFileSliceStore.AZURE_MIN_PG_SIZE>0)
			throw new SVDSException("Offset must be 512 bytes aligned.");

		List<byte[]> chkArr=null;
		if(md!=null){
			SliceDigest smd=new SliceDigest(new SliceDigestInfo(md.getBlkSize(), md.getKey()));
			smd.update(in, 0, length);
			smd.finalizeDigest();
			if(!Resources.convertToHex(smd.getSliceChecksum()).equals(md.getChecksum())){
				//System.out.println("slice " + sliceName + "("+md.getBlkSize()+", "+md.getKey()+") cal checksum: " + Resources.convertToHex(smd.getSliceChecksum())
				//		+ " given checksum: " +md.getChecksum() + "\nData: " + Resources.concatByteArray(in, 0, length));
				throw new CorruptedSVDSException("Checksum does not match.");
			}

			chkArr=smd.getBlkHashes();
		}

		writeBlob(in, pgBlob, offset, length);

		if(md!=null){
			try{
				offset=offset/md.getBlkSize() * IFileSliceStore.AZURE_MIN_PG_SIZE;
				for(int i=0; i<chkArr.size(); i++){
					//System.out.println("write hash to slice " + sliceName +" at "+offset+": " 
					//		+ Resources.convertToHex(chkArr.get(i)));
					writeBlob(chkArr.get(i), pgBlobChk, offset,	Resources.HASH_BIN_LEN);

					offset+=IFileSliceStore.AZURE_MIN_PG_SIZE;
				}
			}catch(Exception ex){
				//if write hash file, delete the slice such that it never exist;
				try {
					pgBlob.deleteIfExists();
				} catch (Exception e) { e.printStackTrace(); }

				ex.printStackTrace();
				throw ex;
			}
		}
	}

	private static void writeBlob(byte[] in, CloudPageBlob pgBlob, long startOffset, int inLen) throws Exception{
		if(inLen==-1) inLen=in.length;

		if(!pgBlob.exists()){
			pgBlob.create(IFileSliceStore.AZURE_PGBLOB_MAX_SIZE);
			pgBlob.getProperties().setContentType("application/octet-stream");
		}

		byte[] data=in;
		//ensure the length to write is multiples of the min page size
		if(inLen%IFileSliceStore.AZURE_MIN_PG_SIZE>0) {
			//System.out.println(inLen);
			inLen=((inLen/IFileSliceStore.AZURE_MIN_PG_SIZE)+1)*IFileSliceStore.AZURE_MIN_PG_SIZE;

			//if the newly cal len is bigger than the original byte array, increase the byte array size
			if(inLen>in.length){
				data=new byte[inLen];
				//System.out.println(inLen + " " + data.length + " " + in.length);
				System.arraycopy(in, 0, data, 0, in.length);
			}
		}

		if(inLen<=IFileSliceStore.AZURE_MAX_PG_SIZE){
			//System.out.println(startOffset+" " + inLen);
			pgBlob.uploadPages(new ByteArrayInputStream(data), startOffset, inLen);
		}else{
			int offset=0;
			while(inLen>IFileSliceStore.AZURE_MAX_PG_SIZE){
				pgBlob.uploadPages(new ByteArrayInputStream(data, offset, IFileSliceStore.AZURE_MAX_PG_SIZE), startOffset, IFileSliceStore.AZURE_MAX_PG_SIZE);

				inLen-=IFileSliceStore.AZURE_MAX_PG_SIZE;
				startOffset+=IFileSliceStore.AZURE_MAX_PG_SIZE;
				offset+=IFileSliceStore.AZURE_MAX_PG_SIZE;
			}
			if(inLen>0){
				pgBlob.uploadPages(new ByteArrayInputStream(data, offset, inLen), startOffset, inLen);
			}
		}
	}

	public static void storeHashes(List<byte[]> in, CloudPageBlob pgBlob)
	throws Exception {
		int offset;
		for(int i=0; i<in.size(); i++){
			offset=i*IFileSliceStore.AZURE_MIN_PG_SIZE;
			//System.out.println("write hash: " + offset);

			writeBlob(in.get(i), pgBlob, offset, Resources.HASH_BIN_LEN);
		}
	}

	public static List<byte[]> retrieveHashes(CloudPageBlob pgBlob) throws Exception {
		List<byte[]> hashes=null;

		if(!pgBlob.exists()) return null;

		List<PageRange> pges=pgBlob.downloadPageRanges();
		if(pges==null || pges.size()==0) return null;

		hashes=new ArrayList<byte[]>();
		int rLen;
		byte[] tmp=null;
		for(PageRange r: pges){
			rLen=(int) (r.getEndOffset()-r.getStartOffset()+1);
			//as each blk checksum is stored in 1 pg (512 byte), cal how many
			//pages are in the return page range and get each blk checksum

			for(int i=0; i<rLen/IFileSliceStore.AZURE_MIN_PG_SIZE; i++){
				tmp=new byte[Resources.HASH_BIN_LEN];
				pgBlob.downloadRange(r.getStartOffset(), Resources.HASH_BIN_LEN, tmp, 0);
				hashes.add(tmp);

				r.setStartOffset(r.getStartOffset()+IFileSliceStore.AZURE_MIN_PG_SIZE);
			}
		}

		return hashes;
	}

	private static List<byte[]> retrieveHashes(CloudPageBlob pgBlob, long sliceOffset, int blkSize, int sliceLen) throws Exception{
		//based on the slice offset, calculate the offset to start reading from the checksum file
		int chkStartOffset=(int)(sliceOffset/blkSize) * IFileSliceStore.AZURE_MIN_PG_SIZE;

		List<byte[]> hashes=null;

		if(!pgBlob.exists()) return null;

		int chkEndOffset;

		if(sliceLen==-1){
			List<PageRange> pges=pgBlob.downloadPageRanges();
			if(pges==null || pges.size()==0) return null;
			chkEndOffset=(int) (pges.get(pges.size()-1).getEndOffset()+1);
		}else{
			chkEndOffset=chkStartOffset+((int)Math.ceil((float)sliceLen/blkSize)*IFileSliceStore.AZURE_MIN_PG_SIZE);
		}

		int start=chkStartOffset;
		byte[] tmp=null;
		hashes=new ArrayList<byte[]>();
		for(int i=0; i<(chkEndOffset-chkStartOffset)/IFileSliceStore.AZURE_MIN_PG_SIZE;i++){
			tmp=new byte[Resources.HASH_BIN_LEN];
			pgBlob.downloadRange(start, Resources.HASH_BIN_LEN, tmp, 0);
			hashes.add(tmp);

			start+=IFileSliceStore.AZURE_MIN_PG_SIZE;
		}

		return hashes;
	}
}
