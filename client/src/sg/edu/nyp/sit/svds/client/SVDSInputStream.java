package sg.edu.nyp.sit.svds.client;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.InfoDispersalFactory;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.*;
import sg.edu.nyp.sit.svds.metadata.*;

public class SVDSInputStream extends InputStream {
	public static final long serialVersionUID = 4L;
	
	private static final Log LOG = LogFactory.getLog(SVDSInputStream.class);
			
	//flags
	private boolean closed=false;
	private boolean eof=false;
	
	//inner class to transform the bytes
	private IInfoDispersal combiner=null; 

	//buffer to keep what is available to read
	private PipedInputStream sbufferIn=null;
	
	private Streamer streamer=null;
	
	//to store the file slices that are available for read, for recovery purposes
	private List<FileSlice> ava_slice=null;
	
	//buffer size in bytes
	private int bufferSize=0;
	
	//buffer for non-streaming read (to enhance performance to randomaccessfile)
	private byte nsbuffer[]=null;
	private int nsbufferAva=0, nsbufferCnt=0;
	
	//metadata contain the IDA required data
	private IdaInfo info=null;
	
	//contains the slices metadata
	private List<FileSlice> slices=null;
	
	private java.io.File localTmpFile=null;
	private RandomAccessFile localTmpFileStream=null;
	
	private boolean streaming=false;
	private boolean deleteTmpFile=true;
	
	private sg.edu.nyp.sit.svds.client.File file=null;
	private FileInfo fileI=null;
	
	public SVDSInputStream(sg.edu.nyp.sit.svds.client.File file, boolean streaming) throws SVDSException{
		if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS) 
				&& streaming && ClientProperties.getInt(ClientProperties.PropName.FILE_SUPPORT_MODE)==FileIOMode.NON_STREAM.value())
			throw new NotSupportedSVDSException("Conflicting file mode between client and slice store.");
		
		if(!file.exist())
			throw new SVDSException("File is not created.");
		
		if(file.isDirectory())
			throw new SVDSException("Illegal operation on non file object.");

		this.file=file;
		this.streaming=streaming;
		
		this.fileI=this.file.fInfo;
		this.slices=this.file.slices;
		
		this.info=fileI.getIda();
		//when a new input stream is created, always read from the start
		this.info.setDataOffset(0);
		
		//System.out.println("To verify checksum: " + fileI.verifyChecksum());
		
		//change mode is only applicable to SVDS
		if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS)){
			MasterTableFactory.getFileInstance().refreshChangeFileMode(fileI, fileI.getLockBy());
			if(fileI.isChgMode())
				throw new ChangeModeSVDSException("File is in the midst of mode change.");
		}
		
		try{
			MasterTableFactory.getFileInstance().accessFile(fileI, fileI.getLockBy());
			combiner = InfoDispersalFactory.getInstance();
			
			if(!streaming){
				localTmpFile = java.io.File.createTempFile((new Long((new Date()).getTime())).toString(), null);
				
				if(slices==null || slices.size()==0){
					nsbufferAva=-1;
					return;
				}
				
				initNonStreaming();
			}else{
				initStreaming();
			}
		}catch(RejectedSVDSException ex){
			throw ex;
		}catch(IncompatibleSVDSException ex){
			combiner=null;
			throw ex;
		}catch(Exception ex){
			combiner=null;
			ex.printStackTrace();
			throw new SVDSException(ex);
		}
	}
	
	//private constructor to be used by SVDSOutputStream only (non-streaming mode)
	SVDSInputStream(sg.edu.nyp.sit.svds.client.File file, java.io.File localTmpFile) throws SVDSException{
		if(file.slices==null || file.slices.size()==0)
			throw new SVDSException("No file slices available for reading.");
		
		if(file.isDirectory())
			throw new SVDSException("Illegal operation on non file object.");
		
		this.file=file;
		this.localTmpFile=localTmpFile;
		
		this.fileI=this.file.fInfo;
		this.slices=this.file.slices;
		
		this.info=fileI.getIda();
		this.info.setDataOffset(0);
		streaming=false;
		deleteTmpFile=false;
		
		try{ 
			combiner = InfoDispersalFactory.getInstance();
			
			initNonStreaming();
		} catch (Exception ex) {
			throw new SVDSException(ex);
		}finally{
			combiner=null;
		}
	}
	
	private void initNonStreaming() throws Exception{
		//check that slice len must not be more than int.max value bytes as in non-streaming
		//mode, the slices are stored temp into byte array before combine and the max elemenrs 
		//in a array is int.max value
		if(info.getSliceLength()>Integer.MAX_VALUE)
			throw new SVDSException("File too big to be read in non-streaming mode");
		
		//check if there are enough complete slices to read
		ava_slice=new ArrayList<FileSlice>();
		int completeCnt=0;
		for(FileSlice s: slices){
			if(s.isComplete()){
				completeCnt++;
				
				if(s.supportMode(FileIOMode.NON_STREAM))
					ava_slice.add(s);
			}
		}
		
		if(ava_slice.size()<info.getQuorum()){
			localTmpFile.delete();
			
			if(completeCnt-ava_slice.size()>=info.getQuorum())
				//file cannot be read because majority of the slice store in incompatible mode
				throw new IncompatibleSVDSException("Unable to retrieve file because storage location does not support non-streaming mode.");
			else
				throw new SVDSException("Unable to retrieve sufficent data to recover original file because the file is in recovery.");
		}
		
		try{
			readLocalFile();
		}finally{
			if(ava_slice!=null){
				ava_slice.clear();
				ava_slice=null;
			}
		}
	}
	
	private void initStreaming() throws SVDSException{
		//if it's a 0 byte file, just put end of file indicator
		if(slices==null || slices.size()==0){
			eof=true;
			return;
		}

		ava_slice=new ArrayList<FileSlice>();
		int completeCnt=0;
		for(FileSlice s: slices){
			//System.out.println("Slice " + s.getFileSliceInfo().getSliceName()+ " stored at "
			//		+ s.getFileSliceInfo().getServerId());
			if(s.isComplete()){
				completeCnt++;
				
				if(s.supportMode(FileIOMode.STREAM))
					ava_slice.add(s);
			}
		}
		
		if(ava_slice.size()<info.getQuorum()){
			if(completeCnt-ava_slice.size()>=info.getQuorum())
				//file cannot be read because majority of the slice store in incompatible mode
				throw new IncompatibleSVDSException("Unable to retrieve file because storage location does not support streaming mode.");
			else
				throw new SVDSException("Unable to retrieve sufficent data to recover original file because the file is in recovery.");
		}
		
		//bufferSize must be in multiples of blkSize of hashing & azure FS 512 byte aligned restriction
		//although the slices may not be stored in azure FS
		if(fileI.getBlkSize()<=0)
			bufferSize=(int) Resources.findCommonMultiple(IFileSliceStore.AZURE_MIN_PG_SIZE, 
					ClientProperties.getInt(ClientProperties.PropName.FILE_SLICE_BLK_SIZE));
		else
			bufferSize=fileI.getBlkSize();	//in bytes
		
		//System.out.println("buffer size: " + bufferSize + ", quorum: " + info.getQuorum());
		
		try{
			//limit the size of the internal buffer
			sbufferIn=new PipedInputStream(bufferSize * info.getQuorum());
			
			streamer=new Streamer(sbufferIn, 0);
			streamer.start();
		}catch(IOException ex){
			throw new SVDSException(ex);
		}
	}

	//destructor method when the object is collected by garbage collector
	protected void finalize() throws Throwable{
		close();
	}
	
	@Override
	public synchronized int read() throws IOException {
		if(closed)
			throw new IOException("Stream is closed.");
		
		int b;
		if(streaming)
			b = streamingRead();
		else
			b = nonStreamingRead();

		return b;
	}
	
	private int streamingRead() throws IOException{
		if(eof)
			return -1;
		
		if(streamer!=null && streamer.err!=null)
			throw streamer.err;
		
		try{
			return sbufferIn.read();
		}catch(EOFException ex){
			eof=true;
			return -1;
		}
	}
	
	private int nonStreamingRead() throws IOException{
		if(nsbufferAva==-1){
			return -1;
		}
		
		if(localTmpFileStream==null)
			throw new IOException("Error reading file.");
		
		
		if(nsbufferAva==0){
			nsbufferAva=nsbufferCnt=localTmpFileStream.read(nsbuffer);
			if(nsbufferAva==-1){
				return -1;
			}
		}
		
		//int pos=nsbufferCnt-nsbufferAva;
		//nsbufferAva--;
		return (int)nsbuffer[(nsbufferCnt-(nsbufferAva--))]& 0xff;
		//return (int)nsbuffer[pos] & 0xff;
	}
	
	public synchronized void seek(long pos) throws IOException{
		if(closed)
			throw new IOException("Stream is closed.");
		
		if(slices.size()==0){
			return;
		}
		
		if(pos<0)
			throw new IOException("Invaild positiion.");
		
		if(streaming)
			streamingSeek(pos);
		else
			nonStreamingSeek(pos);
	}
	
	private void streamingSeek(long pos) throws IOException{
		//stop the streamer thread and the buffers
		streamer.streamerStop=true;
		streamer=null;
		try{
			sbufferIn.close();
		}catch(IOException ex){}
		finally{
			sbufferIn=null;
		}

		//cal the "col" of the new position. if checksum is turned on, then "col" refers to the
		//blk, else "col" refers to the quorum.
		int col=(fileI.verifyChecksum() ? fileI.getBlkSize() : 1) * info.getQuorum();
		
		info.setDataOffset(pos-(pos%col));
		
		//start streaming again
		//clear away the current list and add the available slices again as there might be
		//cases where the currently read slices previously not added back to the list as the
		//thread is interrupted
		ava_slice.clear();
		for(FileSlice s: slices){
			if(s.isComplete() && !s.isLastReadError() && s.supportMode(FileIOMode.STREAM)){
				ava_slice.add(s);
			}
		}
		
		if(ava_slice.size()<info.getQuorum()){
			throw new IOException("Seek error due to insufficent data to recover original file.");
		}
		
		sbufferIn=new PipedInputStream(bufferSize * info.getQuorum());

		//System.out.println("slice offset: " + info.getDataOffset()/info.getQuorum());
		
		//streamer=new Streamer(sbufferIn, pos/col);
		streamer=new Streamer(sbufferIn, info.getDataOffset()/info.getQuorum());
		streamer.start();
		
		//because the combination will begin at the start of the "col", may need to read off
		//some data until to desired position
		for(int i=0; i<(pos%col); i++){
			if(read()==-1)
				break;
		}
	}
	
	private void nonStreamingSeek(long pos) throws IOException{
		nsbufferAva=0;
		
		localTmpFileStream.seek(pos);
	}
	
	public synchronized void close() throws IOException {
		if(closed)
			return;
		
		closed=true;
		
		if(streaming)
			streamingClose();
		else
			nonStreamingClose();
		
		info=null;
		slices=null;
		combiner=null;
	}
	
	private void nonStreamingClose(){
		try{
			if(localTmpFileStream!=null) localTmpFileStream.close();	
			if(localTmpFile!=null && deleteTmpFile) localTmpFile.delete();
			localTmpFile=null;
			localTmpFileStream=null;
			nsbuffer=null;
			nsbufferAva=-1;
		}catch(Exception ex){
			LOG.error(ex);
		}
	}
	
	private void streamingClose(){
		streamer.streamerStop=true;
		streamer=null;
		try{
			sbufferIn.close();
		}catch(IOException ex){}
		finally{
			sbufferIn=null;
		}
		ava_slice=null;
	}

	private void readLocalFile() throws SVDSException{
		//read all the file slices
		List<ReadSlice> readThreads=new ArrayList<ReadSlice>();
		byte[][] fileSlices=new byte[info.getQuorum()][];
		byte[] fileSliceIds=new byte[info.getQuorum()];
		int sliceCnt=0;
		
		AtomicInteger currStreamsCnt=new AtomicInteger(0);
		ReadSlice tmpSlice=null;
		
		//loop to read slices while there's enough to combine
		do{
			for(int cnt=sliceCnt;cnt<info.getQuorum(); cnt++){
				if(!ava_slice.isEmpty()){
					tmpSlice=new ReadSlice(ava_slice.remove(0), currStreamsCnt);
					readThreads.add(tmpSlice);
					currStreamsCnt.incrementAndGet();
					SVDSStreamingPool.stream(tmpSlice);
				}else{
					localTmpFile.delete();
					ava_slice=null;
					
					throw new SVDSException("Insufficent data to combine");
				}
			}
			tmpSlice=null;
			
			//checks if the file slices have finish reading, if not, wait for them to finish
			while(currStreamsCnt.get()>0){
				Thread.yield();
			}
	
			for(ReadSlice t: readThreads){
				//System.out.println(t.slice.getLastReadError());
				if(!t.slice.isLastReadError()){
					//if the read is not successful, don't add it into the array for 
					//combination process later on, note that the data array contains
					//the checksum if it is turn on
					fileSlices[sliceCnt]=t.data;
					fileSliceIds[sliceCnt]=(byte)t.slice.getSliceSeq();
					sliceCnt++;
				}
				
				t.slice=null;
				t.retrievedHash=null;
				t.data=null;
			}
			
			readThreads.clear();
		}while(sliceCnt<info.getQuorum());
		
		readThreads=null;
		currStreamsCnt=null;
		
		try{	
			//writes to the temp file
			//LOG.debug("local tmp file path: " + localTmpFile.getAbsolutePath());
			FileOutputStream out = new FileOutputStream(localTmpFile);
			
			//init buffer (def 4096)
			bufferSize=Resources.DEF_BUFFER_SIZE;
			nsbuffer=new byte[bufferSize];
			
			int len, combineSliceLen, maxSliceLen=bufferSize/info.getQuorum();
			int sliceOffset=0, offset=0, sliceLen=(fileI.verifyChecksum() ? (fileSlices[0].length-Resources.HASH_BIN_LEN) : fileSlices[0].length);
		
			byte[][] tmp=new byte[info.getQuorum()][maxSliceLen+1];
			
			while(offset<info.getDataSize()){
				info.setDataOffset(offset);
				combineSliceLen=(sliceOffset+maxSliceLen>sliceLen ? sliceLen-sliceOffset : maxSliceLen);
				
				for(int i=0; i<tmp.length; i++){
					tmp[i][0]=fileSliceIds[i];	
					System.arraycopy(fileSlices[i], (fileI.verifyChecksum()? (sliceOffset + Resources.HASH_BIN_LEN) : sliceOffset) 
							, tmp[i], 1, combineSliceLen);
				}
				
				len=combiner.combine(tmp, 0, combineSliceLen, info, nsbuffer, 0);
				
				out.write(nsbuffer, 0, len);
				
				offset+=len;
				sliceOffset+=combineSliceLen;
			}
			
			tmp=null;
			
			out.flush();
			out.close();
			
			localTmpFileStream=new RandomAccessFile(localTmpFile, "r");
		}catch(Exception ex){
			if(nsbuffer!=null)
				nsbuffer=null;
			
			ex.printStackTrace();
			LOG.error(ex);
			throw new SVDSException(ex);
		}
	}
	
	private class ReadSlice implements Runnable{
		private FileSlice slice=null;
		private byte[] data=null, retrievedHash=null;
		private long offset;
		private int len, actualLen=0, dataOffset;
		private SliceDigest slice_md=null;
		private AtomicInteger currStreamCnt=null;
		
		public ReadSlice(FileSlice slice, AtomicInteger currStreamCnt){	
			this.slice=slice;
			this.slice.resetReadStatus();
			this.currStreamCnt=currStreamCnt;
			
			len=-1;
			offset=0;
			dataOffset=0;
		}
		
		public ReadSlice(FileSlice slice, long offset, int len, byte[] data, int dataOffset,
				SliceDigest slice_md, AtomicInteger currStreamCnt){
			this.slice=slice;
			this.slice.resetReadStatus();
			this.currStreamCnt=currStreamCnt;
			
			this.offset=offset;
			this.len=len;
			this.data=data;
			this.dataOffset=dataOffset;
			
			this.slice_md=slice_md;
		}
		
		public void run(){
			//System.out.println(slice.getSliceSeq() + " read offset " + offset + " start");
			
			try{
				if(len==-1){
					data=slice.read(fileI.getBlkSize());
					if(data==null || data.length==0){
						//System.out.println("Empty");
						return;
					}
				}else{
					if((actualLen=slice.read(offset, len, fileI.getBlkSize(), data, dataOffset))==0)
						return;
				}
				
				if(fileI.verifyChecksum()){
					if ((len==-1 && data.length<Resources.HASH_BIN_LEN) ||
							(len!=-1 && actualLen<Resources.HASH_BIN_LEN)){
						throw new CorruptedSVDSException("Slice read corrupted");
					}
					
					retrievedHash=new byte[Resources.HASH_BIN_LEN];
					System.arraycopy(data, 0, retrievedHash, 0, Resources.HASH_BIN_LEN);
					
					verifySliceChecksum();
				}
			}catch(CorruptedSVDSException ex){
				ex.printStackTrace();
				slice.flagLastReadError(FileSlice.Codes.CORRUPT);
				
				data=null;
				retrievedHash=null;
				actualLen=-1;
			}catch(SVDSException ex){
				slice.flagLastReadError(FileSlice.Codes.ERR);
				
				data=null;
				retrievedHash=null;
				actualLen=-1;
			}finally{
				currStreamCnt.decrementAndGet();
				currStreamCnt=null;
			}
			
			//System.out.println(slice.getSliceSeq() + " read offset " + offset + " end");
		}
		
		private void verifySliceChecksum() throws SVDSException{
			if(len==-1)
				slice_md=new SliceDigest(new SliceDigestInfo(fileI.getBlkSize() , fileI.getKeyHash()));
			
			long sliceLen=info.getSliceLength();
			
			//assume offset given is multiples of blk size
			slice_md.setOffset(offset, null);
			for(int i=(Resources.HASH_BIN_LEN+dataOffset); i<(len==-1 ? 
					(sliceLen+Resources.HASH_BIN_LEN+dataOffset) : 
						(Math.min(sliceLen-offset+Resources.HASH_BIN_LEN, actualLen)+dataOffset)); i++){
				
				slice_md.update(data[i]);
			}
			
			slice_md.finalizeDigest();
			
			/*
			System.out.print("retrieved hash " +slice.getSliceSeq()+": ");
			Resources.printByteArray(retrievedHash);
			System.out.print("cal hash " +slice.getSliceSeq()+":");
			Resources.printByteArray(slice_md.getSliceChecksum());
			System.out.print("Data"+slice.getSliceSeq()+": " );
			Resources.printByteArray(data);
			*/

			if((len==-1 && !Arrays.equals(retrievedHash, slice_md.getSliceChecksum())) ||
					(len!=-1 && !Arrays.equals(retrievedHash, SliceDigest.combineBlkHashes(slice_md.getBlkHashes(offset, len)))))
				throw new CorruptedSVDSException("Slice checksum does not match.");
		}
	}
	
	private class Streamer extends Thread {
		IOException err=null;
		private PipedOutputStream out=null;
		private long sliceOffset=0;
		volatile boolean streamerStop = false;
		
		public Streamer(PipedInputStream in, long sliceOffset) throws IOException{
			out=new PipedOutputStream(in);
			this.sliceOffset=sliceOffset;
		}

		public void run() {	
			List<ReadSlice> readThreads=null;
			byte[][] fileSlices=null;
			byte[] data=null;
			SliceDigest[] slice_mds=null;
			AtomicInteger currStreamsCnt=null;
			
			try{
				readThreads=new ArrayList<ReadSlice>();
				//System.out.println("verify checksum: " + fileI.verifyChecksum());
				fileSlices=new byte[info.getQuorum()][(fileI.verifyChecksum() ? (bufferSize+Resources.HASH_BIN_LEN) : (bufferSize+1))];
				data=new byte[bufferSize*info.getQuorum()];
				slice_mds=new SliceDigest[info.getQuorum()];
				
				//in IDA implementation, each slice should have the same length when read from the same offset
				int sliceCnt, sliceLen=0, dataLen;
				
				long fileOffset=info.getDataOffset();
				
				currStreamsCnt=new AtomicInteger(0);
				ReadSlice tmpSlice=null;
				
				if(fileI.verifyChecksum()){
					SliceDigestInfo mdInfo=new SliceDigestInfo(fileI.getBlkSize() , fileI.getKeyHash());
					for(int i=0; i<slice_mds.length; i++)
						slice_mds[i]=new SliceDigest(mdInfo);
				}

				do{
					if(streamerStop)
						break;
					
					//loop to read slices until there's enough to combine
					sliceCnt=0;
					do{					
						//TODO: For the slices that have read error some time ago, can try to read again
						for(int cnt=sliceCnt;cnt<info.getQuorum(); cnt++){
							if(!ava_slice.isEmpty()){
								tmpSlice=new ReadSlice(ava_slice.remove(0), sliceOffset, bufferSize, fileSlices[cnt], 
										(fileI.verifyChecksum() ? 0 : 1), slice_mds[cnt], currStreamsCnt);
								readThreads.add(tmpSlice);
								currStreamsCnt.incrementAndGet();
								SVDSStreamingPool.stream(tmpSlice);
							}else{
								err = new IOException("Insufficent data to combine");
								return;
							}
						}
						tmpSlice=null;

						//checks if the file slices have finish reading, if not, wait for them to finish
						while(currStreamsCnt.get()>0){
							Thread.yield();
						}

						for(ReadSlice t: readThreads){
							if(!t.slice.isLastReadError()){
								//if the read is not successful, don't add it into the array for 
								//combination process later on, note that the data array contains
								//the checksum if it is turn on
								fileSlices[sliceCnt][(fileI.verifyChecksum() ? (Resources.HASH_BIN_LEN-1) : 0)]=(byte)t.slice.getSliceSeq();
								sliceLen=t.actualLen-(fileI.verifyChecksum() ? Resources.HASH_BIN_LEN : 0);
								sliceCnt++;

								//put back to the list of ava slice to read from
								ava_slice.add(t.slice);
							}

							t.slice=null;
							t.retrievedHash=null;
							t.data=null;
						}

						readThreads.clear();
					}while(sliceCnt<info.getQuorum());

					//if the prev read happen to read to end of file, then current read will have
					//no data, so consider done
					if(sliceLen==0)
						break;

					dataLen=combiner.combine(fileSlices, (fileI.verifyChecksum() ? (Resources.HASH_BIN_LEN-1) : 0), sliceLen, info, data, 0);

					//put the data into the pipe
					out.write(data, 0, dataLen);
					out.flush();
					
					sliceOffset+=sliceLen;
					info.setDataOffset((fileOffset+=dataLen));
				}while(sliceLen==bufferSize);
				
				//done with reading the file, close the buffer
				out.close();
			}catch(IOException ex){
				ex.printStackTrace();
				err=ex;
			}catch(Exception ex){
				ex.printStackTrace();
				err=new IOException(ex);
			}finally{
				out=null;
				
				fileSlices=null;
				data=null;
				slice_mds=null;
				readThreads=null;
				currStreamsCnt=null;
			}

			//System.out.println("Streamer thread end");
		}
	}
}
