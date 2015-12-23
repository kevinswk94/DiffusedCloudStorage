package sg.edu.nyp.sit.svds.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Collections;
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
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.ChangeModeSVDSException;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.exception.IncompatibleSVDSException;
import sg.edu.nyp.sit.svds.exception.LastRequestSVDSException;
import sg.edu.nyp.sit.svds.exception.LockedSVDSException;
import sg.edu.nyp.sit.svds.exception.NotFoundSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class SVDSOutputStream extends OutputStream {
	public static final long serialVersionUID = 5L;
	
	private static final Log LOG = LogFactory.getLog(SVDSOutputStream.class);
			
	private boolean closed=false;
	
	//inner class to transform the bytes
	private IInfoDispersal transformer=null;
	
	//inner class to refresh the lock periodically so the master server knows that 
	//the file is still being actively opened
	private LockRefresher lck=null;
	//flag to indicate if there's an error with locking
	private SVDSException lckErr=null;
	
	//buffer size in bytes
	private int bufferSize=0;
	
	private byte buffer[]=null;
	private int bufferCnt=0;
	
	//metadata contain the IDA required data
	private IdaInfo info=null;
	
	//contains the slices metadata
	private List<FileSlice> slices=null;
	
	private java.io.File localTmpFile=null;
	private RandomAccessFile localTmpFileStream=null;
	
	private boolean streaming=false;
	
	private sg.edu.nyp.sit.svds.client.File file=null;
	private FileInfo fileI=null;
	
	//To keep track of the bytes written, aka file size for streaming mode
	private long fileOffset=0L;
	private long fileLength=0L;
	
	private IMasterFileTable mt=null;
	private SliceDigest[] slice_mds=null;
	
	private long sliceOffset=0;
	
	private AtomicInteger currStreamsCnt=null;
	private boolean newFile;
	
	private final int streamRetryLimit=10;
	private final int maxStreamThread=15;
	private boolean streamIsClosing = false;
	
	public SVDSOutputStream(sg.edu.nyp.sit.svds.client.File file, boolean streaming) throws SVDSException{
		if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS) 
				&& streaming && ClientProperties.getInt(ClientProperties.PropName.FILE_SUPPORT_MODE)==FileIOMode.NON_STREAM.value())
			throw new NotSupportedSVDSException("Conflicting file mode between client and slice store.");
		LOG.debug("SVDFSOutputStream constructore called");	
		if(!file.exist())
			throw new SVDSException("File is not created.");
			
		if(file.isDirectory())
			throw new SVDSException("Illegal operation on non file object.");

		this.file=file;
		this.streaming=streaming;
		
		this.fileI=this.file.fInfo;
		this.slices=this.file.slices;
		this.info=fileI.getIda();
		this.fileLength=this.file.getFileSize();
		
		//System.out.println("To verify checksum: " + fileI.verifyChecksum());
		
		//attempt to lock the file for writing (only for files that exist at the master server)
		//if there's any error, exception will be thrown
		mt=MasterTableFactory.getFileInstance();
		
		//change mode is only applicable to SVDS
		if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS)){
			mt.refreshChangeFileMode(fileI, fileI.getLockBy());
			if(fileI.isChgMode())
				throw new ChangeModeSVDSException("File is in the midst of mode change.");
		}
		
		mt.lockFileInfo(fileI, fileI.getLockBy());

		//start the lock refresher
		lck=new LockRefresher();
		lck.start();

		try{
			transformer = InfoDispersalFactory.getInstance();

			if(!streaming)
				initNonStreaming();
			else
				initStreaming();
		}catch(IncompatibleSVDSException ex){
			//stop the lock refresher
			lck.interrupt();
			try {lck.join();} catch (InterruptedException e) {}
			mt.unlockFileInfo(fileI, fileI.getLockBy());
			
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			//stop the lock refresher
			lck.interrupt();
			try {lck.join();} catch (InterruptedException e) {}
			mt.unlockFileInfo(fileI, fileI.getLockBy());
			throw new SVDSException("Error accessing file.");
		}
	}
	
	private void initNonStreaming() throws Exception{
		//if not new file write, check that all slice stores of ALL slices can support 
		//non-streaming write, else throw exception
		if(slices.size()>0){
			for(FileSlice fs: slices){
				if(!fs.supportMode(FileIOMode.NON_STREAM))
					throw new IncompatibleSVDSException("Storage location does not support non-streaming.");
			}
		}
		
		localTmpFile = java.io.File.createTempFile((new Long((new Date()).getTime())).toString(), null);
		//java.io.File tempDir = new java.io.File("c:/temp2");
		
		//localTmpFile = java.io.File.createTempFile(file.getFilename(), null, tempDir);
		LOG.debug("tmpfile created:"+localTmpFile.getAbsolutePath());
		if(file.slices!=null && file.slices.size()>0){
			SVDSInputStream in=new SVDSInputStream(file, localTmpFile);
			
			in.close();
			in=null;
		}
	
		localTmpFileStream=new RandomAccessFile(localTmpFile, "rwd");
		
		//creates the buffer for writing
		bufferSize=Resources.DEF_BUFFER_SIZE;
		buffer=new byte[bufferSize];
	}
	
	private void initStreaming() throws Exception{	
		//buffer size is the blk size and it must be in multiples of quorum to prevent additional
		//padding
		bufferSize=(fileI.getBlkSize()<=0 ? ClientProperties.getInt(ClientProperties.PropName.FILE_SLICE_BLK_SIZE)
						: fileI.getBlkSize());
		
		//ensure the buffer size is multiples of quorum and the azure FS 512 byte aligned limit, 
		//if not, round down to the nearest multiples
		if(bufferSize%info.getQuorum()>0 || bufferSize%IFileSliceStore.AZURE_MIN_PG_SIZE>0){
			bufferSize=(int) Resources.findCommonMultiple(info.getQuorum(), 
					IFileSliceStore.AZURE_MIN_PG_SIZE, bufferSize);
			if(fileI.getBlkSize()>0) fileI.setBlkSize(bufferSize);
		}
		
		//System.out.println("Blk size " + fileI.getBlkSize());
		
		//generate slice info from master server only if the file is new
		if(slices.size()==0) {
			generateSlices();
			newFile=true;
		}else{
			//check that there are enough slices to write to, irregardless if the slice is
			//in recovery mode
			if(slices.size()<info.getQuorum())
				throw new SVDSException("Unable to continue writing to incomplete file.");
			
			if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE)
					.equals(ClientProperties.CLIENT_MODE_PVFS) && slices.size()==info.getQuorum()){
				//in PVFS, since no segments are generated, so check that once no of slices only meet the min no to
				//combine, force the file to be opened in non streaming mode so that all slices can be regenerated
				streaming=false;
				initNonStreaming();
				return;
			}else{
				//check that all slice stores of ALL slices can support streaming write, else
				//throw exception
				for(FileSlice fs: slices){
					if(!fs.supportMode(FileIOMode.STREAM))
						throw new IncompatibleSVDSException("Storage location does not support streaming.");
				}
				Collections.sort(slices);
				
				newFile=false;
			}
		}

		//each slice length must be the blk size, cal the data buffer size
		int bufferLen=bufferSize*info.getQuorum();
		buffer=new byte[bufferLen];
		
		if(fileI.verifyChecksum()){
			slice_mds=new SliceDigest[slices.size()];
			//all slice digest will share the same slice digest info cos only will use the blk size
			//and key hash prop
			SliceDigestInfo mdsInfo=new SliceDigestInfo(fileI.getBlkSize(), fileI.getKeyHash());
			for(int i=0; i<slice_mds.length; i++){
				slice_mds[i]=new SliceDigest(mdsInfo);
				
				if(fileI.getFileSize()>0)
					slice_mds[i].setBlkHashes(slices.get(i).getBlkHashes());
			}
		}
		
		currStreamsCnt=new AtomicInteger(0);
		for(FileSlice fs: slices)
			fs.resetWriteStatus();
	}
	
	private void generateSlices() throws SVDSException{
		if(slices.size()<info.getShares()){
			//find a list of missing slice seq
			int seq[]=new int[info.getShares()-slices.size()];
			int seq_index=0;
			boolean isFound;
			for(int i=0; i<info.getShares(); i++){
				isFound=false;
				for(FileSlice f: slices)
					if(f.getSliceSeq()==i)
						isFound=true;
				
				if(!isFound){
					seq[seq_index]=i;
					seq_index++;
				}
			}
			
			//num of existing less than array of input streams
			//create extra file slices object
			List<FileSliceInfo> tmp = mt.generateFileSliceInfo(file.getNamespace(), 
					info.getShares()-slices.size(), 
					(streaming?FileIOMode.STREAM:FileIOMode.NON_STREAM),
					fileI.getLockBy());
			
			if(tmp==null || tmp.size()==0)
				throw new NotFoundSVDSException("Unable to generate slice metadata.");
			
			for(int i=0; i<tmp.size(); i++){
				FileSliceInfo fsi=tmp.get(i);
				
				fsi.setSliceSeq(seq[i]);
				fileI.addSlice(fsi);
				slices.add(new FileSlice(fsi, fileI.getLockBy()));
			}
			
			tmp.clear();
			tmp=null;
			seq=null;
		}
		
		Collections.sort(slices);
	}
	
	//destructor method when the object is collected by garbage collector
	protected void finalize() throws Throwable{
		forceClose();
	}
	
	public synchronized void setEOF(long end) {
		fileOffset=end;
		fileI.setFileSize(fileLength);
	}
	
	@Override
	public synchronized void write(int b) throws IOException {
		if(lckErr!=null){
			forceClose();
			throw new IOException(lckErr);
		}
		
		if(closed)
			throw new IOException("Stream is closed.");
		
		if(streaming)
			streamingWrite(b);
		else
			nonStreamingWrite(b);
		
		fileOffset++;
		if(fileOffset>fileLength){
			fileLength=fileOffset;
		}
		
		//update the file size in the file object for every write
		//the data size in the ida info object will also be updated
		fileI.setFileSize(fileLength);
	}
	
	private void nonStreamingWrite(int b) throws IOException{
		if(bufferCnt>=bufferSize){
			localTmpFileStream.write(buffer);
			bufferCnt=0;
		}
		
		buffer[bufferCnt]=(byte)b;
		bufferCnt++;
	}
	
	private void streamingWrite(int b) throws IOException{
		try{
			//if offset bigger than file length, pad with spaces until
			//the desired offset
			if(fileOffset>fileLength){
				for(int cnt=0; cnt<(fileOffset-fileLength); cnt++){
					if(bufferCnt>=buffer.length)
						streamingSplit();
					
					buffer[bufferCnt]=' ';
					bufferCnt++;
				}
			}
			
			if(bufferCnt>=buffer.length)
				streamingSplit();
			
			buffer[bufferCnt]=(byte)(b & 0xff);
			bufferCnt++;
		}catch(Exception ex){
			LOG.error(ex);
			throw new IOException(ex);
		}
	}
	
	private void streamingSplit() throws IOException{		
		int dataLen;
		
		//System.out.println("buffer size "+bufferSize + " buffer len " + buffer.length);
		
		//the splitted data is stored in a new byte array each time the method is invoked
		//because there might be situations where the previous threads have not finished
		//sending to the slice stores, so if the byte array is reused, the data in the 
		//previous threads will get overwritten
		byte[][] bufferOut=new byte[info.getShares()][bufferSize];
		
		//System.out.print("Data (byte): ");
		//Resources.printByteArray(buffer, 0, bufferCnt);
		//System.out.println("Data (string): " + new String(buffer, 0, bufferCnt));
		
		try{dataLen=transformer.split(buffer, 0, bufferCnt, info, bufferOut, 0, slice_mds);}
		catch(IDAException ex){
			throw new IOException(ex);
		}
		
		//System.out.println("data len " + dataLen);
		
		/*
		for(int i=0; i<info.getShares(); i++){
			System.out.print("Slice " + i+": ");
			Resources.printByteArray(bufferOut[i], 0, dataLen);
		}
		*/

		SliceDigestInfo mdInfo=null;

		for(int i=0; i<slices.size(); i++){
			currStreamsCnt.incrementAndGet();
			if(fileI.verifyChecksum()){
				//cal the blk digest, cannot do the in thread because multiple thread of a slice
				//might corrupt the digest
				slice_mds[i].finalizeDigest();
				
				//create new info object because each stream thread should access its own blk checksum
				mdInfo=null;
				mdInfo=new SliceDigestInfo(fileI.getBlkSize(), fileI.getKeyHash());
				//get the last blk hash generated
				mdInfo.setChecksum(slice_mds[i].getBlkHashes(sliceOffset));
				mdInfo.setSliceDigest(slice_mds[i]);
			}
			
			//do not care if the previous write has completed, just concurrent write to the slice
			//To prevent the pool from creating too much threads in case the network is congested,
			//set a limit on how much threads can be run for each SVDSOutput stream object, if exceeded
			//have to wait until some threads complete before can continue
			while(currStreamsCnt.get()>=maxStreamThread){
				Thread.yield();
			}
			
			SVDSStreamingPool.stream(new StreamerSlice(slices.get(i), sliceOffset, bufferOut[i], dataLen
					, mdInfo));
		}
		
		//need to let it finish running for the first time as in the event of
		//new file, the file slices need to be created before subsequent write can follow
		if(newFile){
			while(currStreamsCnt.get()>0){
				Thread.yield();
			}
			newFile=false;
		}
		
		sliceOffset+=dataLen;
		
		//reset buffer
		bufferCnt=0;
	}

	public synchronized void seek(long pos) throws IOException{
		if(closed)
			throw new IOException("Stream is closed.");
		
		if(pos<0)
			throw new IOException("Invaild positiion.");
		
		if(streaming)
			streamingSeek(pos);
		else
			nonStreamingSeek(pos);
	}
	
	private int readFile(long fileOffset, byte[] out, int len) throws IOException{
		if (out==null || out.length<len)
			throw new IOException("Array index out of bounds.");
		
		int dataIndex=0, index=-1, sLen=len/info.getQuorum();
		byte[][] data=new byte[info.getQuorum()][sLen+1];
		FileSlice fs;
		
		long sliceOffset=(fileOffset-(fileOffset%len))/info.getQuorum();
		
		//System.out.println("slice offset: " + sliceOffset);
		//System.out.println("slice length: " + len);
		
		while(dataIndex<info.getQuorum()){
			index++;
			
			if(index>=slices.size())
				throw new IOException("Insufficient slices to read from.");
			
			fs=slices.get(index);
			if(!fs.isComplete() || !fs.supportMode(FileIOMode.STREAM))
				continue;
			
			try{
				if((sLen=fs.read(sliceOffset, (len/info.getQuorum()), 0, data[dataIndex], 1))<=0)
					continue;
				
				//System.out.print("readFile slice " + fs.getSliceSeq() + ": ");
				//Resources.printByteArray(data[dataIndex], 0, sLen);
				
				data[dataIndex][0]=(byte)fs.getSliceSeq();
				dataIndex++;
			}catch(SVDSException ex){
				continue;
			}
		}
		
		try{
			//set the offset
			this.info.setDataOffset(fileOffset-(fileOffset%len));
			return transformer.combine(data, 0, sLen, info, out, 0);
		}catch(IDAException ex){
			throw new IOException(ex);
		}
	}
	
	private void streamingSeek(long pos) throws IOException{
		if(fileOffset==pos){
			//System.out.println("Current position is the same as seek.");
			return;
		}
		
		if(pos>fileLength && fileOffset==fileLength){
			//System.out.println("Seek position is bigger than file length and current position is at end of file.");
			fileOffset=pos;
			return;			
		}
		
		long oriFileOffset=fileOffset;
		//the length to read depends if checksum is on, if it is then it would be the
		//blk size, else it 1 byte which will make up the "column"
		int rLen=(fileI.verifyChecksum() ? fileI.getBlkSize() : 1) * info.getQuorum();
		
		//System.out.println("read len: " + rLen);
		
		//copy what the current buffer have now so that if the seek fails later, can
		//revert to original position
		byte[] b4SeekData=new byte[rLen];
		System.arraycopy(buffer, (int)(bufferCnt-(fileOffset%rLen)), b4SeekData, 0, rLen);
		
		//System.out.println("buffer cnt: " + nsbufferCnt);
		
		byte[] col=new byte[rLen];
		int colLen;
		
		//System.out.println("curr offset: " + fileOffset);
		//System.out.println("curr len: " + fileLength);
		
		//if the current offset is already at the end of the file, or finish
		//nicely at the end of the "column" (if checksum is not turn on) or end of 
		//blk (if checksum is turn on), then no need to get the remaining bytes
		//cos there's none to get
		if(fileOffset!=fileLength && fileOffset%rLen>0){
			colLen=readFile(fileOffset, col, rLen);
			for(int i=(int)(fileOffset%rLen); i<colLen; i++){
				write((int)col[i]);
			}
		}
		
		//since the new position is bigger than the file length and the "column"/blk of 
		//the current offset the same as "column"/blk of the end of file, client can 
		//continue to write from end of file
		if((fileOffset/rLen)==(fileLength/rLen) && pos>=fileLength && fileOffset%rLen!=0){
			//System.out.println("seek pos at the end");
			//fileOffset=fileLength;
			fileOffset=pos;
			return;
		}
		
		//finish current write
		if(bufferCnt>0)
			streamingSplit();
		
		while(currStreamsCnt.get()>0)
			Thread.yield();
		
		//clear buffers
		bufferCnt=0;
		
		//end of processing current write
//-------------------------------------------------------------------------------------------
		//start of processing write from new position
		
		//if the new position is more than file size, then set offset to file size
		long offset=(pos>fileLength? fileLength : pos);
		IOException iex=null;
		
		try{
			//read the "column"/blk the new position is in
			colLen=readFile(offset, col, rLen);
		}catch(IOException ex){
			iex=ex;
			
			//if error in reading, revert back to original pos and let
			//code continue
			offset=oriFileOffset;
			colLen=(int)(offset%rLen);
			System.arraycopy(b4SeekData, 0, col, 0, rLen);
		}
		
		//System.out.println("read len s:" + colLen);		
		
		//set the offset to start the write
		if(fileI.verifyChecksum()){
			//need to set the offset of the slice digest to the start of the blk
			//where the new pos is.
			try{
				long sOffset=offset/info.getQuorum();
				if(sOffset%fileI.getBlkSize()>0){
					sOffset=sOffset-(sOffset%fileI.getBlkSize());
					for(SliceDigest smd: slice_mds){
						smd.setOffset(sOffset);
					}
					//System.out.println("new seek slice offset " + sOffset);
				}
			}catch(SVDSException ex){
				LOG.debug(ex);
				if(iex!=null)
					iex=new IOException(ex);
			}
		}
		
		fileOffset=offset-(offset%rLen);
		sliceOffset=fileOffset/info.getQuorum();
		//System.out.print("\nData prefix: ");
		//write some bytes from the read data up to the position first
		for(int i=0; i<(offset%rLen); i++){
			if(colLen<i)
				break;
			
			//System.out.print((int)col[i]+" ");
			write((int)col[i]);
		}
		
		//if there is any error encountered earlier, throw it now
		if(iex!=null)
			throw iex;
		
		fileOffset=pos;
		
		//System.out.println("SEEK DONE");
	}
	
	private void nonStreamingSeek(long pos) throws IOException{
		fileOffset=pos;
		
		//write off whatever has been written to the buffer & reset the cnt
		if(bufferCnt>0){
			localTmpFileStream.write(buffer, 0, bufferCnt);
			bufferCnt=0;
		}
		
		localTmpFileStream.seek(pos);
	}
	
	private void forceClose(){
		if(closed)
			return;
		
		closed=true;
		
		if(streaming){
			//wait for prev writes to finish
			while(currStreamsCnt.get()>0)
				Thread.yield();
		}else{
			try{ localTmpFileStream.close(); }catch(IOException ex){}
			localTmpFile.delete();
			localTmpFile=null;
		}
		
		buffer=null;
		bufferCnt=0;
		currStreamsCnt=null;
		
		//stop the lock refresher
		if(lck!=null){
			lck.interrupt();
			try { lck.join(); } catch (InterruptedException e) {}
		}
			
		try{ mt.unlockFileInfo(fileI, fileI.getLockBy()); }
		catch(SVDSException ex){
			LOG.error(ex);
		}
		
		info=null;
		fileI=null;
		slices=null;
		file=null;
		transformer=null;	
		lck=null;
		mt=null;
	}
	
	public synchronized void close() throws IOException {
		
		if(closed){
			return;
		}
		
		if(fileI.getFileSize()==0){
			LOG.debug("Nothing is written. Force close.");
			file.slices.clear();
			fileI.setSlices(null);
			forceClose();
			return;
		}
		streamIsClosing=true;
		try{
			if(streaming){
				streamingClose();
			}else{
				nonStreamingClose();
			}

			//remove the slices that has error when writing, prevent from writing to the
			//master table. Slices that has segments are not considered to have error
			//and will still be sent to the master table for recovery
			int i=0;
			while(i<slices.size()){
				if(slices.get(i).isLastWriteError()){
					//if there is error writting, delete the actual file slice along
					//with any of its segments
					slices.get(i).delete();
					fileI.removeSlice(slices.get(i).getSliceSeq());
					slices.remove(i);
					continue;
				}
				i++;
			}
			
			//if slices after closing is less than required from ida info, then
			//file write has failed completely, therefore throw error
			if(slices.size()<info.getQuorum())
				throw new IOException("File write fail.");

			mt.updateFileInfo(fileI, fileI.getLockBy());
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				throw new IOException(RejectedSVDSException.PROXY+"");
			}
		}catch(LastRequestSVDSException ex){
			
		}catch(SVDSException ex){
			ex.printStackTrace();
			LOG.error(ex);
			throw new IOException(ex.getMessage());
		}finally{
			closed=true;
			
			//stop the lock refresher
			if(lck!=null){
				lck.interrupt();
				try { lck.join(); } catch (InterruptedException e) {}
			}
		
			try{ mt.unlockFileInfo(fileI, fileI.getLockBy()); }
			catch(LastRequestSVDSException ex){}
			catch(SVDSException ex){LOG.error(ex);}
			
			//also remove the segments that have been sent over
			for(FileSliceInfo fsi: fileI.getSlices()){
				if(fsi.hasSegments()){
					fsi.setSliceRecovery(true);
					fsi.clearSegments();
				}
			}

			info=null;
			fileI=null;
			slices=null;
			file=null;
			transformer=null;
			lck=null;
			mt=null;
			slice_mds=null;
			buffer=null;
			currStreamsCnt=null;
			streamIsClosing = false;
		}
	}
	
	private void nonStreamingClose() throws SVDSException{
		
		//List<InputStream> inStreams=null;
		byte[][] sliceData=null;

		try{
			if(bufferCnt>0)
				localTmpFileStream.write(buffer, 0, bufferCnt);
			
			localTmpFileStream.close();
			//info.setDataSize(localTmpFile.length());
			
			long sliceLen=info.getSliceLength();
			if(sliceLen>Integer.MAX_VALUE){
				//do something to handle cos max elements in array cannot be more than Integer.MAX_VALUE
				//currently throw exception
				throw new SVDSException("File is too big in non-streaming mode.");
			}
			
			//if the current no of slices is not enough, can generate new ones to fill in
			//because of the local write, the tmp file would have contain the entire data
			//to enable a new complete slice generation.
			//slices generated will be in sequence and sequence of existing slices are preserved.
			generateSlices();
			
			if(fileI.verifyChecksum()){
				//check that the blk size is in multiples of quorum and azure FS 512 byte aligned
				 if(fileI.getBlkSize()%info.getQuorum()>0 || 
						fileI.getBlkSize()%IFileSliceStore.AZURE_MIN_PG_SIZE>0)
					fileI.setBlkSize((int) Resources.findCommonMultiple(info.getQuorum(), 
							IFileSliceStore.AZURE_MIN_PG_SIZE, fileI.getBlkSize()));
				 
				 
				slice_mds=new SliceDigest[slices.size()];
				for(int i=0; i<slice_mds.length; i++){
					slice_mds[i]=new SliceDigest(new SliceDigestInfo(fileI.getBlkSize(), fileI.getKeyHash()));
				}
			}
			
			sliceData=new byte[info.getShares()][(int)sliceLen];
			//the buffer needs to be in multiples of the quorum else there will be extra padding
			byte[] tmp=new byte[Resources.DEF_BUFFER_SIZE-(Resources.DEF_BUFFER_SIZE%info.getQuorum())];
			int tmpLen, sliceOffset=0;
			FileInputStream in=new FileInputStream(localTmpFile);
			
			while((tmpLen=in.read(tmp))!=-1){
				//System.out.println(tmpLen);
				tmpLen=transformer.split(tmp, 0, tmpLen, info, sliceData, sliceOffset, slice_mds);
				//System.out.println(tmpLen);
				sliceOffset+=tmpLen;
			}
			tmp=null;
			in.close();
		}catch(NotFoundSVDSException ex){
			//as long as the slices meet the qurom allow to go thru
			if(slices.size()<info.getQuorum())
				throw ex;
		}catch(Exception ex){
			ex.printStackTrace();
			LOG.error(ex);
			throw new SVDSException(ex);
		}
		
		currStreamsCnt=new AtomicInteger(slices.size());
		for(int i=0; i<slices.size(); i++){
			//update the slice seq (zero based)
			//slices.get(i).setSliceSeq(i);
			
			slices.get(i).resetWriteStatus();
			
			if(fileI.verifyChecksum()){
				slice_mds[i].finalizeDigest();
				slice_mds[i].getSliceDigestInfo().setChecksum(slice_mds[i].getSliceChecksum());
				
				/*
				System.out.print("slice " + slices.get(i).getSliceSeq()+ ": ");
				Resources.printByteArray(slice_mds[i].getSliceChecksum());
				System.out.println("data: ");
				Resources.printByteArray(sliceData[i]);
				*/
				
				slices.get(i).setSliceChecksum(slice_mds[i].getSliceDigestInfo().getChecksum());
			}
			//run threads to run the saving of file slices to
			//physical file servers
			SVDSStreamingPool.stream(new NonStreamerSlice(slices.get(i), sliceData[i],
					(fileI.verifyChecksum()?slice_mds[i].getSliceDigestInfo() : null)));
		}
		
		//checks if the file slices have finish writing, if not, wait for them to finish
		while(currStreamsCnt.get()>0)
			Thread.yield();		

		sliceData=null;
		
	//	localTmpFile.delete();
		localTmpFile=null;
		
	}
	
	private void streamingClose() throws IOException{
		
		try{
			//if ending offset is less than file size & does not end at the end of the "column"
			//(if checksum off) or blk (if checksum on), need to finish the "column"/blk
			int rLen=(fileI.verifyChecksum() ? fileI.getBlkSize() : 1) * info.getQuorum();
			if(fileOffset<fileLength && fileOffset%rLen>0){
				byte[] col=new byte[rLen];
				int len=readFile(fileOffset, col, rLen);
				//System.out.println("Finish col: " + fileOffset + " " + fileLength + " " + rLen+ " " +len);
				for(int i=(int)(fileOffset%rLen); i<len; i++){
					//System.out.println("Write close finish col read: " + (int)(col[i]& 0xff));
					write((int)col[i]);
				}
				col=null;
			}
			
			if(bufferCnt>0)
				streamingSplit();
			
			//wait for prev writes to finish
			while(currStreamsCnt.get()>0) 
				Thread.yield();
			
			if(fileI.verifyChecksum()){
				//update the slice checksum
				for(int i=0; i<slices.size(); i++)
					slices.get(i).setSliceChecksum(Resources.convertToHex(slice_mds[i].getSliceChecksum()));
			}
			
			//System.out.println("Write done");
			
			//check the slices with seg to make sure that no segments are redundant
			//eg. length == 0; if found, remove them (can happen when error occurred during
			//write for that segment only)
			List<FileSliceInfo> lstSlice=null;
			int i;
			for(FileSlice s: slices){
				//System.out.println();
				//System.out.println("Slice " + s.getSliceSeq() + ":");
				//System.out.println("main seg " + s.getFileSliceInfo().getSliceName() 
				//		+ " offset:" + s.getFileSliceInfo().getOffset() + " len:" + s.getFileSliceInfo().getLength());
				if(s.getFileSliceInfo().hasSegments()){
					i=0;
					lstSlice=s.getFileSliceInfo().getSegments();
					
					while(i<lstSlice.size()){
						if(lstSlice.get(i).getLength()==0){
							//System.out.println("removing seg");
							s.delete(lstSlice.get(i));
							lstSlice.remove(i);
						}else{ 
							i++;
							//System.out.println("seg " + seg.getSliceName() + " offset:" + seg.getOffset() + " len:" + seg.getLength());
						}
					}
					
					lstSlice=null;
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
			throw new IOException(ex);
		}
		
	}
	
	public boolean isStreamClosing() {
		return streamIsClosing;
	}
	private class LockRefresher extends  Thread{
		private long interval=300*1000; //in milliseconds, default to 5 mins if not defined in prop file
		
		public LockRefresher(){	
			interval=ClientProperties.getLong(ClientProperties.PropName.FILE_LOCK_INTERVAL)*1000;
		}
		
		public void run(){
			try{
				while(true){
					try{
						//System.out.println("Refreshing lock by "+ fileI.getLockBy() +" - " + fileI.getFullPath());
						mt.lockFileInfo(fileI, fileI.getLockBy());
					}catch(RejectedSVDSException ex){
						if(ex.getOrigin()==RejectedSVDSException.PROXY){
							lckErr=ex;
							break;
						}
					}catch(LockedSVDSException ex){
						//encounter lock error means the file is being locked by other people
						//possible to happen even if the lock is obtained at first may be
						//forcefully unlocked by other means and locked by another user
						lckErr=ex;
						//end the thread execution
						break;
					}catch(NotFoundSVDSException ex){
						//encounter not found error means the file is being deleted
						//forcefully by other means
						lckErr=ex;
						//end the thread execution
						break;
					}catch(SVDSException ex){
						//maybe master server temporarily not available, can try again later
					}
					Thread.sleep(interval);
				}
			}catch(InterruptedException ex){
				//stream being closed, so just end the thread execution
				//System.out.println("Stream for " + fileI.getFullPath() + " interrupted.");
			}
			//System.out.println("Stream lock refresher end - "+fileI.getFullPath());
		}
	}
	
	private class NonStreamerSlice implements Runnable{
		private FileSlice slice=null;
		private byte[] in=null;
		private SliceDigestInfo slice_md=null;
		
		public NonStreamerSlice(FileSlice slice, byte[] in, SliceDigestInfo slice_md){
			//if(in==null || in.length==0){
			//	currStreamsCnt.decrementAndGet();
			//	return;
			//}
			
			this.slice=slice;
			this.in=in;
			this.slice_md=slice_md;
		}

		
		public void run(){
			int retryCnt=0;
			FileSlice.Codes lastWriteStatus=FileSlice.Codes.OK;
			
			//if slice is in recovery, non-streaming write should generate a new slice to replace
			if(!slice.isComplete()){
				boolean success=true;
				try{
					success=generateNewSlice();
				}catch(SVDSException ex){
					success=false;
				}finally{
					if(!success){
						slice.flagLastWriteError(FileSlice.Codes.ERR);
						end();
						return;
					}
				}
			}

			do{
				if(lastWriteStatus!=FileSlice.Codes.OK){
					try{
						//in.reset();
						
						//generate a new file slice info to write to
						if(!generateNewSlice())
							break;
						
						retryCnt++;
					}catch(SVDSException ex){
						ex.printStackTrace();
						LOG.error(ex);
						//error occurred, write can no longer continue, so break out of loop
						break;
					}
				}
				
				lastWriteStatus=slice.write(in, slice_md);
			}while(lastWriteStatus!=FileSlice.Codes.OK && retryCnt<streamRetryLimit);
			
			if(lastWriteStatus!=FileSlice.Codes.OK)
				slice.flagLastWriteError(lastWriteStatus);
			else
				//update the length of the slice
				slice.getFileSliceInfo().setLength(in.length);
			
			end();
		}
		
		private void end(){
			currStreamsCnt.decrementAndGet();
			
			slice=null;
			in=null;
			slice_md=null;
		}
		
		private boolean generateNewSlice() throws SVDSException{
			List<FileSliceInfo> lstNewSlice=mt.generateFileSliceInfo(file.getNamespace(), 1, 
					FileIOMode.NON_STREAM, fileI.getLockBy());

			//if no info is returned by the master, then write can no longer
			//continue, so break out of loop
			if(lstNewSlice==null || lstNewSlice.size()==0)
				return false;
			
			FileSliceInfo newSlice=lstNewSlice.get(0);
			newSlice.setSliceSeq(slice.getSliceSeq());
			
			lstNewSlice.clear();
			lstNewSlice=null;
			
			fileI.getSlices().set(fileI.getSlices().indexOf(slice.getFileSliceInfo()), newSlice);
			slice.setFileSliceInfo(newSlice);
			
			return true;
		}
	}

	private class StreamerSlice implements Runnable{
		private FileSlice slice=null;
		private byte[] data=null;
		private int dataLen;
		private long sliceOffset, segOffset;
		private SliceDigestInfo slice_md=null;
		
		public StreamerSlice(FileSlice slice, long sliceOffset, byte[] data, int dataLen, 
				SliceDigestInfo slice_md){
			//if there are errors in the previous writes then the data is lost,
			//so just waste the bytes
			if(slice.isLastWriteError()){
				currStreamsCnt.decrementAndGet();
				return;
			}
			
			this.slice=slice;
			this.data=data;
			this.dataLen=dataLen;
			this.sliceOffset=sliceOffset;
			
			this.slice_md=slice_md;
		}
		
		public void run() {
			if (data==null){
				currStreamsCnt.decrementAndGet();
				return;
			}

			//PROBLEM!!!!!!!
			//Because now one file slice may have multiple threads writing to it at the 
			//same time, if one write fail and new segment generated, getLastSegmentXXX 
			//may not necessary really refer to the last segment of the current thread 
			//write.
			
			//SOLUTION!!!!
			//Each thread to keep track of it's own segment that it is writting to, so
			//instead of just calling write frm the FileSlice class, need to pass in the
			//segment as well.
			
			//In the FileSlice class, instead of a single var storing one slice store impl,
			//a call to each read/write will get it's own slice store impl based on the
			//segment pass in. *If none is provided then use the main seg. 
			
			//The segments in the FileSlice/FileSliceInfo class has to be sorted by the offset
			//cos when a new thread is started, it will attempt to get the segment with the
			//largest offset [DONE] to compare and see if it is a continuation from there (thus no
			//new segments need to be created). 
		
			//That means the length of the new segment always
			//have to be set/added to existing before actually writting to the slice store
			//so any new thread would be able to determine if it can continue from the same
			//segment
			
			//if the slice goes into recovery mode (means that segments are created),
			//the running threads that writes must execute sequentially using synchronized 
			//code so as to prevent many segments from created since multiple threads can
			//write to the same file slice in normal situations.
			//assumption is made such that earlier threads do get executed first before
			//later threads (writting to same slice)
			FileSliceInfo seg;
			if(!slice.getFileSliceInfo().hasSegments() || (slice.getFileSliceInfo().isSliceRecovery() &&
					slice.getFileSliceInfo().getSegmentCount()<=1)){
				if(slice.getFileSliceInfo().isSliceRecovery() && !slice.getFileSliceInfo().hasSegments()){
					if((seg=addSegment())==null){
						slice.flagLastWriteError(FileSlice.Codes.ERR);
						currStreamsCnt.decrementAndGet();
						return;
					}
					seg.setOffset(sliceOffset);
					slice.getFileSliceInfo().addSegment(seg);
				}else{
					seg=slice.getFileSliceInfo().getLastSegment();
				}
				
				segOffset=sliceOffset-seg.getOffset();
				
				//non-sync slice write
				writeSlice(seg);
			}else{
				//sync write
				synchronized(slice){
					seg=slice.getFileSliceInfo().getLastSegment();
					
					//check if there is a need to generate a new segment when the there is a
					//gap between the current offset and the last seg retrieved
					//System.out.println("slice " +slice.getSliceSeq() +" offset: " + sliceOffset + " seg offset: " + seg.getOffset()
					//		+ " seg len: " + seg.getLength());
					if (sliceOffset<seg.getOffset() || 
							sliceOffset>(seg.getOffset()+seg.getLength())){
						if((seg=addSegment())==null){
							slice.flagLastWriteError(FileSlice.Codes.ERR);
							currStreamsCnt.decrementAndGet();
							return;
						}
						
						seg.setOffset(sliceOffset);
						slice.getFileSliceInfo().addSegment(seg);
					}
					
					segOffset=sliceOffset-seg.getOffset();
					
					writeSlice(seg);
				}
			}
			
			currStreamsCnt.decrementAndGet();
		}
		
		private void writeSlice(FileSliceInfo seg){
			int retryCnt=0;
			FileSlice.Codes lastWriteStatus=FileSlice.Codes.OK;
			
			//increase the length to be written to the segment/slice so any new thread 
			//would be able to determine if it can continue from the same segment/slice
			seg.incrementLength(dataLen, segOffset);
			
			do{
				//System.out.println("write to " + seg.getSliceName()+": " + Resources.concatByteArray(data, 0, dataLen));
				lastWriteStatus=slice.write(seg, data, segOffset, dataLen, slice_md);
			
				if(lastWriteStatus==FileSlice.Codes.ERR){
					//if the mode is not SVDS (means PVFS), then just attempt retry to write block to the same slice store without adding segments,
					//only if retry time has reached then the whole slice write is considered fail
					if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE)
							.equals(ClientProperties.CLIENT_MODE_SVDS)){
						//ask master to generate a new file slice info
						FileSliceInfo nseg=addSegment();
						if(nseg==null){
							slice.flagLastWriteError(FileSlice.Codes.ERR);
							break;
						}

						if(!newFile){
							//if error with the current write, check if the seg is created
							//in this thread (the offset will be the same as the slice offset)
							//if yes, set the offset of the seg to exclude this portion of data
							//but don remove the segment cos other threads may already use that
							//segment for writing
							synchronized(seg){
								if(seg.getOffset()==sliceOffset){
									seg.setOffset(sliceOffset+dataLen);
									//System.out.println("slice " + slice.getSliceSeq() + " seg "+seg.getSliceName()+" decrease len " + dataLen);
									seg.decrementLength(dataLen);
								}
							}

							seg=nseg;
							seg.setOffset(sliceOffset);
							slice.getFileSliceInfo().addSegment(seg);
						}else{
							//for first write, if the write fail, then replace the slice info since
							//nothing is written
							fileI.removeSlice(seg);
							seg=nseg;
							slice.setFileSliceInfo(seg);
							fileI.addSlice(seg);
						}

						segOffset=0;
						seg.incrementLength(dataLen, segOffset);
					}
					
					retryCnt++;
				}
			}while(lastWriteStatus!=FileSlice.Codes.OK && retryCnt<streamRetryLimit);
			
			//release all resources
			slice_md=null;
			data=null;
			slice=null;
		}
		
		private FileSliceInfo addSegment(){
			//System.out.println("add segment for slice " + slice.getSliceSeq() + " for offset " + sliceOffset);
			try{
				FileSliceInfo seg;
				
				//ask master to generate a new file slice info
				List<FileSliceInfo> segs=mt.generateFileSliceInfo(file.getNamespace(), 1,
						FileIOMode.STREAM, fileI.getLockBy());
				if(segs==null || segs.size()==0)
					//master also cannot generate, have to give up writing this slice
					return null;
				else{
					seg=segs.get(0);
					//seg.setOffset(sliceOffset);
					//slice.getFileSliceInfo().addSegment(seg);
				}
				
				segs.clear();
				segs=null;

				return seg;
			}catch(Exception ex){
				LOG.error(ex);
				return null;
			}
		}
	}
}
