package sg.edu.nyp.sit.svds.client.ida;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.gf2.GaloisField;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

public class RabinImpl2 implements IInfoDispersal {
	public static final long serialVersionUID = 1L;
	
	public static void main(String[] args)
	{
		int row = 10, col = 7;
		int[][] mat = Util.generateIndependenceMatrix(row, col);
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				System.out.print(mat[i][j] + ",");
			}
			System.out.print("|");
		}
	}
	
//	private int[][] m_nKeyMatrix = {
//	        {1,        1,      1},
//	        {1,        2,      4},
//	        {1,        3,      9},
//	        {1,        4,      16},
//	        {1,        5,      25}
//		};
	
//	private int[] getRow(int row, int[][] matrix)
//	{
//		return matrix[row];
//	}
	
//	public int[][] generateMatrix(int row, int col)
//	{
//		int[] x = new int[row];
//		int[] y = new int[col];
//		
//		for (int i = 0; i < x.length; i++) {
//			x[i] = i*2 + 1;
//		}
//		for (int i = 0; i < y.length; i++) {
//			y[i] = i*2;
//		}
//		
//		int[][] mat = new int[row][col];
//		GaloisField gf = GaloisField.getInstance();
//		int n;
//		for (int r = 0; r < row; r++) {
//			mat[r] = new int[col];
//			for (int c = 0; c < col; c++) {
//				mat[r][c] = (int)(gf.gf2_inv(8, x[r] ^ y[c]));
//			}
//		}
//		
//		return mat;
//		
//	}
	
//Alternative method 1, j, j^2
//	public int[][] generateMatrix(int row, int col)
//	{
//		int[][] mat = new int[row][col];
//		for (int i = 0; i < row; i++) {
//			mat[i] = new int[col];
//			for (int j = 0; j < col; j++) {
//				if(i == 0)
//					mat[i][j] = 1;
//				else if(j == 0)
//					mat[i][j] = 1;
//				else if(j == 1) {
//					mat[i][j] = i+1;
//				}
//				else {
//					//i = 2, j = 2
//					mat[i][j] = (int)GaloisField.getInstance().gf2_pow(8, i+1, j); 
//					//mat[i][j] = (int)Math.pow(i+1, j);
//					System.out.println("i+1:" + (i+1));
//					System.out.println("j:" + j);
//					System.out.println("mat:" + mat[i][j]);
//				}
//			}
//		}
//		
//		return mat;
//	}
	
	//parameter "in" may be a fix size array that is reused in
	//SVDSOutputStream so the actual data length may not be the array length therefore
	//need to pass in the length of the data to be splitted. Because padding is set
	//based on the length of the data to be splitted, the data length pass in should
	//be mutliples of the quorum in the event that the split is called multiple times
	//(e.g streaming mode)
	//return value is the no of padded bytes. The actual length of the splitted data
	//for each slice can get retrieved through the idainfo class getSliceLength(int)
	//if digest is needed, pass in the class so the digest can be cal while splitting
	//instead of running thru the array to cal again outside the method
	@Override
	public int split(byte[] in, int inOffset, int inLen, IdaInfo info, byte[][] out, int outOffset,
			SliceDigest[] mds)
		throws IDAException{
		if(inLen<=0)
			return 0;
		
		if(in==null || in.length<(inLen+inOffset) || out==null || out.length!=info.getShares())
			throw new IDAException ("Input/Output is null/empty/out of bounds.");
		
		//check that all out array are big enough to accommodate
		int sliceLen=(int)info.getSliceLength(inLen);
		for(byte[] o: out){
			if((o.length-outOffset)<sliceLen)
				throw new IDAException("Output array out of bounds.");
		}
		
		int inIndex=inOffset, outIndex=outOffset;
		int[] segment = new int[info.getQuorum()];
		int[] keyrow = null;
		
		int b, padCnt=0;
		for(int n=0; n<sliceLen; n++) {
			for (int i = 0; i < segment.length; i++) {
				if(inIndex>=(inLen+inOffset)) {	
					segment[i] = 0; //Padding
					padCnt++;
				}else {
					segment[i]=in[inIndex] & 0xff;
					inIndex++;
				}
				//System.out.print(segment[i] + " ");
			}

			for (int row = 0; row < info.getShares(); row++) {
				keyrow = info.getMatrix()[row]; //get the correct row in the key matrix
				if(keyrow.length != segment.length) {
					throw new IDAException("Error:split(InputStream, int):  Segment length and key length does not match");
				}
				
				b=mac(segment, keyrow);
				//System.out.print(b + " ");
				out[row][outIndex]=(byte)b;
				if(mds!=null) mds[row].update((byte)b);
			}
			
			outIndex++;
		}
		//System.out.println();
		
		return sliceLen;
	}

	@Override
	public List<InputStream> split(InputStream is, IdaInfo info)
			throws IDAException {
		
		is=new BufferedInputStream(is);

		List<ByteArrayOutputStream> lstOut = new ArrayList<ByteArrayOutputStream>();
		//IdaInfo idainfo = new IdaInfo(5,3);
		
		//Calculate parameters for split operation
		int nSliceCount = info.getShares(); // n
		int nSliceLength = info.getSliceLength(); // N/m
		int m = info.getQuorum(); // m
		
		//Prepare streams for output
		for (int i = 0; i < nSliceCount; i++) {
			lstOut.add(new ByteArrayOutputStream(nSliceLength));
		}
		
		//Tag the segment number and size of stream at the front, they are needed when combining the file
		//tagSegmentExtraData(lstOut, (int)info.getDataSize());
		
		int[] keyrow = null;
		int[] segment = null;
		
		//Performs the multiplexing of input stream and key matrix to generate slices  
		try {
			for (int slice = 0; slice < nSliceLength; slice++) {
				segment = getNextSegment(is, m);
				for (int row = 0; row < nSliceCount; row++) {
					//keyrow = getRow(row, info.getMatrix()); //get the correct row in the key matrix
					keyrow = info.getMatrix()[row]; //get the correct row in the key matrix
//					System.out.println("Matrix:Row" + row);
//					for (int i = 0; i < keyrow.length; i++) {
//						System.out.print(keyrow[i] + " ");
//					}
//					System.out.println("");
					if(keyrow.length != segment.length) {
						throw new IDAException("Error:split(InputStream, int):  Segment length and key length does not match");
					}
					lstOut.get(row).write(mac(segment, keyrow));
				}
			}
			
			is.close();
		}
		catch(IOException ex) {
			throw new IDAException(ex.getMessage());
		}
		
		return toListOfInputStream(lstOut);
	}
	
	//parameter "slices" may be a fix size array that is reused in
	//SVDSInputStream so the individual actual data length may not be the array length 
	//therefore need to pass in the length of data to combine. Also the padding count
	//is passed in if the slices are the tail of the file and need to remove the padded bytes
	//when split if performed earlier.
	//to get the length of the data combined (does not always equal to the length of the array
	//because it is fixed and reused in SVDSInputStream), call idainfo class getDataSize(int, int)
	@Override
	public int combine(byte[][] slices, int sliceOffset, int sliceLen, IdaInfo info, byte[] out, int outOffset)
		throws IDAException{
		if(sliceLen<=0)
			return 0;
		
		long dataLen=info.getDataSize();
		long dataOffset=info.getDataOffset();
		if(slices==null || slices.length<info.getQuorum() || out==null || out.length<outOffset)
			throw new IDAException ("Input/Output is null/empty/out of bounds.");
		
		for(byte[] s: slices){
			if(s.length<(sliceLen+1+sliceOffset))
				throw new IDAException("Input array out of bounds");
		}
		
		int slicesIndex=1+sliceOffset, outIndex=outOffset;
		
		int[] sliceIds=new int[info.getQuorum()];
		for(int i=0; i<sliceIds.length; i++){
			sliceIds[i]=slices[i][sliceOffset];
		}
		
		int[][] inverse = generateInverse(sliceIds, info.getMatrix());
		
		int[] bytesFromSlices = new int[info.getQuorum()];
		int b, dataCnt=0;
		
		try{
			for(int i=0; i<sliceLen; i++){
				for(int n=0; n<bytesFromSlices.length; n++){
					bytesFromSlices[n]=slices[n][slicesIndex] & 0xff;
				}
				slicesIndex++;

				for(int j=0; j<info.getQuorum(); j++){
					b = mac(bytesFromSlices, inverse[j]);

					if(dataOffset>=dataLen)
						return dataCnt;

					//System.out.print(b + " ");
					out[outIndex]=(byte)b;
					outIndex++;

					dataCnt++;
					dataOffset++;
				}
			}
			//System.out.println();
		}catch(Exception ex){
			throw new IDAException(ex);
		}
		
		return dataCnt;
	}
	
	@Override
	public InputStream combine(List<InputStream> slices, IdaInfo info) throws IDAException {

		//Prepare output stream (the combined file)
		ByteArrayOutputStream osCombinedFile = new ByteArrayOutputStream();
		
		//IdaInfo class helps in managing parameters for the IDA algorithm (size, lengths, mod value etc)
		//IdaInfo idainfo = new IdaInfo(5,3);
		int m = info.getQuorum(); // if the input number of stream < m, algorithm cannot function
		
		//Check if the List contains enough InputStream to perform the combine() operation.
		if(slices.size() < m)
			throw new IDAException("Error:combine(List<InputStream>):Insufficient slice available for combine operations");
		
		//If the input list > m slices, we remove some, we only need m slices
		List<InputStream> lstSlices = new ArrayList<InputStream>();
		for (int i = 0; i < m; i++) {
			lstSlices.add(slices.get(i));
		}

		//Retrieve the id of each slice.  This is required to get the correct row in the key matrix 
		int[] sliceIds = null;
		int[][] inverse = null;
		int[] bytesFromSlices = null;
		//use to track the number of bytes of output, this will remove adding padding bytes added previously
		//int nOutputCount = 0; 
		long nOutputCount = info.getDataOffset();
		
		int abyte;
		//int nStreamSize = parseSegmentExtraData(lstSlices, sliceIds);
		sliceIds = parseSegmentExtraData(lstSlices);
		long nStreamSize = info.getDataSize();
		//idainfo.setDataSize(nStreamSize);
		
		//Prepare the inverse matrix
		//inverse = generateInverse(sliceIds, m_nKeyMatrix);
		inverse = generateInverse(sliceIds, info.getMatrix());
		
		for (int i = 0; i < info.getSliceLength(); i++) {
			bytesFromSlices = getNextByteFromSlices(lstSlices);
			if(bytesFromSlices==null)
				break;
			
			for (int j = 0; j < m; j++) {
				abyte = mac(bytesFromSlices, inverse[j]);
				osCombinedFile.write(abyte);
				nOutputCount++;
				if(nOutputCount >= nStreamSize) {
					//System.out.println("Output Count = " + nOutputCount + ", StreamSize = " + nStreamSize);
					break;
				}
			}
		}
			
		
		//Return an InputStream for ease of use by calling function
		return new ByteArrayInputStream(osCombinedFile.toByteArray());
		
	}

	
	/***
	 * Converts from a list of output stream to a list of input streams (for easy reading by calling function)
	 * @param lstOut
	 * @return
	 */
	private List<InputStream> toListOfInputStream(List<ByteArrayOutputStream> lstOut)
	{
		List<InputStream> lis = new ArrayList<InputStream>();
		for(ByteArrayOutputStream os : lstOut) {
			lis.add(new ByteArrayInputStream(os.toByteArray()));
		}
		return lis;
	}
	
	/***
	 * This is a function to multiply and accumulate (+).  The * and + operations
	 * are performed using gf2 (gf2_mul and ^ respectively).
	 * @param a1  First array containing int values to be multiplied. 
	 * @param a2 Second array containing int values to be multiplied.
	 * @param mod The mod value to be performed
	 * @return Return the final value of the mod operation.
	 */
	private int mac(int[] a1, int[] a2)
	{
		int acc = 0;
		for (int i = 0; i < a1.length; i++) {
			acc = acc ^ (int)GaloisField.getInstance().gf2_mul(8, a1[i], a2[i]);
		}
		
		return acc;
	}
	
	/***
	 * Retrieves the next segment of the input file
	 * @return Array of bytes of the file. Note that it is in int form
	 * because in Java, bytes are SIGNED! That is, from -127 to 128.  As such
	 * we cannot calculate it based on byte and must first convert it to integer
	 * @throws IOException
	 */
	private int[] getNextSegment(InputStream is, int length) throws IOException
	{
		int[] ba = new int[length];
		for (int i = 0; i < ba.length; i++) {
			ba[i] = is.read();
			if(ba[i] == -1) {
				ba[i] = 0; //Padding
			}
		}
		
		return ba;
	}
	
	/**
	 * Adds segment id and full stream size to each of the streams.  Data are
	 * needed during recombination.  Stream size is needed to support padding
	 * @param lst
	 * @count The Size of the stream
	 */
//	private void tagSegmentExtraData(List<ByteArrayOutputStream> lst, int count)
//	{
//		ByteArrayOutputStream baos;
//		for (int i = 0; i < lst.size(); i++) {
//			baos = lst.get(i);
//			baos.write(i);
//			baos.write(count);
//		}
//	}


	/***
	 * Currently parses only the segment id that was tagged using the tagSegmentExtraData() function
	 * during the combine operation
	 * @param lst
	 * @param SliceId
	 * @return The size of the original stream
	 * @throws IDAException
	 */
	private int[] parseSegmentExtraData(List<InputStream> lst) throws IDAException
	{
		int[] SliceId = new int[lst.size()];;
		try {
			for (int i = 0; i < lst.size(); i++) {
				SliceId[i]= lst.get(i).read();
			}
			
			if(lst.size() < 1)
				throw new IDAException("parseSegmentExtraData::Incorrect number of streams");

//stream size is now provided by IdaInfo class			
//			int nStreamSize = lst.get(0).read();
//			for (int i = 1; i < lst.size(); i++) {
//				if(lst.get(i).read() != nStreamSize)
//					throw new IDAException("parseSegmentExtraData::Stream Size check failed.");
//			}
			
			//return nStreamSize;
		}
		catch(Exception ex) {
			throw new IDAException("parseSegmentExtraData: " + ex.getMessage());
		}
		return SliceId;
	}
	
	/***
	 * Generates the inverse matrix used to recombine the slices.  The rows needed depends
	 * on the slices used to perform the recombination.
	 * @param sliceIds An array of the IDs of the slices used for the recombination. 
	 * @param key The key matrix used during the splitting.
	 * @return The inverse matrix
	 */
	private int[][] generateInverse(int[] sliceIds, int[][] key) throws IDAException
	{
		//Verify the input parameters
		int rows = sliceIds.length;
		if(rows == 0)
			return null;
		
		int cols = rows;
		if(cols == 0) {
			return null;
		}
		
		//Allocate array for the return result
		int d[][] = new int[rows][cols];
		
		//Extract the rows of the key matrix corresponding to the input sliceIds parameter
		int index = 0;
		for (int i = 0; i < sliceIds.length; i++) {
			for (int j = 0; j < cols; j++) {
				if(j <= sliceIds.length) {
					d[index][j] = key[sliceIds[i]][j];
				}
			}
			index++;
		}
		
		return Util.invertMatrixGf2(d);
		
	}

	/***
	 * Generates the inverse that is needed to recombine slices
	 * @param sliceIds
	 * @return
	 */
//	private int[][] generateInverse(int[] sliceIds, int[][] key) {
//		
//		int row = sliceIds.length;
//		if(row == 0)
//			return null;
//		
//		int col = row;
//		if(col == 0) {
//			return null;
//		}
//		
//		double d[][] = new double[row][col];
//		int indexRow = 0;
//		for (int i = 0; i < key.length; i++) {
//			//skip the rows that are not in the sliceIds
//			if(inArray(i, sliceIds) == false)
//				continue;
//			for (int j = 0; j < col; j++) {
//				if(j <= sliceIds.length) {
//					d[indexRow][j] = (double)key[i][j];
//				}
//			}
//			indexRow++;
//		}
//		
//		Matrix matInverse = (new Matrix(d)).inverse();
//		d = matInverse.getArray();
//		
//		//Convert from double[][] to int[][]
//		int[][] n = new int[row][col];
//		for (int i = 0; i < row; i++) {
//			for (int j = 0; j < col; j++) {
//				n[i][j] = Math.round((float)d[i][j]);//Convert from float to integer
//			}
//		}
//		
//		return n;
//		
//	}
	
	@SuppressWarnings("unused")
	private boolean inArray(int i, int[] array)
	{
		for (int j = 0; j < array.length; j++) {
			if(i == array[j])
				return true;
		}
		
		return false;
	}

	private int[] getNextByteFromSlices(List<InputStream> in) throws IDAException
	{
		int[] a = new int[in.size()];
		int b;
		try {
			for (int i = 0; i < a.length; i++) {
				//if the stream ends, return null
				if((b=in.get(i).read())==-1)
					return null;
				
				a[i] = b;
			}
		}
		catch(IOException ex) {
			throw new IDAException("Error:getNextByteFromSlice:Error in reading byte from input stream (slice)");
		}
		return a;
	}

	public void split(ArrayBlockingQueue<Integer> in, BlockingQueue<Integer>[] out, 
			IdaInfo info){
		(new Splitter(in, out, info)).start();
	}
	
	public void combine(BlockingQueue<int[]>[] in, ArrayBlockingQueue<Integer> out, 
			IdaInfo info){
		(new Combiner(in, out, info)).start();
	}
	
	private class Splitter extends Thread {
		private ArrayBlockingQueue<Integer> in=null;
		private BlockingQueue<Integer>[] out=null;
		private IdaInfo info=null;
		
		public Splitter(ArrayBlockingQueue<Integer> in, BlockingQueue<Integer>[] out, 
			IdaInfo info){
			this.in=in;
			this.out=out;
			this.info=info;
		}

		public void run() {
			boolean done=false;
			int byteCnt;
			int[] keyrow;
			
			//String debug="Data to split: ";
			
			//if the output stream is not closed or the buffer still have data waiting to be read,
			//read m number of bytes from the buffer, transform it and put in the out queue for
			//the streamer thread to pick up
			int buff[] = new int[info.getQuorum()];
			while (!done) {
				//int buff[] = new int[info.getQuorum()];
				
				//pick up enough bytes to transform
				for (byteCnt = 0; byteCnt < info.getQuorum(); byteCnt++) {
					
					try {
						buff[byteCnt] = in.take();
						
						//debug+=buff[byteCnt] + " ";
						
						//if the buffer receive a -1, means the client has finish writing
						//so break out of the for loop. The byteCnt will store how many bytes
						//is read into the array, so can do padding if needed
						if(buff[byteCnt]==FileInfo.EOF){
							done=true;
							break;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				//if the first byte read is already -1, then exit the loop
				if(done && byteCnt==0)
					break;
				
				//Padding
				if(done == true) {
					for(int i = byteCnt; i < info.getQuorum(); i++) {
						buff[byteCnt] = 0;
					}
				}
				
				//transform the bytes		
				for (int row = 0; row < info.getShares(); row++) {
					//keyrow = getRow(row, info.getMatrix()); //get the correct row in the key matrix
					keyrow = info.getMatrix()[row]; //get the correct row in the key matrix
					if(keyrow.length != buff.length) {
						//throw new IDAException("Error:split(InputStream, int):  Segment length and key length does not match");
						System.out.println("Error:split(InputStream, int):  Segment length and key length does not match");
						break;
					}
					
					try {
						int b=mac(buff, keyrow);
						out[row].put(b);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
				}
				
				//buff=null;
				Arrays.fill(buff, 0);
			}
			buff=null;
			
			//System.out.println(debug);
			
			//put -1 in the out queues to indicate that transformation has finished
			for (int n=0; n<info.getShares(); n++) {
				try {
					out[n].put(FileInfo.EOF);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			keyrow=null;
		}

	}

	private class Combiner extends Thread {
		private BlockingQueue<int[]>[] in=null;
		private ArrayBlockingQueue<Integer> out=null;
		private IdaInfo info=null;
		
		public Combiner(BlockingQueue<int[]>[] in, ArrayBlockingQueue<Integer> out, 
				IdaInfo info){
			this.in=in;
			this.out=out;
			this.info=info;
		}
		
		public void run() {
			boolean done=false, error=false;
			long nOutputCount = info.getDataOffset();
			
			//String debug="Data combined: ";
			int[] buff_seq=new int[info.getQuorum()];
			int buff[][] = new int[info.getQuorum()][];
			while (!done) {
				//int buff[][] = new int[info.getQuorum()][];
				//int[] buff_seq=new int[info.getQuorum()];
				
				//read m array of bytes from the in queues together with the seq
				for (int i = 0; i < info.getQuorum(); i++) {
					try {
						buff[i]=in[i].take();
						//first integer in the array is the slice seq
						buff_seq[i]=buff[i][0];
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				//file slices are not read 1 byte at a time but multiple of bytes 
				//(therefore result in array of array of bytes) to reduce no of http 
				//calls. So in order to transform, place in single byte array first
				for(int index=1; index<buff[0].length; index++){ //NOTE: starts from 1 because 1st byte is used to indicate seq
					int[] tmp=new int[info.getQuorum()];
					for(int i=0; i<info.getQuorum(); i++){
						//-1 in the slice to indicate end of slice has been reached,
						//so skip the transformation
						if(buff[i][index]==FileInfo.EOF){
							done=true;
							break;
						}else{
							tmp[i]=buff[i][index];
						}
					}

					if(done==true)
						break;

					//transform the bytes
					//Prepare the inverse matrix
					int count;
					int[] data=new int[info.getQuorum()];
					try {
						int[][] inverse = generateInverse(buff_seq, info.getMatrix());
						
						/*
						System.out.println("Display inverse matrix");
						for(int i=0; i<inverse.length; i++){
							for(int n=0; n<inverse[i].length; n++)
								System.out.print(inverse[i][n] + " ");
							
							System.out.println();
						}
						
						String str="To be combined: ";
						for(int i=0; i<tmp.length; i++){
							str+= tmp[i] + " ";
						}
						System.out.println(str);
						*/
						
						for (count = 0; count < info.getQuorum(); count++) {
							//perform the combination here and put the result in var data
							data[count] = mac(tmp, inverse[count]);
							nOutputCount++;
							//System.out.println("count: " + count+ ", output cnt: " + nOutputCount + ", data size: " + info.getDataSize());
							if(nOutputCount > info.getDataSize()) {
								break;
							}
						}
					}
					catch (IDAException e) {
						e.printStackTrace();
						//set to true so will break out of the loop
						done=true;
						//set to true so will put the error code in the out queue
						error=true;
						break;
					}

					//put in the buffer for reading
					//for(int i=0; i<data.length; i++){
					for(int i=0; i<count; i++){ //based on count to handle case of padding removal
						try {
							//debug+=data[i]+ " ";
							
							out.put(data[i]);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
					data=null;
					tmp=null;
				}
				
				
				//buff=null;
				Arrays.fill(buff, null);
				//buff_seq=null;
				Arrays.fill(buff_seq, 0);
			}
			buff_seq=null;

			//System.out.println(debug);
			
			//put a -1 to indicate finish transformation or error
			try{ out.put((!error?FileInfo.EOF:IInfoDispersal.ERROR_CODE)); }catch (InterruptedException e) {}
		}
	}
	
	
}
