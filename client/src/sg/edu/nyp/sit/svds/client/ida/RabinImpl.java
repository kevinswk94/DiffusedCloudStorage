package sg.edu.nyp.sit.svds.client.ida;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;
import Jama.Matrix;

public class RabinImpl implements IInfoDispersal {
	public static final long serialVersionUID = 1L;

	IdaInfo m_IdaInfo = new IdaInfo(5,3);
	Matrix m_matA;
	private static final int FIX_POINT_FACTOR = 1;
	private static final int FIX_POINT_SHIFT = 0;//-16383;
	private static final int CONVERSION_SIZE = 2;//2 - short, 4 - float
	//private int m_nPaddingCount = 0;
	
	public RabinImpl()
	{
	}
	
	/***
	 * Initialize the x and y coefficients for generating the
	 * independent vectors (ai)
	 */

	private void initCoeff()
	{
//		double[][] dvalues = {
//	        {1.000,        0.500,      0.3333},
//	        {0.500,        0.333,      0.2500},
//	        {0.333,        0.250,      0.2000},
//	        {0.250,        0.200,      0.1667},
//	        {0.200,        0.167,      0.1428}
//		};
        
		double[][] dvalues = {
		        {1,        1,      1},
		        {1,        2,      4},
		        {1,        3,      9},
		        {1,        4,      16},
		        {1,        5,      25}
			};


		//		double[][] dvalues = {
//				{1.00,        0.50,        0.33,        0.25,        0.20,        0.17,        0.14,       0.12},
//		        {0.50,        0.33,        0.25,        0.20,        0.17,        0.14,        0.12,       0.11},
//		        {0.33,        0.25,        0.20,        0.17,        0.14,        0.12,        0.11,       0.10},
//		        {0.25,        0.20,        0.17,        0.14,        0.12,        0.11,        0.10,       0.09},
//		        {0.20,        0.17,        0.14,        0.12,        0.11,        0.10,        0.09,       0.08},
//		        {0.17,        0.14,        0.12,        0.11,        0.10,        0.09,        0.08,       0.08},
//		        {0.14,        0.12,        0.11,        0.10,        0.09,        0.08,        0.08,       0.07},
//		        {0.12,        0.11,        0.10,        0.09,        0.08,        0.08,        0.07,       0.07},
//		        {0.11,        0.10,        0.09,        0.08,        0.08,        0.07,        0.07,       0.06},
//		        {0.10,        0.09,        0.08,        0.08,        0.07,        0.07,        0.06,       0.06}
//		};
		
		m_matA = new Matrix(dvalues);
		//m_matAInverse = m_matA.getMatrix(1, 3, 0, 2).inverse();
		//System.out.println("Inverse Matrix");
		//m_matAInverse.print(5, 2);
	}
	
	private Matrix getInverseMatrix(int[] rows) {
		
		return m_matA.getMatrix(rows, 0, 2).inverse();
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
				//Padding
				ba[i] = 0;
				//m_nPaddingCount++;
				//throw new IOException("RabinImpl::getNextSegment: Data Read Failed");
			}
		}
		
		return ba;
	}
	
	/***
	 * Combines File segments into its original sequence
	 */
	@Override
	public InputStream combine(List<InputStream> lst, IdaInfo info) throws IDAException 
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		
		try {
			
			//Put all input inputstreams into an arraylist
			List<InputStream> lstSegments = new ArrayList<InputStream>();
			m_IdaInfo = new IdaInfo(5,3);
			int m = m_IdaInfo.getQuorum();
			for (int i = 0; i < m; i++) {
				lstSegments.add(lst.get(i));
			}

			//Get Segment Id from first byte
			int[] anSegmentId = getSegmentId(lstSegments);
			int nStreamSize = parseStreamSize(lstSegments);

			m_IdaInfo.setDataSize(nStreamSize);
			int nSliceLength = m_IdaInfo.getSliceLength();
			
			Matrix matInverse = getInverseMatrix(anSegmentId).transpose();
			
			int nOutputCount = 0;
			float[] afSegment = new float[m];
			for (int i = 0; i < nSliceLength; i++) {
				afSegment = getNextBytesFromSegments(lstSegments);
				for (int j = 0; j < m; j++) {
					baos.write(dotProduct(afSegment, getMatrixRow(matInverse, j)));
					nOutputCount++;
					if(nOutputCount >= nStreamSize)
						break;
				}
			}
			
			return new ByteArrayInputStream(baos.toByteArray());
		}
		catch(IOException ex)
		{
			throw new IDAException(ex.getMessage());
		}
	}
	
	private int[] getSegmentId(List<InputStream> lst) throws IOException
	{
		int[] an = new int[lst.size()];
		for (int i = 0; i < lst.size(); i++) {
			an [i]= lst.get(i).read();
		}
		return an;
	}
	
	private int parseStreamSize(List<InputStream> lst) throws IOException, IDAException
	{
		if(lst.size() < 1)
			return 0;
		
		int nStreamSize = lst.get(0).read();
		for (int i = 1; i < lst.size(); i++) {
			if(lst.get(i).read() != nStreamSize)
				throw new IDAException("parseStreamSize::Stream Size check failed.");
		}
		return nStreamSize;
	}
	
	private int dotProduct(float[] f1, float[] f2) throws IDAException
	{
		if(f1.length != f2.length)
			throw new IDAException("dotProduct::Incorrect length");
		float f = 0.0F;
		for (int i = 0; i < f1.length; i++) {
			f += f1[i] * f2[i];
		}
		
		return Math.round(f);
	}

	/**
	 * Split a file into segments according to Rabin's algorithm
	 * Takes in an inputstream and generates a list of segments.  Number of segments
	 * is n (which is k + m)
	 */
	@Override
	public List<InputStream> split(InputStream isFile, IdaInfo info) throws IDAException 
	{
		return splitWithoutReset(isFile, (int)info.getDataSize());
	}
	
	private List<InputStream> splitWithoutReset(InputStream isFile, int size) throws IDAException
	{
		//Calculate segment size
		m_IdaInfo.setDataSize(size);
		
		//Initialize the A matrix
		initCoeff();

		List<ByteArrayOutputStream> lstOut = new ArrayList<ByteArrayOutputStream>();
		int n = m_IdaInfo.getShares();
		int Nm = m_IdaInfo.getSliceLength();
		int m = m_IdaInfo.getQuorum();

		for (int i = 0; i < n; i++) {
			lstOut.add(new ByteArrayOutputStream(Nm)); //OutputSegmentLength = N/m
		}
		
		//Tag the segment number and size of stream at the front, they are needed when combining the file
		tagSegmentExtraData(lstOut, size);
		
		Matrix matSegment, matResult, matA;
		double dResult;
		short sResult;
		int[] baSegment;
		try {
			for (int i = 0; i < Nm; i++) { //n, for each Fi
				baSegment = getNextSegment(isFile, m); //there should be N/m segments
				matSegment = segmentToMatrix(baSegment);
				for (int j = 0; j < n; j++) {
					matA = m_matA.getMatrix(j, j, 0, m_matA.getColumnDimension()-1);
					matResult = matA.times(matSegment);
					dResult = matResult.get(0, 0);
					sResult = (short)((dResult * FIX_POINT_FACTOR)+FIX_POINT_SHIFT);
					lstOut.get(j).write(Util.shortToBytes(sResult));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new IDAException(e.getMessage());
		}
		
		List<InputStream> lis = new ArrayList<InputStream>();
		for(ByteArrayOutputStream os : lstOut) {
			lis.add(new ByteArrayInputStream(os.toByteArray()));
		}
		return lis;
		
	}
	
	@SuppressWarnings("unused")
	private List<InputStream> splitWithReset(InputStream isFile, int size) throws IDAException
	{
		//Calculate segment size
		m_IdaInfo.setDataSize(size);
		
		//Initialize the A matrix
		initCoeff();
		
		List<ByteArrayOutputStream> lstOut = new ArrayList<ByteArrayOutputStream>();
		for (int i = 0; i < m_IdaInfo.getShares(); i++) { //n
			lstOut.add(new ByteArrayOutputStream(m_IdaInfo.getSliceLength())); //OutputSegmentLength = N/m
		}
		
		//Tag the segment number and size of stream at the front, they are needed when combining the file
		tagSegmentExtraData(lstOut, size);
		
		ByteArrayOutputStream baos;
		Matrix ai, matResult;
		try {
			
			isFile.mark(0);
			
			double dResult;//, dError, dErrorMax = 0;
			//short sMax= Short.MIN_VALUE, sMin = Short.MAX_VALUE;
			short sResult;
			for (int i = 0; i < m_IdaInfo.getShares(); i++) { //n, for each Fi
				//Calculate F_i where i = 1 to n
				//Fi = Ci1Ci2Ci3...Ci(N/m)
				//Cik = ai1.Sk
				baos = lstOut.get(i);
				ai = m_matA.getMatrix(i, i, 0, m_matA.getColumnDimension()-1);
				isFile.reset();
 				for (int j = 0; j < m_IdaInfo.getSliceLength(); j++) { //N/m
					//result = cik
					matResult = ai.times(segmentToMatrix(getNextSegment(isFile, m_IdaInfo.getQuorum())));
					dResult = matResult.get(0, 0);
					
					sResult = (short)((dResult * FIX_POINT_FACTOR)+FIX_POINT_SHIFT);
					
					baos.write(Util.shortToBytes(sResult));
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			throw new IDAException(e.getMessage());
		}
		
		List<InputStream> lis = new ArrayList<InputStream>();
		for(ByteArrayOutputStream os : lstOut) {
			lis.add(new ByteArrayInputStream(os.toByteArray()));
		}
		
		//System.out.println("Padding count = " + m_nPaddingCount);
		
		return lis;
	}
	
	/**
	 * Adds segment id and stream size
	 * @param lst
	 */
	private void tagSegmentExtraData(List<ByteArrayOutputStream> lst, int count)
	{
		ByteArrayOutputStream baos;
		for (int i = 0; i < lst.size(); i++) {
			baos = lst.get(i);
			baos.write(i);
			baos.write(count);
		}
	}
	
	private float[] getMatrixRow(Matrix mat, int row)
	{
		double[][] d = mat.getArray();
		float[] arr = new float[mat.getRowDimension()];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = (float)d[i][row];
		}
		
		return arr;
	}
	
	private Matrix segmentToMatrix(int[] segment)
	{
		double[][] dvalues = new double[segment.length][1];
		for (int i = 0; i < segment.length; i++) {
			dvalues[i][0] = segment[i];
		}
		
		Matrix mat = new Matrix(dvalues);
		
		return mat;
	}
	
	float[] getNextBytesFromSegments(List<InputStream> lst) throws IOException
	{
		//byte[] ba = new byte[4];
		byte[] ba = new byte[CONVERSION_SIZE];
		float[] f = new float[lst.size()];
		int index = 0;
		for(InputStream is : lst) {
			is.read(ba);
			f[index] = (((float)Util.bytesToShort(ba)));
			f[index] -= FIX_POINT_SHIFT; //This uses the full range of short
			f[index] /= FIX_POINT_FACTOR;
			//dError = dResult - dError;
			index++;
			//f[index++] = (((float)Util.bytesToShort(ba)) / FIX_POINT_FACTOR);
		}
		return f;
	}
	
	public void split(ArrayBlockingQueue<Integer> in, BlockingQueue<Integer>[] out, 
			IdaInfo info){
	}
	
	public void combine(BlockingQueue<int[]>[] in, 
			ArrayBlockingQueue<Integer> out, IdaInfo info){
	}
	
	@Override
	public int split(byte[] in, int intOffset, int inLen, IdaInfo info, byte[][] out, int outOffset, SliceDigest[] mds)
		throws IDAException{
		return 0;
	}
	
	@Override
	public int combine(byte[][] slices, int sliceOffset, int sliceLen, IdaInfo info, byte[] out, int outOffset)
		throws IDAException{
		return 0;
	}
}
