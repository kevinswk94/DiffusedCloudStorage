package sg.edu.nyp.sit.svds.client.ida;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

public interface IInfoDispersal {
	public static final long serialVersionUID = 1L;
	
	public static final int ERROR_CODE=-2;
	/**
	 * The split method splits array of byte data into n number of slices.
	 * @param is The input stream of the byte of data.
	 * @param size The size of the array of byte data 
	 * @return A list of slices (each slide is read from the InputStream).
	 * Note that each number of slices can be inferred from the size of the List object.
	 */
	List<InputStream> split(InputStream is, IdaInfo info) throws IDAException;
	InputStream combine(List<InputStream> is, IdaInfo info) throws IDAException;
	
	//method for streaming read/write
	void split(ArrayBlockingQueue<Integer> in, BlockingQueue<Integer>[] out, 
			IdaInfo info);
	void combine(BlockingQueue<int[]>[] in, ArrayBlockingQueue<Integer> out, 
			IdaInfo info);

	int combine(byte[][] slices, int sliceOffset, int sliceLen, IdaInfo info, byte[] out, int outOffset) throws IDAException;
	int split(byte[] in, int intOffset, int inLen, IdaInfo info, byte[][] out, int outOffset, SliceDigest[] mds) throws IDAException;
}
