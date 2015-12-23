package sg.edu.nyp.sit.svds.client.ida;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

public class DummyInfoDispersalImpl implements IInfoDispersal {
	public static final long serialVersionUID = 1L;
	
	private int fixedSize=100*1024; //100KB
	
	@Override
	public List<InputStream> split(InputStream is, IdaInfo info) throws IDAException{
		ArrayList<InputStream> slices=new ArrayList<InputStream>();
		
		try{
			byte[] data=new byte[fixedSize];
			int len;
			while((len=is.read(data))!=-1){
				slices.add(new ByteArrayInputStream(data, 0, len));
			}
		}catch(Exception ex){
			throw new IDAException(ex.getMessage());
		}
		
		return slices;
	}
	
	@Override
	public InputStream combine(List<InputStream> is, IdaInfo info) throws IDAException{
		//note that the item in the array must be in the order in the split
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		ByteArrayInputStream in=null;
		byte[] data=new byte[512];
		int len;
		try{
			for(InputStream i: is){
				while((len=i.read(data))!=-1){
					out.write(data, 0, len);
				}
			}
			
			in=new ByteArrayInputStream(out.toByteArray());
			out.close();
		}catch(Exception ex){
			throw new IDAException(ex.getMessage());
		}
	
		out=null;
		
		return in;
	}
	
	@Override
	public void split(ArrayBlockingQueue<Integer> in, BlockingQueue<Integer>[] out, 
			IdaInfo info){
	}
	
	@Override
	public void combine(BlockingQueue<int[]>[] in, 
			ArrayBlockingQueue<Integer> out, IdaInfo info){
	}
	
	@Override
	public int split(byte[] in, int inOffset, int inLen, IdaInfo info, byte[][] out, int outOffset, SliceDigest[] mds)
		throws IDAException{
		return 0;
	}
	
	@Override
	public int combine(byte[][] slices, int sliceOffset, int sliceLen, IdaInfo info, byte[] out, int outOffset)
		throws IDAException{
		return 0;
	}
}
