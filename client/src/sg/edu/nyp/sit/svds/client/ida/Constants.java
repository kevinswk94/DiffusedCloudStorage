package sg.edu.nyp.sit.svds.client.ida;

/***
 * Constant values used in IDA component
 * @author Law Chee Yong
 *
 */
public class Constants {
	public static final long serialVersionUID = 1L;
	
	//(p) Prime number > range of possible value of element in file
	//for byte, p can be 257
	public static final int MODULUS_P = 257;	
	
	//(n) The number of slice to be created
//	public static final int SLICE_COUNT = 5;
	
	//(m) slice size (arbitrarily selected for now)
	//public static final int SLICE_LENGTH = 8;

	///(k), if > LOST_THRESHOLD slices are lost, cannot be recovered
	//public static final int LOST_THRESHOLD = SLICE_COUNT - SLICE_LENGTH;
//	public static final int LOST_THRESHOLD = 2;
	
	//public static final int FIXED_POINT = 512;
}
