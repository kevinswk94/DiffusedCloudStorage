package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred as a result of data corrupted. Most often due to mismatch checksum/hashes
 * between the retrieved value and the computed value.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class CorruptedSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public CorruptedSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public CorruptedSVDSException(String msg){
		super (msg);
	}
	

	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public CorruptedSVDSException(Exception innerException){
		super(innerException);
	}
}