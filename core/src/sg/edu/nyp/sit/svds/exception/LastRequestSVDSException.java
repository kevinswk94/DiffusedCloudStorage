package sg.edu.nyp.sit.svds.exception;

/**
 * Exception to inform the caller method that no furture request will be accepted.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class LastRequestSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public LastRequestSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public LastRequestSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public LastRequestSVDSException(Exception innerException){
		super(innerException);
	}
}
