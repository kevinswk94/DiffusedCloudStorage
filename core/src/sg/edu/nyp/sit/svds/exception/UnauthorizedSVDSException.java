package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred when the user is not permitted to access the resource or user is not authenticated against
 * standalone slice store because of expired credentials.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class UnauthorizedSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public UnauthorizedSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public UnauthorizedSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public UnauthorizedSVDSException(Exception innerException){
		super(innerException);
	}
}
