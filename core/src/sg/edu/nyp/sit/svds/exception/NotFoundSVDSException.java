package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred when the request information cannot be found. It can applies to file, meta data etc.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class NotFoundSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public NotFoundSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public NotFoundSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public NotFoundSVDSException(Exception innerException){
		super(innerException);
	}
}
