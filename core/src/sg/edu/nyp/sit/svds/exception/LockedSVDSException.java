package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred when the user attempts to perform write related operations (including file change mode)
 * when the file is currently locked by another user or system (for maintainence purposes).
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class LockedSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public LockedSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public LockedSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public LockedSVDSException(Exception innerException){
		super(innerException);
	}
}
