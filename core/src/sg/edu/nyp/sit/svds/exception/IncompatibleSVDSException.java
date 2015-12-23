package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred because the mode that the user wants to open the file for reading/writing is not
 * supported by the one or multiple slice stores.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class IncompatibleSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public IncompatibleSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public IncompatibleSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public IncompatibleSVDSException(Exception innerException){
		super(innerException);
	}
}
