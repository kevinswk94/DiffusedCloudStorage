package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred during file change mode operation.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class ChangeModeSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public ChangeModeSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public ChangeModeSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public ChangeModeSVDSException(Exception innerException){
		super(innerException);
	}
}