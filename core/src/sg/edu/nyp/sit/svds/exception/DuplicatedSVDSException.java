package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred because the information already exist and is therefore duplicated. 
 * Applies to file names etc.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class DuplicatedSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public DuplicatedSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public DuplicatedSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public DuplicatedSVDSException(Exception innerException){
		super(innerException);
	}
}
