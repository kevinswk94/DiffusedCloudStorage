package sg.edu.nyp.sit.svds.exception;

/**
 * General SVDS exception.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class SVDSException extends Exception {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public SVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public SVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public SVDSException(Exception innerException){
		super(innerException);
	}
}
