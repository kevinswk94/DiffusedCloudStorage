package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred when the invoked method is not supported by the current mode 
 * (E.g calling a streaming write method on file slice store interface when the underlying
 * implementation does not support random write).
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class NotSupportedSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public NotSupportedSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public NotSupportedSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public NotSupportedSVDSException(Exception innerException){
		super(innerException);
	}
}
