package sg.edu.nyp.sit.svds.exception;

public class UnauthorizedSharedAccessSVDSException extends SVDSException {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Creates a empty exception
	 */
	public UnauthorizedSharedAccessSVDSException(){
		super();
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public UnauthorizedSharedAccessSVDSException(String msg){
		super (msg);
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public UnauthorizedSharedAccessSVDSException(Exception innerException){
		super(innerException);
	}
}
