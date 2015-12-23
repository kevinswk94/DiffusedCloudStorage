package sg.edu.nyp.sit.svds.exception;

/**
 * Exception that occurred when the a method is still invoked despite prior receiving the LastRequestSVDSException.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class RejectedSVDSException extends SVDSException {
	public static final long serialVersionUID = 2L;
	
	public static final int BLUETOOTH=1;
	public static final int PROXY=2;
	
	private int origin;
	
	/**
	 * Creates a empty exception
	 */
	public RejectedSVDSException(int origin){
		super();
		this.origin=origin;
	}
	
	/**
	 * Creates a exception with a message.
	 * 
	 * @param msg The message for the exception.
	 */
	public RejectedSVDSException(int origin, String msg){
		super (msg);
		this.origin=origin;
	}
	
	/**
	 * Creates a exception with a inner/base exception.
	 * 
	 * @param innerException
	 */
	public RejectedSVDSException(int origin, Exception innerException){
		super(innerException);
		this.origin=origin;
	}
	
	public int getOrigin(){
		return origin;
	}
}
