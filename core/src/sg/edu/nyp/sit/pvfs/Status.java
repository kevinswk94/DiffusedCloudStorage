package sg.edu.nyp.sit.pvfs;

/**
 * Status codes for C2DM and bluetooth communication.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class Status {
	public static final long serialVersionUID = 3L;
	
	public static final int ERR_BAD_REQUEST=400;
	public static final int ERR_NOT_ACCEPTABLE=406;
	public static final int ERR_INTERNAL=500;
	public static final int INFO_OK=200;
	public static final int INFO_CONTINUE=100;
	public static final int INFO_NO_MORE_CONTENT=101;
	public static final int ERR_CONFLICT=409;
	public static final int ERR_NOT_FOUND=404;
	public static final int ERR_FILE_LOCKED=423;
	
	public static final int ERR_C2DM_ACCOUNT_MISSING=801;
	public static final int ERR_C2DM_AUTHENTICATION_FAILED=802;
	public static final int ERR_C2DM_TOO_MANY_REGISTRATIONS=803;
	public static final int ERR_C2DM_INVALID_SENDER=804;
	public static final int ERR_C2DM_PHONE_REGISTRATION_ERROR=805;
}
