package sg.edu.nyp.sit.pvfs.svc;

/**
 * Events that occurs between the PC and the mobile application. For PVFS only.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public enum PVFSEvents {
	/**
	 * New connection
	 */
	NEW_CONN,
	/**
	 * Re-connection
	 */
	RE_CONN,
	/**
	 * Authentication request. Happens when authentication is required.
	 */
	AUTH_REQ,
	/**
	 * Authentication reply. Reponse to authentication request
	 */
	AUTH_REP,
	/**
	 * Authentication retry. Happens when the password entered is wrong
	 */
	AUTH_RETRY,
	/**
	 * Invalid authentication. Happens when the password entered is wrong and has exceeded the number of tries
	 */
	AUTH_IVD,
	/**
	 * Authentication OK
	 */
	AUTH_OK,
	/**
	 * Initialization done. Happens after authentication is successful or authentication is not required
	 */
	INIT_READY,
	/**
	 * Shutdown request. This shutdown refers to shutting down the bluetooth connection
	 */
	SHUTDOWN_REQ,
	/**
	 * File metadata request
	 */
	OP_FILE_REQ,
	/**
	 * File metadata reply. Reponse to file metadata request
	 */
	OP_FILE_REP,
	/**
	 * Namespace query request
	 */
	OP_NAMESPACE_REQ,
	/**
	 * Namespace query reply. Response to namespace query request
	 */
	OP_NAMESPACE_REP,
	/**
	 * To check if the other party is still connected
	 */
	PING,
	STREAM_ERR,
	/**
	 * C2DM registration ID request
	 */
	C2DM_REG_ID_REQ,
	/**
	 * C2DM registration ID reply. Response to C2DM registration request
	 */
	C2DM_REG_ID_REP;
	
	/**
	 * Gets the enum based on the string representation.
	 * 
	 * @param str String representation of the event.
	 * @return Enum that matches the string representation of the event
	 * @throws NullPointerException Occurs when no match can be found
	 */
	public static PVFSEvents get(String str){
		str=str.trim().toLowerCase();
		
		if(str.equalsIgnoreCase(NEW_CONN.toString()))
			return NEW_CONN;
		if(str.equalsIgnoreCase(RE_CONN.toString()))
			return RE_CONN;
		else if(str.equalsIgnoreCase(AUTH_REQ.toString()))
			return AUTH_REQ;
		else if(str.equalsIgnoreCase(AUTH_REP.toString()))
			return AUTH_REP;
		else if(str.equalsIgnoreCase(AUTH_RETRY.toString()))
			return AUTH_RETRY;
		else if(str.equalsIgnoreCase(AUTH_IVD.toString()))
			return AUTH_IVD;
		else if(str.equalsIgnoreCase(AUTH_OK.toString()))
			return AUTH_OK;
		else if(str.equalsIgnoreCase(INIT_READY.toString()))
			return INIT_READY;
		else if(str.equalsIgnoreCase(SHUTDOWN_REQ.toString()))
			return SHUTDOWN_REQ;
		else if(str.equalsIgnoreCase(OP_FILE_REQ.toString()))
			return OP_FILE_REQ;
		else if(str.equalsIgnoreCase(OP_FILE_REP.toString()))
			return OP_FILE_REP;
		else if(str.equalsIgnoreCase(OP_NAMESPACE_REQ.toString()))
			return OP_NAMESPACE_REQ;
		else if(str.equalsIgnoreCase(OP_NAMESPACE_REP.toString()))
			return OP_NAMESPACE_REP;
		else if(str.equalsIgnoreCase(PING.toString()))
			return PING;
		else if(str.equalsIgnoreCase(STREAM_ERR.toString()))
			return STREAM_ERR;
		else if(str.equalsIgnoreCase(C2DM_REG_ID_REQ.toString()))
			return C2DM_REG_ID_REQ;
		else if(str.equalsIgnoreCase(C2DM_REG_ID_REP.toString()))
			return C2DM_REG_ID_REP;
		
		throw new NullPointerException("Invalid event");
	}
}
