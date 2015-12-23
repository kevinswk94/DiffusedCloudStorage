package sg.edu.nyp.sit.svds.metadata;

/**
 * Contains enumerations for transmitting and receiving data to and from the master application.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class RestletMasterQueryPropName {
	public static final long serialVersionUID = 3L;
	
	/**
	 * Names for the file properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum File{
		/**
		 * Namespace the file belongs to
		 */
		NAMESPACE ("namespace"),
		/**
		 * Sequence of the file slice in the file
		 */
		SEQ ("seq"),
		/**
		 * Owner of the file
		 */
		OWNER ("owner"),
		/**
		 * Type of the file
		 */
		TYPE ("type"),
		/**
		 * Size of the file in bytes
		 */
		SIZE ("size"),
		/**
		 * IDA version used in the file transformation
		 */
		IDA_VERSION ("version"),
		/**
		 * Block size used to calculate the block hashes for the file slice contents
		 */
		FILE_BLKSIZE ("blksize"),
		/**
		 * Salt value used to caculate the block hashes for the file slice contents
		 */
		FILE_KEYHASH ("key"),
		/**
		 * The path used to identify the file
		 */
		PATH ("path"),
		/**
		 * The user that is making the file request
		 */
		USER ("by"),
		/**
		 * The existing namespace of the file
		 */
		OLD_NAMESPACE("old_namespace"),
		/**
		 * The existing path used to identify the file
		 */
		OLD_PATH("old_filename"),
		/**
		 * The requested mode to open the file
		 */
		MODE("mode"),
		/**
		 * Date time the directory was last checked for changes
		 */
		LAST_CHECK("lastChk"),
		/**
		 * Status of the file
		 */
		STATUS ("status");
		
		private String name;
		File(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	/**
	 * Names for the namespace properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Namespace{
		/**
		 * Name of the namespace
		 */
		NAMESPACE ("name"),
		/**
		 * ID of the slice store server registering/registered to the namespace/to be removed from the namespace
		 */
		SVR_ID("id"),
		/**
		 * URL of the slice store server registering to the namespace
		 */
		SVR_HOST("ip"),
		/**
		 * Type of the slice store server registering to the namespace
		 */
		SVR_TYPE("svr_type"),
		/**
		 * Supported mode of the slice store server registering to the namespace
		 */
		SVR_MODE("mode"),
		/**
		 * If the slice store server any request to be verified. Applies to SVDS slice store server only
		 */
		SVR_REQ_VERIFY("req_ver"),
		/**
		 * Available memory of the namespace
		 */
		MEM("mem"),
		/**
		 * Status of the slice store server registering to the namespace
		 */
		SVR_STATUS("status"),
		/**
		 * Name of the file slice
		 */
		SLICE_NAME("slice");
		
		private String name;
		Namespace(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
}
