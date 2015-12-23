package sg.edu.nyp.sit.svds.metadata;

public class RestletProxyQueryPropName {
	public static final String SIGNATURE="s";
	
	public enum Subscriber{
		ID("id"),
		KEY("key"),
		NAME("name"),
		EMAIL("email"),
		DT("dt"), //the request date time, usually used to different the signature generated from the request
		TOKEN("token");
		
		private String name;
		Subscriber(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	public enum Metadata{
		MEMORY("mem"),
		CURR_IDA_VERSION("ida"),
		DRIVE_NAME("drive");
		
		private String name;
		Metadata(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	public enum SliceStore{
		SUBSCRIBER("user"),
		SVR_ID("id"),
		SVR_HOST("ip"),
		SVR_TYPE("type"),
		SVR_MODE("mode"),
		SVR_KEY_ID("keyid"),
		SVR_KEY("key"),
		SVR_STATUS("status");
		
		private String name;
		SliceStore(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	public enum File{
		/**
		 * Sequence of the file slice in the file
		 */
		SEQ ("seq"),
		/**
		 * Owner of the file
		 */
		SUBSCRIBER ("user"),
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
		LAST_CHECK("lastChk");
		
		private String name;
		File(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
}
