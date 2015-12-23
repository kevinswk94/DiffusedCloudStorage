package sg.edu.nyp.sit.svds.metadata;

/**
 * Contains enumerations for transmitting and receiving data to and from the SVDS slice store server application.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class RestletFileSliceServerQueryPropName {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Values for status properties when transmitting and receiving data to and from the SVDS slice store server application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum StatusReq{
		/**
		 * To check the status from the SVDS slice store server application
		 */
		STATUS_ALIVE (1),
		/**
		 * To check the available physcial memory from the SVDS slice store server application
		 */
		AVA_STORAGE (2),
		/**
		 * To get authentication key for for SVDS slice store server application
		 */
		KEY (3);
		
		private int value;
		StatusReq(int value){this.value=value;}
		/**
		 * Gets the property value.
		 * 
		 * @return Value of the property
		 */
		public int value(){return value;}
		
		/**
		 * Gets the enum based on the integer value of the property
		 * 
		 * @param status Integer value of the property
		 * @return Enum representing the value of the property or null if nothing matches
		 */
		public static StatusReq valueOf(int value){
			switch(value){
				case 1: return STATUS_ALIVE;
				case 2: return AVA_STORAGE;
				case 3: return KEY;
				default: return null;
			}
		}
	}
	
	/**
	 * Names for the status properties when transmitting and receiving data to and from the SVDS slice store server application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Status{
		/**
		 * To request for information from the SVDS slice store server
		 */
		INFO("info");
		
		private String name;
		Status(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	/**
	 * Names for the file slice properties when transmitting and receiving data to and from the SVDS slice store server application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Slice{
		/**
		 * Name of the file slice
		 */
		NAME("slicename"),
		/**
		 * Offset within the file slice
		 */
		OFFSET("offset"),
		/**
		 * Aggregated checksum of the file size contents
		 */
		CHECKSUM("checksum"),
		/**
		 * The block size used to calculate the block hashes for the file size contents
		 */
		FILE_BLKSIZE ("blksize"),
		/**
		 * The salt used to calculate the block hashes for the file size contents
		 */
		FILE_KEYHASH ("key"),
		/**
		 * Size of the file slice
		 */
		LENGTH("len");
		
		private String name;
		Slice(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
}
