package sg.edu.nyp.sit.svds.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Meta data for each slice store server
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class FileSliceServerInfo {
	public static final long serialVersionUID = 3L;
	
	public enum AzurePropName{
		RETRY_CNT ("azure.retrycnt"),
		RETRY_INTERVAL ("azure.retryinterval"),
		USE_DEVELOPMENT ("azure.usedevelopment");
		
		private String name;
		AzurePropName(String name){ this.name=name; }
		public String value(){ return name; }
	}
	
	public enum S3PropName{
		RETRY_CNT ("s3.retrycnt"),
		CONTAINER ("s3.bucket");

		private String name;
		S3PropName(String name){ this.name=name; }
		public String value(){ return name; }
	}
	
	public enum RestletPropName{
		STATUS_HOST ("slicestore.status.address"),
		STATUS_SSL ("slicestore.status.ssl");
		
		private String name;
		RestletPropName(String name){ this.name=name; }
		public String value(){ return name; }
	}
	/**
	 * Names for the slice store server properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum PropName{
		/**
		 * Number of slice store server object in the list
		 */
		COUNT ("svrMapCount"),
		/**
		 * ID of the slice store server
		 */
		ID ("svrId"),
		/**
		 * URL of the slice store server
		 */
		HOST ("svrHost"), 
		/**
		 * Type of the slice store server
		 */
		TYPE ("svrType"),
		/**
		 * Authentication key of the slice store server. Used by SVDS slice store server only
		 */
		KEY ("svrKey"),
		/**
		 * Other slice store server information
		 */
		OPT ("svrOpt"),
		/**
		 * Supported mode of the slice store server
		 */
		MODE ("svrMode"), 
		/**
		 * Status of the slice store server
		 */
		STATUS("svrStatus"),
		KEYID ("svrKeyId");
		
		private String name;
		PropName(String name){ this.name=name; }
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	/**
	 * Acceptable values for slice store server status
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Status{
		/**
		 * The slice store server is ready to accept request
		 */
		ACTIVE (1),
		/**
		 * The slice store server is not ready to accept request
		 */
		INACTIVE(0),
		/**
		 * The slice store server is not available
		 */
		DEAD (-1);
		
		private int status;
		Status(int status) {this.status=status;}
		/**
		 * Gets the integer value of the slice store server status
		 * 
		 * @return Integer value of the slice store server status
		 */
		public int value() {return status; }
		
		/**
		 * Gets the enum based on the integer value of the slice store server status
		 * 
		 * @param status Integer value of the slice store server status
		 * @return Enum representing the status of the slice store server 
		 */
		public static Status valueOf(int status){
			switch(status){
				case -1: return DEAD;
				case 0: return INACTIVE;
				case 1: return ACTIVE;
				default: return null;
			}
		}
	}
	
	/**
	 * Acceptable values for slice store server type
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum Type {
		/**
		 * SVDS slice store server
		 */
		RESTLET (1),
		/**
		 * Windows azure storage
		 */
		AZURE (2),
		/**
		 * Amazon S3 storage
		 */
		S3 (3);
		
		private int type;
		Type(int type) {this.type=type;}
		/**
		 * Gets the integer value of the slice store server type
		 * 
		 * @return Integer value of the slice store server type
		 */
		public int value() {return type;}
		
		/**
		 * Gets the enum based on the integer value of the slice store server type
		 * 
		 * @param status Integer value of the slice store server type
		 * @return Enum representing the type of the slice store server
		 */
		public static Type valueOf(int type){
			switch(type){
				case 1: return RESTLET;
				case 2: return AZURE;
				case 3: return S3;
				default: return null;
			}
		}
	}
	
	private String serverId=null;
	private String serverHost=null;
	//date time the file slice server notify the master table that it is active
	private Date lastChecked=null;
	private Status status=Status.ACTIVE;
	
	private Type type=Type.RESTLET;	//default to restlet type
	private FileIOMode mode=FileIOMode.NONE;
	
	private String key=null;
	private String keyId=null;
	
	private List<String> namespaces=null;
	
	private Properties props=null;
	
	/**
	 * Creates a slice store server object by using the ID
	 * 
	 * @param serverId ID of the slice store server
	 */
	public FileSliceServerInfo(String serverId){
		this.serverId=serverId;
		lastChecked=new Date();
	}
	
	/**
	 * Creates a slice store server object using the ID, URL, type and mode
	 *  
	 * @param serverId ID of the slice store server
	 * @param serverHost URL of the slice store server
	 * @param type Type of the slice store server
	 * @param mode Supported mode of the slice store server
	 */
	public FileSliceServerInfo(String serverId, String serverHost, Type type, FileIOMode mode){
		this.serverId=serverId;
		this.serverHost=serverHost;
		this.type=type;
		this.mode=mode;
		lastChecked=new Date();
	}
	
	/**
	 * Sets the URL of the slice store server
	 * 
	 * @param serverHost URL of the slice store server
	 */
	public void setServerHost(String serverHost) {
		this.serverHost = serverHost;
	}
	/**
	 * Gets the URL of the slice store server
	 * 
	 * @return URL of the slice store server
	 */
	public String getServerHost() {
		return this.serverHost;
	}
	
	/**
	 * Sets the status last checked date of the slice store server
	 * 
	 * @param lastChecked Date time when the slice store server status is enquired
	 */
	public void setLastChecked(Date lastChecked) {
		this.lastChecked = lastChecked;
	}
	/**
	 * Gets the status last checked date of the slice store server
	 * 
	 * @return Date time when the slice store server status is enquired
	 */
	public Date getLastChecked() {
		return this.lastChecked;
	}
	
	/**
	 * Sets the status of the slice store server
	 * 
	 * @param status Status of the slice store server
	 */
	public void setStatus(Status status) {
		this.status = status;
	}
	/**
	 * Gets the status of the slice store server
	 * 
	 * @return Status of the slice store server
	 */
	public Status getStatus() {
		return this.status;
	}

	/**
	 * Sets the ID of the slice store server
	 * 
	 * @param serverId ID of the slice store server
	 */
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	/**
	 * Gets the ID of the slice store server
	 * 
	 * @return ID of the slice store server
	 */

	public String getServerId() {
		return this.serverId;
	}

	/**
	 * Sets the type of the slice store server
	 * 
	 * @param type Type of the slice store server
	 */
	public void setType(Type type) {
		this.type = type;
	}
	/**
	 * Gets the type of the slice store server
	 * 
	 * @return Type of the slice store server
	 */
	public Type getType() {
		return type;
	}

	public synchronized void setKey(String key) {
		this.key = key;
	}
	public synchronized String getKey() {
		return key;
	}
	public synchronized void setKeyId(String keyId) {
		this.keyId = keyId;
	}
	public synchronized String getKeyId() {
		return keyId;
	}
	
	/**
	 * Sets the mode of the slice store server
	 * 
	 * @param mode Mode of the slice store server
	 */
	public void setMode(FileIOMode mode) {
		this.mode = mode;
	}
	/**
	 * Gets the mode of the slice store server
	 * 
	 * @return Mode of the slice store server
	 */
	public FileIOMode getMode() {
		return mode;
	}

	/**
	 * Gets the list of namespaces the slice store server is registered to. Only applies to SVDS.
	 * 
	 * @return List of namespaces the slice store server is registered to
	 */
	public List<String> getRegisteredNamespaces() {
		return this.namespaces;
	}
	/**
	 * Register a namespace to the slice store server. 
	 * 
	 * Once registered, the slice store server will be able to accept request(s) to store file slices from the namespace
	 * 
	 * @param namespace Namespace to register to the slice store server
	 */
	public void addRegisteredNamespace(String namespace){
		if(this.namespaces==null)
			this.namespaces=Collections.synchronizedList(new ArrayList<String>());
		
		if(!namespaces.contains(namespace))this.namespaces.add(namespace);
	}
	/**
	 * Remove a namespace from the slice store server. 
	 * 
	 * When a namespace is removed, the slice store server will no longer accept request(s) to store file slices but will
	 * still accept request to retrieve existing file slices
	 * 
	 * @param namespace Namespace to remove from the slice store server
	 */
	public void removeRegisteredNamespace(String namespace){
		if(this.namespaces==null)
			return;
		
		this.namespaces.remove(namespace);
	}
	
	/**
	 * Gets a specific property for the slice store server using the name
	 * 
	 * @param name Name of the property to get
	 * @return The value of the property or null if no property with the given name is found
	 */
	public String getProperty(String name){
		if(props==null || !props.containsKey(name))
			return null;
		
		return props.get(name).toString();
	}
	/**
	 * Add a new property for the slice store server. If a property with the given name already exist, the value of the property will be updated to the given value
	 * 
	 * @param name Name of the property to add/update
	 * @param value Value of the property to add/update
	 */
	public void setProperty(String name, Object value){
		if(props==null)
			props=new Properties();
		
		props.put(name, value);
	}
	/**
	 * Checks if a property with the given name exists
	 * 
	 * @param name Name of the property to check
	 * @return True if the property with the given name exists, false otherwise
	 */
	public boolean isPropertyExist(String name){
		if(props==null)
			return false;
		
		return props.containsKey(name);
	}
	/**
	 * Gets the entire list of properties for the slice store server
	 * 
	 * @return List of properties for the slice store server
	 */
	public Properties getAllProperties(){
		return props;
	}
	/**
	 * Copies all of the given properties to the existing one. If a property with the given name already exist, the value of the property will be updated to the given value
	 * 
	 * @param props List of properties to add/update
	 */
	public void setAllProperties(Properties props){
		if(this.props==null){
			this.props=new Properties();
		}
		
		this.props.putAll(props);
	}
	/**
	 * Removes all the properties for the slice store server
	 */
	public void clearAllProperties(){
		if(this.props==null)
			return;
		
		this.props.clear();
		this.props=null;
	}
	/**
	 * Checks if the slice store server has any properties
	 * 
	 * @return True if the slice store server has at least 1 property or false otherwise
	 */
	public boolean hasProperties(){
		return (props!=null && props.size()>0);
	}
}
