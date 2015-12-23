package sg.edu.nyp.sit.svds.metadata;

import java.util.*;

/**
 * Meta data for each namespace. Used in SVDS only.
 * 
 * A namespace functions like a virtual harddisk. It has a memory limit.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class NamespaceInfo {
	public static final long serialVersionUID = 1L;
	
	/**
	 * Names for the namespace properties when transmitting and receiving data to and from the master application.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	public enum PropName{
		/**
		 * Available memory in bytes
		 */
		AVA_MEM ("avaMem"),
		/**
		 * Used memory in bytes
		 */
		USED_MEM ("usedMem");
		
		private String name;
		PropName(String name){ this.name=name; }
		
		/**
		 * Gets the property name.
		 * 
		 * @return Name of the property
		 */
		public String value(){ return name; }
	}
	
	private List<String> serverIds=null;
	private String namespace=null;
	private long bytes_ava=0;
	private long bytes_used=0;

	/**
	 * Creates a namespace info object using the name of the namespace, the bytes available and bytes used
	 * 
	 * @param namespace Name of the namespace
	 * @param bytes_ava Available memory in bytes
	 * @param bytes_used Used memory in bytes
	 */
	public NamespaceInfo(String namespace, long bytes_ava, long bytes_used){
		this.namespace=namespace;
		this.bytes_ava=bytes_ava;
		this.bytes_used=bytes_used;
		
		this.serverIds=Collections.synchronizedList(new ArrayList<String>());
	}
	
	/**
	 * Gets the total memory of the namespace
	 * 
	 * @return Total memory in bytes
	 */
	public synchronized long getTotalMemory(){
		return bytes_ava+bytes_used;
	}
	/**
	 * Gets the available memory of the namespace
	 * 
	 * @return Available memory in bytes
	 */
	public synchronized long getMemoryAvailable(){
		return bytes_ava;
	}
	/**
	 * Gets the used memory of the namespace
	 * 
	 * @return Used memory in bytes
	 */
	public synchronized long getMemoryUsed(){
		return bytes_used;
	}
	
	/**
	 * Reduces the available memory by the given value
	 * 
	 * @param bytes Memory that is used in bytes
	 */
	public synchronized void useMemory(long bytes){
		bytes_ava-=bytes;
		bytes_used+=bytes;
	}
	/**
	 * Increases the available memory by the given value
	 * 
	 * @param bytes Memory that is freed in bytes
	 */
	public synchronized void freeMemory(long bytes){
		bytes_ava+=bytes;
		bytes_used-=bytes;
	}
	
	/**
	 * Sets the available memory of the namespace
	 * 
	 * @param bytes Available memory in bytes
	 */
	public void setMemoryAvailable(long bytes){
		bytes_ava=bytes;
	}
	/**
	 * Sets the used memory of the namespace
	 * 
	 * @param bytes Used memory in bytes
	 */
	public void setMemoryUsed(long bytes){
		bytes_used=bytes;
	}

	/**
	 * Gets the name of the namespace
	 * 
	 * @return Name of the namespace
	 */
	public String getNamespace() {
		return this.namespace;
	}

	/**
	 * Gets the list of slice store servers register to this namespace
	 * @return
	 */
	public List<String> getServers() {
		return this.serverIds;
	}
	/**
	 * Register a new slice store server to this namespace. If the slice store server is already registered, then nothing is done  
	 * 
	 * @param serverId ID of the slice store server to register to the namespace
	 */
	public void addFileSliceServer(String serverId){
		if(this.serverIds==null)
			this.serverIds=Collections.synchronizedList(new ArrayList<String>());
		
		if(!serverIds.contains(serverId))this.serverIds.add(serverId);
	}
	/**
	 * Remove an existing slice store server from the namespace
	 * 
	 * @param serverId ID of the slice store server to remove from the namespace
	 */
	public void removeFileSliceServer(String serverId){
		if(this.serverIds==null)
			return;
		
		this.serverIds.remove(serverId);
	}
	/**
	 * Checks if a slice store server is registered to the namespace
	 * 
	 * @param serverId ID of the slice store server to check
	 * @return True if the slice store server is registered to the namespace, false otherwise
	 */
	public boolean isFileSliceServerExist(String serverId){
		if(serverIds==null)
			return false;
		
		return serverIds.contains(serverId);
	}
}
