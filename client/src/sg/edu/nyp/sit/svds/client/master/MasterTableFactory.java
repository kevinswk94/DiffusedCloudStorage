package sg.edu.nyp.sit.svds.client.master;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.ClientProperties;

public class MasterTableFactory {
	public static final long serialVersionUID = 3L;
	
	private static final Log LOG = LogFactory.getLog(MasterTableFactory.class);
	private static IMasterFileTable FILE_INSTANCE = null;
	private static IMasterNamespaceTable NAMESPACE_INSTANCE = null;
	
	private static final String REST_FILE_IMPL="client.master.rest.file";
	private static final String REST_NAMESPACE_IMPL="client.master.rest.namespace";
	private static final String PROXY_FILE_IMPL="client.master.proxy.file";
	private static final String PROXY_NAMESPACE_IMPL="client.master.proxy.namespace";
	private static final String BLUETOOTH_FILE_IMPL="client.master.bluetooth.file";
	private static final String BLUETOOTH_NAMESPACE_IMPL="client.master.bluetooth.namespace";
	
	private static final String SSL_TRUSTSTORE_PROP="client.master.truststore";
	private static final String SSL_TRUSTSTORE_PWD="client.master.truststorepwd";
	private static final String SSL_TRUSTSTORE_TYPE="client.master.truststoretype";

	public static IMasterFileTable getFileInstance() {
		if (FILE_INSTANCE != null) 
			return FILE_INSTANCE;
		
		String mode=ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE);
		
		if(mode.equals(ClientProperties.CLIENT_MODE_SVDS))
			initRESTFileInstance();
		else if(mode.equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("File instance has not been initialized.");
		
		return FILE_INSTANCE;
	}
	
	public static IMasterNamespaceTable getNamespaceInstance() {
		if (NAMESPACE_INSTANCE != null) 
			return NAMESPACE_INSTANCE;
		
		String mode=ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE);
		
		if(mode.equals(ClientProperties.CLIENT_MODE_SVDS))
			initRESTNamespaceInstance();
		else if(mode.equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("File instance has not been initialized.");
		
		return NAMESPACE_INSTANCE;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initRESTFileInstance(){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS))
			throw new ExceptionInInitializerError("Mode is not defined as SVDS in properties file");
		
		String connector=ClientProperties.getString("client.master.rest.file.connector");

		try{
			//if it's trying to use a ssl connection, have to set the trust store etc
			if(connector.equalsIgnoreCase("https")){
				System.setProperty(Resources.TRUST_STORE, Resources.findFile(ClientProperties.getString(SSL_TRUSTSTORE_PROP)));
				System.setProperty(Resources.TRUST_STORE_PWD, ClientProperties.getString(SSL_TRUSTSTORE_PWD));
				System.setProperty(Resources.TRUST_STORE_TYPE, ClientProperties.getString(SSL_TRUSTSTORE_TYPE));
			}
			
			Class cls=Class.forName(ClientProperties.getString(REST_FILE_IMPL));
			//implementations of master table should take in 2 parameters else
			//will get exception
			Constructor con=cls.getConstructor(new Class[] {String.class, String.class});
			FILE_INSTANCE=(IMasterFileTable)con.newInstance(new Object[]{
					ClientProperties.getString("client.master.rest.file.host")+":"
						+ClientProperties.getString("client.master.rest.file.port")
					, connector});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initProxyFileInstance(){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("Mode is not defined as PVFS in properties file");
		
		String connector=ClientProperties.getString("client.master.proxy.connector");

		try{
			//if it's trying to use a ssl connection, have to set the trust store etc
			if(connector.equalsIgnoreCase("https")){
				System.setProperty(Resources.TRUST_STORE, Resources.findFile(ClientProperties.getString(SSL_TRUSTSTORE_PROP)));
				System.setProperty(Resources.TRUST_STORE_PWD, ClientProperties.getString(SSL_TRUSTSTORE_PWD));
				System.setProperty(Resources.TRUST_STORE_TYPE, ClientProperties.getString(SSL_TRUSTSTORE_TYPE));
			}
			
			Class cls=Class.forName(ClientProperties.getString(PROXY_FILE_IMPL));
			//implementations of master table should take in 2 parameters else
			//will get exception
			Constructor con=cls.getConstructor(new Class[] {String.class, String.class});
			FILE_INSTANCE=(IMasterFileTable)con.newInstance(new Object[]{
					ClientProperties.getString("client.master.proxy.host")+":"
						+ClientProperties.getString("client.master.proxy.port")
					, connector});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	//need to have this method as the bluetooth implementation needs to pass in
	//the input/output stream to the class before it can be used
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initBluetoothFileInstance(Runnable svc){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("Mode is not defined as PVFS in properties file");
		
		try{
			Class cls=Class.forName(ClientProperties.getString(BLUETOOTH_FILE_IMPL));
			Constructor con=cls.getConstructor(new Class[] {Runnable.class});
			FILE_INSTANCE=(IMasterFileTable)con.newInstance(new Object[]{svc});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initBluetoothNamespaceInstance(Runnable svc){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("Mode is not defined as PVFS in properties file");
		
		try{
			Class cls=Class.forName(ClientProperties.getString(BLUETOOTH_NAMESPACE_IMPL));
			Constructor con=cls.getConstructor(new Class[] {Runnable.class});
			NAMESPACE_INSTANCE=(IMasterNamespaceTable)con.newInstance(new Object[]{svc});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initRESTNamespaceInstance(){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_SVDS))
			throw new ExceptionInInitializerError("Mode is not defined as SVDS in properties file");
		
		String host=ClientProperties.getString("client.master.rest.namespace.host")+":"
			+ClientProperties.getString("client.master.rest.namespace.port");
		String connector=ClientProperties.getString("client.master.rest.namespace.connector");

		try{
			//if it's trying to use a ssl connection, have to set the trust store etc
			if(connector.equalsIgnoreCase("https")){
				System.setProperty("javax.net.ssl.trustStore", Resources.findFile(ClientProperties.getString(SSL_TRUSTSTORE_PROP)));
				System.setProperty("javax.net.ssl.trustStorePassword", ClientProperties.getString(SSL_TRUSTSTORE_PWD));
				System.setProperty("javax.net.ssl.trustStoreType", ClientProperties.getString(SSL_TRUSTSTORE_TYPE));
			}
			
			Class cls=Class.forName(ClientProperties.getString(REST_NAMESPACE_IMPL));
			//implementations of master table should take in 2 parameters else
			//will get exception
			Constructor con=cls.getConstructor(new Class[] {String.class, String.class});
			NAMESPACE_INSTANCE=(IMasterNamespaceTable)con.newInstance(new Object[]{host, connector});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void initProxyNamespaceInstance(){
		if(!ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_PVFS))
			throw new ExceptionInInitializerError("Mode is not defined as PVFS in properties file");
		
		String connector=ClientProperties.getString("client.master.proxy.connector");

		try{
			//if it's trying to use a ssl connection, have to set the trust store etc
			if(connector.equalsIgnoreCase("https")){
				System.setProperty(Resources.TRUST_STORE, Resources.findFile(ClientProperties.getString(SSL_TRUSTSTORE_PROP)));
				System.setProperty(Resources.TRUST_STORE_PWD, ClientProperties.getString(SSL_TRUSTSTORE_PWD));
				System.setProperty(Resources.TRUST_STORE_TYPE, ClientProperties.getString(SSL_TRUSTSTORE_TYPE));
			}
			
			Class cls=Class.forName(ClientProperties.getString(PROXY_NAMESPACE_IMPL));
			//implementations of master table should take in 2 parameters else
			//will get exception
			Constructor con=cls.getConstructor(new Class[] {String.class, String.class});
			NAMESPACE_INSTANCE=(IMasterNamespaceTable)con.newInstance(new Object[]{
					ClientProperties.getString("client.master.proxy.host")+":"
						+ClientProperties.getString("client.master.proxy.port")
					, connector});
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			throw new ExceptionInInitializerError(ex);
		}
	}
}
