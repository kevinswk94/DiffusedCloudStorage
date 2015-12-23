package sg.edu.nyp.sit.svds.client;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;

public class ClientProperties {
	public static final long serialVersionUID = 3L;
	
	private static final Log LOG = LogFactory.getLog(ClientProperties.class);
	
	public static final String CLIENT_MODE_PVFS="PVFS";
	public static final String CLIENT_MODE_SVDS="SVDS";
	
	public enum PropName{
		FILE_LOCK_INTERVAL ("file.lock.interval"),
		FILE_SLICE_BLK_SIZE ("file.slice.blkSize"),
		FILE_VERIFY_CHECKSUM ("file.slice.checksum"),
		FILE_RANDOM_ALGO ("file.random.algorithm"),
		FILE_RANDOM_SIZE ("file.random.size"),
		FILE_SUPPORT_MODE ("client.file.mode"),
		CLIENT_MODE("client.mode"),
		SLICESTORE_USE_SHARED_ACCESS("client.slicestore.sharedaccess");
		
		private String name;
		PropName(String name){ this.name=name; }
		public String value(){ return name; }
	}
			
	private static boolean init=false;
	private static Properties prop=new Properties();
	
	public static void load(String path) throws Exception{
		FileInputStream in=new FileInputStream(path);
		prop.load(in);
		in.close();
		in=null;
	}
	
	public static void load(java.io.File f) throws Exception{
		FileInputStream in=new FileInputStream(f);
		prop.load(in);
		in.close();
		in=null;
	}
	
	public static void init(){
		try{
			//prop.load(ClientProperties.class.getClassLoader().getResourceAsStream("svdsclient.properties"));
			FileInputStream in=new FileInputStream(Resources.findFile("svdsclient.properties"));
			prop.load(in);
			in.close();
			in=null;
			init=true;
		}catch(Exception ex){
			LOG.error(ex);
			throw new NullPointerException(ex.getMessage());
		}
	}
	
	public static String getString(String propName){
		if(!init)init();
		
		return prop.get(propName).toString();
	}
	
	public static String getString(PropName prop){
		return getString(prop.value());
	}
	
	public static boolean getBool(String propName){
		if(!init)init();
		
		String tmp=prop.get(propName).toString();
		if(tmp.equalsIgnoreCase("off") || tmp.equalsIgnoreCase("0"))
			return false;
		else
			return true;
	}
	
	public static boolean getBool(PropName prop){
		return getBool(prop.value());
	}
	
	public static int getInt(String propName){
		if(!init)init();
		
		if(!prop.containsKey(propName))
			return 0;
		
		return Integer.parseInt(prop.get(propName).toString());
	}
	
	public static int getInt(PropName prop){
		return getInt(prop.value());
	}
	
	public static long getLong(String propName){
		if(!init)init();
		
		if(!prop.containsKey(propName))
			return 0;
		
		return Long.parseLong(prop.get(propName).toString());
	}
	
	public static long getLong(PropName prop){
		return getLong(prop.value());
	}
	
	public static void set(String propName, Object value){
		if(!init)init();
		
		prop.put(propName, value);
	}
	
	public static void set(PropName prop, Object value){
		set(prop.value(), value);
	}
	
	public static boolean exist(String propName){
		return prop.containsKey(propName);
	}
	
	public static boolean exist(PropName prop){
		return exist(prop.value());
	}
	
	public static void remove(String propName){
		prop.remove(propName);
	}
	
	public static void remove(PropName prop){
		remove(prop.value());
	}
}
