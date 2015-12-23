package sg.edu.nyp.sit.svds.client.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.ClientProperties;

public class MasterAuthenticationFactory {
	public static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(MasterAuthenticationFactory.class);
	private static IMasterAuthentication INSTANCE=null;
	
	private static final String AUTHENTICATION_CLASS_PROP="client.master.authentication";
	
	public static IMasterAuthentication getInstance(){
		String clsName=ClientProperties.getString(AUTHENTICATION_CLASS_PROP);
		if(clsName.equalsIgnoreCase("NONE"))
			return null;
		
		if(INSTANCE !=null)
			return INSTANCE;
		
		try{
			@SuppressWarnings("rawtypes")
			Class cls=Class.forName(clsName);
			INSTANCE=(IMasterAuthentication)cls.newInstance();
			return INSTANCE;
		}catch(Exception ex){
			LOG.error(ex);
			return null;
		}
	}
}
