package sg.edu.nyp.sit.svds.client.master.impl;

import java.net.URLEncoder;

import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.metadata.User;

public class MasterAuthenticationByLDAP implements IMasterAuthentication {
	public static final long serialVersionUID = 1L;
	
	private static final String UID_PROP="uid";
	private static final String PWD_PROP="pwd";
	
	public Object generateAuthentication(User u, Object o) throws Exception{
		return "&" + UID_PROP + "=" 
			+ URLEncoder.encode((u.getId()==null?"":u.getId()), "UTF-8")
			+ "&" + PWD_PROP + "="
			+ URLEncoder.encode((u.getPwd()==null?"":u.getPwd()), "UTF-8");		
	}
}
