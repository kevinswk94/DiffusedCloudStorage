package sg.edu.nyp.sit.svds.client.master.impl;

import java.security.MessageDigest;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.metadata.RestletProxyQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class MasterAuthenticationByToken implements IMasterAuthentication {
	public static final long serialVersionUID = 1L;
	
	@Override
	public Object generateAuthentication(User u, Object o) throws Exception {
		String query=(String)o;
		
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		return "&"+RestletProxyQueryPropName.SIGNATURE+"="
			+Resources.convertToHex(md.digest((query+u.getPwd()).getBytes()));
	}

}
