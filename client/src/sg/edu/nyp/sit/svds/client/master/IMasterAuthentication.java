package sg.edu.nyp.sit.svds.client.master;

import sg.edu.nyp.sit.svds.metadata.User;

public interface IMasterAuthentication {
	public static final long serialVersionUID = 1L;
	
	public Object generateAuthentication(User u, Object o) throws Exception;
}
