package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.security.MessageDigest;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.filestore.IRestletSliceStoreAuthentication;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class RestletSliceStoreAuthenticationByKey implements
		IRestletSliceStoreAuthentication {
	public static final long serialVersionUID = 2L;
	
	private static final String SIGNATURE_QUERY_PROP="s";
	
	@Override
	public Object generateAuthentication(FileSliceServerInfo fs, Object o)
			throws SVDSException {
		if(fs.getKey()==null)
			return "";
		
		String query=(String)o;
		
		try{
			return "&"+SIGNATURE_QUERY_PROP+"="+Resources.convertToHex(
					(MessageDigest.getInstance(Resources.HASH_ALGO)).digest((query+fs.getKey()).getBytes()));
		}catch(Exception ex){
			throw new SVDSException(ex);
		}
	}

}
