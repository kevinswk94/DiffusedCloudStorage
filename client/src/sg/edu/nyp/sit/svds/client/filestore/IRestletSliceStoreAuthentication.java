package sg.edu.nyp.sit.svds.client.filestore;

import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public interface IRestletSliceStoreAuthentication
{
	public static final long serialVersionUID = 1L;

	public Object generateAuthentication(FileSliceServerInfo fs, Object o) throws SVDSException;
}
