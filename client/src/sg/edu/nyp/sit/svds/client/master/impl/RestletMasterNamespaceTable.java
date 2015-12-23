package sg.edu.nyp.sit.svds.client.master.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterAuthenticationFactory;
import sg.edu.nyp.sit.svds.exception.CorruptedSVDSException;
import sg.edu.nyp.sit.svds.exception.NotFoundSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class RestletMasterNamespaceTable implements IMasterNamespaceTable {
	public static final long serialVersionUID = 4L;
	
	private static final Log LOG = LogFactory.getLog(RestletMasterNamespaceTable.class);
	
	private IMasterAuthentication auth=null;
	private final String host;
	private final String connector;
	
	//no of times to retry if the request to external servers failed
	private int retryTimes=10;
	private int retryPause=5000;	//5 seconds
	
	public RestletMasterNamespaceTable(String host, String connector) {
		this.host=host;
		this.connector=connector;
		auth=MasterAuthenticationFactory.getInstance();
	}

	@Override
	public NamespaceInfo getNamespaceMemory(String namespace, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		NamespaceInfo rec;
		do{
			toRetry=false;
			rec=new NamespaceInfo(namespace, 0,0);
			success=getNamespaceMemory(namespace, rec);
			
			if(!success	&& retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				rec=null;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting namespace memory usage.");
		
		return rec;
	}
	
	private boolean getNamespaceMemory(String namespace, NamespaceInfo ni) 
		throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/mem?"
				+ RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=" 
				+ URLEncoder.encode(namespace, "UTF-8");
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NO_CONTENT:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
			
			if(!data.containsKey(NamespaceInfo.PropName.AVA_MEM.value()) ||
					!data.containsKey(NamespaceInfo.PropName.USED_MEM.value()))
				throw new CorruptedSVDSException("Missing return information.");
			
			ni.setMemoryAvailable(Long.parseLong(data.get(NamespaceInfo.PropName.AVA_MEM.value()).toString()));
			ni.setMemoryUsed(Long.parseLong(data.get(NamespaceInfo.PropName.USED_MEM.value()).toString()));
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			ex.printStackTrace();
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public void refreshRestletSliceServerKey(String svrId, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=getSliceServerKey(svrId, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to refresh slice store key.");
	}

	private boolean getSliceServerKey(String svrId, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/key?" 
				+ RestletMasterQueryPropName.Namespace.SVR_ID.value()+"=" + URLEncoder.encode(svrId, "UTF-8")
				+(auth==null || usr==null?"":auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NO_CONTENT:
					IFileSliceStore.updateServerKey(svrId, null, null);
					return true;
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String key=reader.readLine();
			reader.close();

			IFileSliceStore.updateServerKey(svrId, null, key);
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public List<FileSliceServerInfo> getAvailableSliceServers(String namespace, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry;
		boolean success;
		List<FileSliceServerInfo> servers=new ArrayList<FileSliceServerInfo>();
		do{
			toRetry=false;
			servers.clear();
			success=getAvailableSliceServers(namespace, servers);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting available slice stores.");
		
		return servers;
	}
	
	private boolean getAvailableSliceServers(String namespace, List<FileSliceServerInfo> stores) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/available?" 
				+ RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8");
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
 
			int cnt=Integer.parseInt(data.get(FileSliceServerInfo.PropName.COUNT.value()).toString());
			FileSliceServerInfo fssi;
			for(int i=0; i<cnt; i++){
				fssi=new FileSliceServerInfo(data.get(FileSliceServerInfo.PropName.ID.value()).toString());
				fssi.setServerHost(data.get(FileSliceServerInfo.PropName.HOST.value()).toString());
				stores.add(fssi);
			}
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void removeSliceServer(String namespace, String svrId, User usr) throws SVDSException {
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=removeServer(namespace, svrId);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error removing slice server.");		
	}
	
	private boolean removeServer(String namespace, String svrId) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/remove?" 
				+ RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&" + RestletMasterQueryPropName.Namespace.SVR_ID.value()+"="+URLEncoder.encode(svrId, "UTF-8");
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public FileSliceServerInfo getSliceServer(String svrId, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry;
		boolean success;
		FileSliceServerInfo fssi=new FileSliceServerInfo(svrId);
		do{
			toRetry=false;
			success=getSliceServer(svrId, fssi);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting slice store.");
		
		return fssi;
	}
	
	private boolean getSliceServer(String svrId, FileSliceServerInfo fssi) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/get_ss?" 
				+ RestletMasterQueryPropName.Namespace.SVR_ID.value()+"=" + URLEncoder.encode(svrId, "UTF-8");
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
 
			fssi.setServerHost(data.get(FileSliceServerInfo.PropName.HOST.value()).toString());
			fssi.setMode(FileIOMode.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.MODE.value()).toString())));
			fssi.setStatus(FileSliceServerInfo.Status.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.STATUS.value()).toString())));
			fssi.setType(FileSliceServerInfo.Type.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.TYPE.value()).toString())));
			
			if(data.containsKey(FileSliceServerInfo.PropName.KEYID.value())){
				fssi.setKeyId(data.get(FileSliceServerInfo.PropName.KEYID.value()).toString());
			}
			if(data.containsKey(FileSliceServerInfo.PropName.KEY.value())){
				fssi.setKey(data.get(FileSliceServerInfo.PropName.KEY.value()).toString());
			}
			
			int pos=(FileSliceServerInfo.PropName.OPT.value()+".").length();
			for(@SuppressWarnings("rawtypes") Map.Entry k: data.entrySet()){
				if(k.getKey().toString().equals(FileSliceServerInfo.PropName.OPT.value()+".")){
					fssi.setProperty(k.getKey().toString().substring(pos), k.getValue().toString());
				}
			}
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public String[] getSharedAccessURL(String svrId, String sliceName, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry;
		boolean success;
		List<String> URLs=new ArrayList<String>();
		do{
			toRetry=false;
			URLs.clear();
			success=getSharedAccessURL(svrId, sliceName, URLs);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting shared access URLs.");
		
		return URLs.toArray(new String[0]);
	}
	
	private boolean getSharedAccessURL(String svrId, String sliceName, List<String> URLs)
		throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/namespace/accessurl?" 
				+ RestletMasterQueryPropName.Namespace.SVR_ID.value()+"=" + URLEncoder.encode(svrId, "UTF-8")
				+ "&" + RestletMasterQueryPropName.Namespace.SLICE_NAME.value()+"="+ URLEncoder.encode(sliceName, "UTF-8");
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_BAD_METHOD:
					throw new NotSupportedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			
			String tmp;
			while((tmp=reader.readLine())!=null){
				if(tmp.length()>0) URLs.add(tmp);
			}
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void updateSliceServer(FileSliceServerInfo fssi, User usr)
			throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

	@Override
	public String getNamespace(User usr) throws SVDSException {
		//do not throw exception cos the SVDS can use virtual drive and it requires a label
		return ClientProperties.getString("client.virtualdisk.namespace");
	}

	@Override
	public void updateNamespace(String namespace, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

	@Override
	public long getReconTimeout(User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

	@Override
	public void updateReconTimeout(long interval, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

	@Override
	public boolean isAuthReq(User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

	@Override
	public void updateAuthReq(boolean req, String newPwd, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not supported in restlet version.");
	}

}
