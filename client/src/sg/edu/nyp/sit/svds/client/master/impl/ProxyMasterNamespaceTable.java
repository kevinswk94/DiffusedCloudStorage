package sg.edu.nyp.sit.svds.client.master.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterAuthenticationFactory;
import sg.edu.nyp.sit.svds.exception.LockedSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.RestletProxyQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class ProxyMasterNamespaceTable implements IMasterNamespaceTable {
	public static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(ProxyMasterNamespaceTable.class);
	
	private static final int AUTH_FAIL=417;
	
	private IMasterAuthentication auth=null;
	private final String host;
	private final String connector;
	
	//no of times to retry if the request to external servers failed
	private int retryTimes=10;
	private int retryPause=5000;	//5 seconds
	
	public ProxyMasterNamespaceTable(String host, String connector) {
		this.host=host;
		this.connector=connector;
		auth=MasterAuthenticationFactory.getInstance();
	}
	
	@Override
	public NamespaceInfo getNamespaceMemory(String namespace, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry, success=false;
		NamespaceInfo rec;
		do{
			toRetry=false;
			rec=new NamespaceInfo(namespace, 0,0);
			success=getNSMem(namespace, rec, usr);
			
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
	
	private boolean getNSMem(String namespace, NamespaceInfo ni, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;

		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8");
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));

			URL fsUrl = new URL(connector+"://" + host + "/slicestore/mem?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();

			fsConn.setDoInput(true);

			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
				default:
					return false;
			}

			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String mem=in.readLine();
			in.close();

			//set a default of 1GB availability
			ni.setMemoryAvailable(104857600L);
			ni.setMemoryUsed(Long.parseLong(mem));

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
	public void refreshRestletSliceServerKey(String svrId, User usr)
			throws SVDSException {
		//no need to implement this method because the authentication for stand alone 
		//slice store is not required in PVFS
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

	@Override
	public String[] getSharedAccessURL(String svrId, String sliceName, User usr)
			throws SVDSException {
		//no need to implement this method because shared access URL is not implemented for PVFS
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
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
			success=getAvaFS(servers, usr);
			
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
	
	private boolean getAvaFS(List<FileSliceServerInfo> stores, User usr) 
		throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8");
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
			
			URL fsUrl = new URL(connector+"://" + host + "/slicestore/available?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
			
			//data.list(System.out);
			
			int cnt=Integer.parseInt(data.get(FileSliceServerInfo.PropName.COUNT.value()).toString());
			FileSliceServerInfo fssi;
			for(int i=0; i<cnt; i++){
				fssi=new FileSliceServerInfo(data.get(FileSliceServerInfo.PropName.ID.value()+i).toString());
				fssi.setStatus(FileSliceServerInfo.Status.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.STATUS.value()+i).toString())));
				stores.add(fssi);
			}
			
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
	public void removeSliceServer(String namespace, String svrId, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=remFS(svrId, usr);
			
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
	
	private boolean remFS(String svrId, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_ID.value()+"="+URLEncoder.encode(svrId, "UTF-8");
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
			
			URL fsUrl = new URL(connector+"://" + host + "/slicestore/remove?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_CONFLICT:
					throw new LockedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
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
			success=getFS(svrId, fssi, usr);
			
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
	
	private boolean getFS(String svrId, FileSliceServerInfo fssi, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_ID.value()+"="+URLEncoder.encode(svrId, "UTF-8");
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
		
			URL fsUrl = new URL(connector+"://" + host + "/slicestore/get?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
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
				if(k.getKey().toString().startsWith(FileSliceServerInfo.PropName.OPT.value()+".")){
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
	public void updateSliceServer(FileSliceServerInfo fssi, User usr)
			throws SVDSException {
		int retryCnt=0;
		boolean toRetry;
		boolean success;
		do{
			toRetry=false;
			success=updateFS(fssi, usr);
			
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
			throw new SVDSException("Error updating slice store.");
	}
	
	private boolean updateFS(FileSliceServerInfo fssi, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;

		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_ID.value()+"="+URLEncoder.encode(fssi.getServerId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_HOST.value()+"="+URLEncoder.encode(fssi.getServerHost(), "UTF-8")
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_TYPE.value()+"="+fssi.getType().value()
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_MODE.value()+"="+fssi.getMode().value()
				+"&"+RestletProxyQueryPropName.SliceStore.SVR_STATUS.value()+"="+fssi.getStatus().value()
				+(fssi.getKeyId()==null ? "" : "&"+RestletProxyQueryPropName.SliceStore.SVR_KEY_ID.value()+"="
						+URLEncoder.encode(fssi.getKeyId(), "UTF-8"))
				+(fssi.getKey()==null? "" : "&"+RestletProxyQueryPropName.SliceStore.SVR_KEY.value()+"="
						+URLEncoder.encode(fssi.getKey(), "UTF-8"));
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));

			URL fsUrl = new URL(connector+"://" + host + "/slicestore/register?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();

			if(fssi.hasProperties()){
				fsConn.setDoOutput(true);
				
				OutputStream out=fsConn.getOutputStream();
				for(@SuppressWarnings("rawtypes") Map.Entry k: fssi.getAllProperties().entrySet()){
					out.write((k.getKey().toString()+"="+Resources.encodeKeyValue(k.getValue().toString())+"\n").getBytes());
				}
				out.close();
			}

			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
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
	public String getNamespace(User usr) throws SVDSException {
		int retryCnt=0;
		boolean toRetry;
		String ns=null;
		do{
			toRetry=false;
			ns=getNS(usr);
			
			if(ns==null	&& retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(ns==null)
			throw new SVDSException("Error getting namespace memory usage.");
		
		return ns;
	}
	
	private String getNS(User usr) throws SVDSException{
		HttpURLConnection fsConn=null;

		try{
			String query=RestletProxyQueryPropName.SliceStore.SUBSCRIBER.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8");
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));

			URL fsUrl = new URL(connector+"://" + host + "/slicestore/get_ns?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();

			fsConn.setDoInput(true);

			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case AUTH_FAIL:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_FOUND:
				case HttpURLConnection.HTTP_FORBIDDEN:
					throw new RejectedSVDSException(RejectedSVDSException.PROXY, fsConn.getResponseMessage());
				default:
					return null;
			}

			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String ns=in.readLine();
			in.close();
			
			return ns;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return null;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public void updateNamespace(String namespace, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

	@Override
	public long getReconTimeout(User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

	@Override
	public void updateReconTimeout(long interval, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

	@Override
	public boolean isAuthReq(User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

	@Override
	public void updateAuthReq(boolean req, String newPwd, User usr) throws SVDSException {
		throw new NotSupportedSVDSException("Method not implemented in proxy version.");
	}

}
