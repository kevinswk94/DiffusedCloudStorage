package sg.edu.nyp.sit.pvfs.svc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.client.master.MasterAuthenticationFactory;
import sg.edu.nyp.sit.svds.metadata.RestletProxyQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class ProxySvc {
	private static final String SSL_TRUSTSTORE_PROP="client.master.truststore";
	private static final String SSL_TRUSTSTORE_PWD="client.master.truststorepwd";
	private static final String SSL_TRUSTSTORE_TYPE="client.master.truststoretype";
	
	private IMasterAuthentication auth=null;
	private final String host;
	private final String connector;
	
	private static final int AUTH_FAIL=417;
	
	public ProxySvc(){
		connector=ClientProperties.getString("client.master.proxy.connector");
		host=ClientProperties.getString("client.master.proxy.host")+":"
			+ClientProperties.getString("client.master.proxy.port");
		
		//if it's trying to use a ssl connection, have to set the trust store etc
		if(connector.equalsIgnoreCase("https")){
			try{
				System.setProperty(Resources.TRUST_STORE, Resources.findFile(ClientProperties.getString(SSL_TRUSTSTORE_PROP)));
				System.setProperty(Resources.TRUST_STORE_PWD, ClientProperties.getString(SSL_TRUSTSTORE_PWD));
				System.setProperty(Resources.TRUST_STORE_TYPE, ClientProperties.getString(SSL_TRUSTSTORE_TYPE));
			}catch(Exception ex){ ex.printStackTrace(); }
		}
		
		auth=MasterAuthenticationFactory.getInstance();
	}
	
	public boolean auth(User usr) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.Subscriber.ID.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.Subscriber.DT.value()+"="+(new Date()).getTime();
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
			
			URL fsUrl = new URL(connector+"://" + host + "/subscriber/authRemote?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					return true;
				case HttpURLConnection.HTTP_NOT_FOUND:
				case AUTH_FAIL:
					return false;
				default:
					throw new Exception(fsConn.getResponseMessage());
			}
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	public void end(User usr) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.Subscriber.ID.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.Subscriber.DT.value()+"="+(new Date()).getTime();
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
			
			URL fsUrl = new URL(connector+"://" + host + "/subscriber/endRemote?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				default:
					throw new Exception(fsConn.getResponseMessage());
			}
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	public boolean checkEnded(User usr) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			String query=RestletProxyQueryPropName.Subscriber.ID.value()+"="+URLEncoder.encode(usr.getId(), "UTF-8")
				+"&"+RestletProxyQueryPropName.Subscriber.DT.value()+"="+(new Date()).getTime();
			query+=(auth==null || usr==null ? "" : auth.generateAuthentication(usr, query));
			
			URL fsUrl = new URL(connector+"://" + host + "/subscriber/chkRemote?" + query);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				default:
					throw new Exception(fsConn.getResponseMessage());
			}
			
			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String tmp=in.readLine();
			in.close();
			
			return (Integer.parseInt(tmp)<=0);
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
}
