package sg.edu.nyp.sit.svds.client;

import java.net.HttpURLConnection;
import java.net.URL;

import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;

@SuppressWarnings("unused")
public class TwoWaySSLTestApp {
	//client is running as hostname "victoriac" while master running as hostname "localhost"
	
	private static String client_truststore_path="D:\\Projects\\CloudComputing\\Certs\\moeifks_localClient.jks";
	private static String client_keystore_path="D:\\Projects\\CloudComputing\\Certs\\moeifks_victoriac.jks";

	public static void main(String[] args) throws Exception {
		System.setProperty("javax.net.ssl.trustStore", args[0]);
		System.setProperty("javax.net.ssl.trustStorePassword", "moeifssl");
		if(!args[1].equalsIgnoreCase("NA")){
			System.setProperty("javax.net.ssl.keyStore",args[1]);
			System.setProperty("javax.net.ssl.keyStorePassword","moeifssl");
		}

	    HttpURLConnection fsConn=null;
		
		try{
			String strUrl = "https://"+args[2]+"/namespace/mem?"+RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=abc" ;
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_NO_CONTENT:
				case HttpURLConnection.HTTP_OK:
					System.out.println("OK");
					break;
				default:
					System.out.println(fsConn.getResponseCode() + ":" + fsConn.getResponseMessage());
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

}
