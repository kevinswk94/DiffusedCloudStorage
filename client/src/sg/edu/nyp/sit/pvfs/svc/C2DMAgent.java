package sg.edu.nyp.sit.pvfs.svc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.HttpRetryException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.AccountException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class that acts as the third part application server in C2DM framework. See http://code.google.com/android/c2dm/
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class C2DMAgent {
	public static final long serialVersionUID = 1L;
	
	private static final Log LOG = LogFactory.getLog(C2DMAgent.class);
	
	private static final String senderID="nyp.cloud@gmail.com";
	private static final String senderPwd="cloudpass";
	
	private static String authToken=null;
	
	private static final int defaultRetryWait=5000;
	
	/**
	 * Gets an authentication token from the C2DM server in order to send messages. 
	 * This method will retry for a fix number of time until an exception is thrown.
	 * 
	 * @throws Exception Occurs when the authentication token cannot be retrieved after a fix number of retries
	 */
	public static void getAuthToken() throws Exception{
		boolean toContinue=true;
		int retryCnt=1;
		
		do{
			try{
				authToken();
				
				toContinue=false;
			}catch(HttpRetryException ex){
				//TODO: may need to implement UI to get the user to enter the captcha
				throw ex;
			}catch(ConnectException ex){
				try{Thread.sleep(defaultRetryWait*retryCnt);}catch(InterruptedException ie){}
				
				retryCnt++;
			}
		}while(toContinue);
	}
	
	/**
	 * Sends a REST web request to the C2DM server to get authentication token
	 * 
	 * @throws Exception Occurs when the authentication token cannot be retrieved
	 */
	private static void authToken() throws Exception{
		HttpsURLConnection conn=null;
		
		try{
			String reqData="accountType=GOOGLE"
				+ "&Email=" + URLEncoder.encode(senderID, "UTF-8")
				+ "&Passwd=" + URLEncoder.encode(senderPwd, "UTF-8")
				+ "&service=ac2dm"
				+ "&source=" + URLEncoder.encode("NYP-PVFS-" + serialVersionUID, "UTF-8");
			
			URL url = new URL("https://www.google.com/accounts/ClientLogin");
			conn=(HttpsURLConnection)url.openConnection();
			conn.setHostnameVerifier(new SSLVerifier());
			
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
			
			conn.setDoInput(true);
			conn.setDoOutput(true);

			OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
			wr.write(reqData);
		    wr.flush();

		    int respCode=conn.getResponseCode();
		    String respMsg=conn.getResponseMessage();

		    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			Properties respData=new Properties();
			respData.load(reader);
			reader.close();
			
			LOG.debug("auth token resp:"+respCode);
			
			if(respCode==HttpURLConnection.HTTP_OK){
				authToken=respData.get("Auth").toString();
				LOG.debug("auth token:"+authToken);
			}else if(respCode==HttpURLConnection.HTTP_FORBIDDEN){
				String err=respData.get("Error").toString();
				if(err.equals("BadAuthentication") || err.equals("NotVerified") || err.equals("AccountDeleted") 
						|| err.equals("AccountDisabled") || err.equals("TermsNotAgreed") || err.equals("ServiceDisabled")){
					//error with account
					throw new AccountException(err);
				}else if(err.equals("CaptchaRequired")){
					//TODO: may need to implement UI to get the user to enter the captcha
					throw new HttpRetryException(err, HttpURLConnection.HTTP_FORBIDDEN);
				}else{
					throw new ConnectException(err);
				}
			}else
				throw new ConnectException(respCode + ":" + respMsg);
		}finally{
			if(conn!=null)
				conn.disconnect();
		}
	}
	
	/**
	 * Sends a C2DM message to the mobile device specified by the regId.
	 * This method will retry for a fix number of time until an exception is thrown.
	 * 
	 * @param regId ID that identifies the mobile device
	 * @param msg C2DM message to send to the mobile device
	 * @throws Exception Occurs when the message cannot be send after a fix number of retries
	 */
	public static void sendMsg(String regId, String msg) throws Exception{
		if(authToken==null) getAuthToken();
		
		boolean toContinue=true;
		int retryCnt=1;
		
		do{
			try{
				send(regId, msg);
				
				toContinue=false;
			}catch(AccountException ex){
				//token is invalid, so maybe token expires, try to get the token again
				authToken();
			}catch(HttpRetryException ex){
				int retryTime=Integer.parseInt(ex.getReason());
				if(retryTime==-1) retryTime=defaultRetryWait;
				try{Thread.sleep(retryTime*retryCnt);}catch(InterruptedException ie){}
				
				retryCnt++;
			}
		}while(toContinue);
	}
	
	/**
	 * Sends a REST web request to the C2DM server to send a message to the specific mobile device
	 * 
	 * @param regId ID that identifies the mobile device
	 * @param msg C2DM message to send to the mobile device
	 * @throws Exception Occurs when the message cannot be send
	 */
	private static void send(String regId, String msg) throws Exception{
		HttpsURLConnection conn=null;
		
		try{
			String reqData="registration_id=" + URLEncoder.encode(regId, "UTF-8")
				+ "&collapse_key=" + System.nanoTime()
				+ "&data.payload=" + URLEncoder.encode(URLEncoder.encode(msg, "UTF-8"), "UTF-8");

			URL url = new URL("https://android.apis.google.com/c2dm/send");
			conn=(HttpsURLConnection)url.openConnection();
			conn.setHostnameVerifier(new SSLVerifier());
			
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type","application/x-www-form-urlencoded;charset=UTF-8");
			conn.setRequestProperty("Content-Length", Integer.toString(reqData.length()));
			conn.setRequestProperty("Authorization", "GoogleLogin auth=" + authToken);

			conn.setDoInput(true);
			conn.setDoOutput(true);

			OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
			wr.write(reqData);
		    wr.flush();

		    int respCode=conn.getResponseCode();

		    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			Properties respData=new Properties();
			respData.load(reader);
			reader.close();
			
			//check for Update-Client-Auth header cos need to update the token, may not need since
			//C2DM is only use to quickly send a few msg
			//http://goobr.blogspot.com/2010/11/c2dm-sending-messages.html
			
			LOG.debug("send c2dm resp:"+respCode);
			respData.list(System.out);
			
			if(respCode==HttpURLConnection.HTTP_OK){
				if(respData.contains("Error")){
					String err=respData.get("Error").toString();
					LOG.debug("send c2dm err:"+err);
					if(err.equals("QuotaExceeded") || err.equals("DeviceQuotaExceeded")){
						String retryWait="-1";
						if (conn.getHeaderField("Retry-After")!=null){
							retryWait=conn.getHeaderField("Retry-After");
						}
						throw new HttpRetryException(retryWait, HttpURLConnection.HTTP_OK);
					}else {
						throw new ConnectException(err);
					}
				}
			}else if(respCode==HttpURLConnection.HTTP_UNAUTHORIZED){
				throw new AccountException("Auth token is invalid");
			}else if(respCode==HttpURLConnection.HTTP_UNAVAILABLE){
				String retryWait="-1";
				if (conn.getHeaderField("Retry-After")!=null){
					retryWait=conn.getHeaderField("Retry-After");
				}
				throw new HttpRetryException(retryWait, HttpURLConnection.HTTP_UNAVAILABLE);
			}
		}finally{
			if(conn!=null)
				conn.disconnect();
		}
	}
	
	/**
	 * To bypass the SSL client authentication
	 * 
	 * @author Victoria Chin
	 */
	private static class SSLVerifier implements HostnameVerifier {
		@Override
		public boolean verify(String hostname, SSLSession session) {
			return true;
		}
	}
}
