package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.exception.ChangeModeSVDSException;
import sg.edu.nyp.sit.svds.exception.IncompatibleSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.master.filestore.AzureSliceStoreRegistration;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

@SuppressWarnings("unused")
public class FileModeTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	private static String masterNSHost="localhost", masterFileHost="localhost";
	private static int masterNSPort=9011, masterFilePort=9010;
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr1=null;
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr2=null;
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr3=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private static User owner=new User("moeif_usrtest", "p@ssw0rd");	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		
		deleteFiles();
		
		msvr=new sg.edu.nyp.sit.svds.master.Main(basePath+"/resource/IDAProp.properties",
				basePath+"/resource/MasterConfig_unitTest.properties");
		MasterProperties.set("master.directory", masterFilePath);
		MasterProperties.set("master.namespace.port", masterNSPort);
		MasterProperties.set("master.file.port", masterFilePort);
		//do not turn on 2 way ssl
		MasterProperties.set("master.namespace.ssl.clientauth", "off");
		MasterProperties.set("master.maintainence.ssl.clientauth", "off");
		MasterProperties.set("slicestore.sharedaccess", "off");
		msvr.startupMain();
		
		if(MasterProperties.getString("master.file.ssl").equalsIgnoreCase("on")){
			masterFileHost=MasterProperties.getString("master.file.ssl.address");
		}
		if(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("on")){
			masterNSHost=MasterProperties.getString("master.namespace.ssl.address");
		}
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
		ClientProperties.set("client.master.rest.file.host", masterFileHost);
		ClientProperties.set("client.master.rest.namespace.host", masterNSHost);
		ClientProperties.set("client.master.rest.file.port", masterFilePort);
		ClientProperties.set("client.master.rest.namespace.port", masterNSPort);
		ClientProperties.set("client.slicestore.sharedaccess", "off");
		
		fsvr1=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		fsvr2=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		
		SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		
		fsvr1.startup("TESTFS1", 8010, 8011, "localhost", basePath+"/filestore/storage", 0);
		changeFSMode("TESTFS1", 8010, FileIOMode.NON_STREAM);
		
		
		fsvr2.startup("TESTFS2", 8020, 8021, "localhost", basePath+"/filestore/storage", 0);
		changeFSMode("TESTFS2", 8020, FileIOMode.NON_STREAM);
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(fsvr1!=null)fsvr1.shutdown();
		if(fsvr2!=null)fsvr2.shutdown();
		if(fsvr3!=null)fsvr3.shutdown();
		if(msvr!=null)msvr.shutdown();
		
		deleteFiles();
		
		clearSlices(basePath+"/filestore/storage");
	}
	
	@Before
	public void setUp() throws Exception {
		
	}
	
	@After
	public void tearDown() throws Exception {
		
	}
	
	private static void changeFSMode(String id, int port, FileIOMode mode) throws Exception{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = ClientProperties.getString("client.master.rest.namespace.connector")
				+"://"+masterNSHost+":"+masterNSPort+"/namespace/register?" 
				+ RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.Namespace.SVR_ID.value()+"=" + URLEncoder.encode(id, "UTF-8")
				+ "&"+RestletMasterQueryPropName.Namespace.SVR_HOST.value()+"=localhost:" + port 
				+ "&"+RestletMasterQueryPropName.Namespace.SVR_TYPE.value()+"=" + FileSliceServerInfo.Type.RESTLET.value()
				+ "&"+RestletMasterQueryPropName.Namespace.SVR_REQ_VERIFY.value()+"=" + SliceStoreProperties.getString(SliceStoreProperties.PropName.REQ_VERIFY)
				+ "&"+RestletMasterQueryPropName.Namespace.SVR_MODE.value()+"="+mode.value();

			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			Properties p=new Properties();
			p.put(FileSliceServerInfo.RestletPropName.STATUS_HOST.value(), "localhost:"+(port+1));
			p.put(FileSliceServerInfo.RestletPropName.STATUS_SSL.value(), 
					SliceStoreProperties.getString(FileSliceServerInfo.RestletPropName.STATUS_SSL.value()));
			
			fsConn.setDoInput(true);
			fsConn.setDoOutput(true);
			
			p.store(fsConn.getOutputStream(), null);
			
			fsConn.getOutputStream().close();
			
			if(fsConn.getResponseCode()!=HttpURLConnection.HTTP_OK){
				throw new Exception(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}
			
			if(SliceStoreProperties.getBool(SliceStoreProperties.PropName.REQ_VERIFY)){
				BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
				String key=in.readLine();
				in.close();
				
				SliceStoreProperties.set(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+id, key);
			}else
				SliceStoreProperties.remove(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+id);
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	private static void deleteFiles(){
		File f=new File(masterFilePath+"/svds.img");
		if(f.exists())
			f.delete();
		
		f=new File(masterFilePath+"/svdsTrans.log");
		if(f.exists())
			f.delete();
		
		f=new File(masterFilePath+"/namespaceTrans.log");
		if(f.exists())
			f.delete();
	}
	
	private static void clearSlices(String path){
		java.io.File root=new java.io.File(path);
		for(java.io.File f:root.listFiles()){
			if(f.isDirectory())
				continue;
			
			f.delete();
		}
	}

	@Test
	public void testReadIncompatibleMode() throws Exception{
		ClientProperties.set(ClientProperties.PropName.FILE_SUPPORT_MODE.value(), FileIOMode.STREAM.value());

		//create file in non-streaming, attempt to read in streaming
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+"testRead2", owner);
		f.createNewFile();
		
		SVDSOutputStream out=new SVDSOutputStream(f, false);
		out.write("Hello World".getBytes());
		out.close();
		
		boolean hasError=false;
		SVDSInputStream in=null;
		try{
			in=new SVDSInputStream(f, true);
		}catch(IncompatibleSVDSException ex){
			hasError=true;
		}finally{
			if(in!=null) in.close();
		}
		if(!hasError)
			fail("Able to read file in streaming mode");
		
		f.deleteFile();
	}
	
	@Test
	public void testClientIncompatibleMode() throws Exception{
		//attempt to create file in streaming mode when client only supports non-streaming mode
		ClientProperties.set(ClientProperties.PropName.FILE_SUPPORT_MODE.value(), FileIOMode.NON_STREAM.value());
		
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+"testClient", owner);
		f.createNewFile();
		
		boolean hasError=false;
		SVDSOutputStream out=null;
		try{
			out=new SVDSOutputStream(f, true);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}finally{
			if(out!=null) out.close();
		}
		if(!hasError)
			fail("Able to create file in streaming mode");
		
		//attempt to create file in non-streaming mode, should pass
		out=new SVDSOutputStream(f, false);
		out.close();
		
		f.deleteFile();
	}
	
	@Test
	public void testWriteIncompatibleMode() throws Exception{
		ClientProperties.set(ClientProperties.PropName.FILE_SUPPORT_MODE.value(), FileIOMode.STREAM.value());

		//create file in non-streaming, attempt to write again in streaming
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+"writeClient2", owner);
		f.createNewFile();
		
		SVDSOutputStream out=new SVDSOutputStream(f, false);
		out.write("Hello World".getBytes());
		out.close();
		out=null;
		
		boolean hasError=false;
		try{
			out=new SVDSOutputStream(f, true);
		}catch(IncompatibleSVDSException ex){
			hasError=true;
		}finally{
			if(out!=null) out.close();
		}
		
		if(!hasError)
			fail("Able to write file in streaming mode");
		
		f.deleteFile();
	}
	
	@Test
	public void testChangeMode() throws Exception{
		ClientProperties.set(ClientProperties.PropName.FILE_SUPPORT_MODE.value(), FileIOMode.STREAM.value());
		
		//create file in non-stream
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+"testChgMode", owner);
		f.createNewFile();
		
		//write long data so change mode will not be done so soon
		SVDSOutputStream out=new SVDSOutputStream(f, false);
		for(int i=0; i<10; i++){
			out.write("Hello World".getBytes());
		}
		out.close();
		
		System.out.println("Write OK");
		
		//attempt to read
		SVDSInputStream in=new SVDSInputStream(f, false);
		in.close();
		
		//start up slices store that support streaming mode
		//AzureSliceStoreRegistration.main(new String[]{basePath+"/client/test/sg/edu/nyp/sit/svds/client/azureChgMode.txt"
		//		, masterNSHost+":"+masterNSPort, "http"});
		
		fsvr3=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		
		fsvr3.startup("TESTFS3", 8030, 8031, "localhost", basePath+"/filestore/storage", 0);
		changeFSMode("TESTFS3", 8030, FileIOMode.STREAM);
		
		//request to change mode
		f.changeMode(FileIOMode.STREAM);
		
		boolean hasError=false;
		try{
			f.changeMode(FileIOMode.NON_STREAM);
		}catch(ChangeModeSVDSException ex){
			ex.printStackTrace();
			hasError=true;
		}
		if(!hasError)
			fail("Able to issue subsequent change mode request.");
		
		System.out.println("wait for change mode to complete.");
		//wait a while to change mode to complete
		int retryCnt=10;
		while(retryCnt>0){
			//refresh the slice info
			f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+"testChgMode", owner);
			if(!f.fInfo.isChgMode())
				break;
			
			Thread.sleep(10*1000);
			
			retryCnt--;
		}

		//attempt to read in streaming mode, should be okie after change mode
		System.out.println("opening input stream");
		in=new SVDSInputStream(f, true);
		System.out.println("Closing input stream");
		in.close();
		
		//attempt to write in streaming mode, should be okie after change mode
		out=new SVDSOutputStream(f, true);
		out.close();
		
		f.deleteFile();
		
		System.out.println("DONE");
	}
}
