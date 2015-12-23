package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.master.filestore.AzureSliceStoreRegistration;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

@SuppressWarnings("unused")
public class FileRecoveryTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	
	private sg.edu.nyp.sit.svds.master.Main msvr=null;
	private String masterNSHost="localhost", masterFileHost="localhost";
	private static int masterNSPort=9011, masterFilePort=9010;
	
	private sg.edu.nyp.sit.svds.filestore.Main fsvr1=null;
	private sg.edu.nyp.sit.svds.filestore.Main fsvr2=null;
	private static String fsvr1_path=null, fsvr2_path=null;
	private String fsvr1_id="TESTFS1", fsvr2_id="TESTFS2";
	private int fsvr1_port=8010, fsvr2_port=8020;
	private static User owner=null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		
		fsvr1_path=basePath+"/filestore/storage/fsvr1";
		fsvr2_path=basePath+"/filestore/storage/fsvr2";
		
		delSliceStorage(fsvr1_path);
		delSliceStorage(fsvr2_path);
		
		owner=new User("moeif_usrtest", "p@ssw0rd");	
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}
	
	@Before
	public void setUp() throws Exception {
		File f=new File(masterFilePath+"/svds.img");
		if(f.exists())
			f.delete();
		f=null;
		
		f=new File(masterFilePath+"/svdsTrans.log");
		if(f.exists())
			f.delete();
		f=null;
		
		f=new File(masterFilePath+"/namespaceTrans.log");
		if(f.exists())
			f.delete();
		f=null;
		
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
		
		ClientProperties.set("client.master.rest.file.host", masterFileHost);
		ClientProperties.set("client.master.rest.namespace.host", masterNSHost);
		ClientProperties.set("client.master.rest.file.port", masterFilePort);
		ClientProperties.set("client.master.rest.namespace.port", masterNSPort);
		ClientProperties.set("client.slicestore.sharedaccess", "off");

		fsvr1_path=basePath+"/filestore/storage/fsvr1";
		fsvr2_path=basePath+"/filestore/storage/fsvr2";

		f=new File(fsvr1_path);
		f.mkdir();
		f=null;

		f=new File(fsvr2_path);
		f.mkdir();
		f=null;
		
		fsvr1=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_noverify_unitTest.properties");
		fsvr2=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_noverify_unitTest.properties");
		
		SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		
		fsvr1.startup(fsvr1_id, fsvr1_port, fsvr1_port+1, "localhost", fsvr1_path, 0);
		fsvr2.startup(fsvr2_id, fsvr2_port, fsvr2_port+1, "localhost", fsvr2_path, 0);
		
		//AzureSliceStoreRegistration.main(new String[]{basePath+"/client/test/sg/edu/nyp/sit/svds/client/azureRecovery.txt", masterNSHost+":"+masterNSPort, "http"});
	}
	
	private static void delSliceStorage(String path){
		//create the directory if necessary
		File f=new File(path);
		if(f.exists()){
			//deletes the files inside
			for(File i: f.listFiles()){
				i.delete();
			}
		}
		f.delete();
	}

	@After
	public void tearDown() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(msvr!=null)msvr.shutdown();
		if(fsvr1!=null)fsvr1.shutdown();
		if(fsvr2!=null)fsvr2.shutdown();
		
		delSliceStorage(fsvr1_path);
		delSliceStorage(fsvr2_path);
	}
	
	@Test
	public void testNonStreamingFileRecovery() throws Exception{
		//change both fs to non-streaming mode
		//changeFSMode(fsvr1_id, fsvr1_port, FileIOMode.NON_STREAM);
		changeFSMode(fsvr2_id, fsvr2_port, FileIOMode.NON_STREAM);
		
		//shutdown server 2 after it has registered itself to the master server
		fsvr2.shutdown();
		fsvr2=null;

		//create a file
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"recoverTest.txt";
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(testFilePath, owner);
		f.createNewFile();

		//write something to the file
		SVDSOutputStream out=new SVDSOutputStream(f, false);
		out.write("Hello World".getBytes());
		out.close();

		//check if there are any slices that write to slice server 2
		Field i=sg.edu.nyp.sit.svds.client.File.class.getDeclaredField("fInfo");
		i.setAccessible(true);
		FileInfo fi=(FileInfo)i.get(f);
		
		if(fi.getSlices().size()<fi.getIda().getShares())
			fail("File slices are lost.");
		
		for(FileSliceInfo fsi: fi.getSlices()){
			if(fsi.getServerId().equals(fsvr2_id))
				fail("File slices are still stored at slice server that is shut down");
		}
		
		System.out.println("Test succeed.");
		
		f.deleteFile();
	}
	
	@Test
	public void testStreamingFileRecovery() throws Exception{
		//change both fs to non-streaming mode
		//changeFSMode(fsvr1_id, fsvr1_port, FileIOMode.STREAM);
		changeFSMode(fsvr2_id, fsvr2_port, FileIOMode.STREAM);
		
		//create a file
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"recoverTest.txt";
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(testFilePath, owner);
		f.createNewFile();

		//write something to the file (at least more than buffer size)
		String data="New file created on " + (new Date()).toString() + " ";
		while(data.length()<(1024*100*3))
		//while(data.length()<(1536*3))
			data+=data;
		
		System.out.println("Data length: " + data.getBytes().length);
		
		System.out.println("WRITE NOW");
		
		SVDSOutputStream out=new SVDSOutputStream(f, true);
		out.write(data.getBytes());
		
		//wait a while for the transformation to finish
		System.out.println("WAIT FOR TRANSFORMATION TO FINISH");
		Thread.sleep(1000 * 8);
		
		//shutdown slice server 2 so some slice will fail the write and init the recovery
		System.out.println("SHUT DOWN FS2");
		fsvr2.shutdown();
		fsvr2=null;
		
		//continue to write (at least more than buffer size)
		out.write(data.getBytes());
		
		//wait a while for the transformation to finish
		Thread.sleep(1000 * 8);
		
		//before close, check that (thru reflection) the slices has segments
		Field i=sg.edu.nyp.sit.svds.client.File.class.getDeclaredField("fInfo");
		i.setAccessible(true);
		FileInfo fi=(FileInfo)i.get(f);
		boolean isFound=false;
		for(FileSliceInfo fsi: fi.getSlices()){
			System.out.println("Slice server: " + fsi.getServerId());
			if(fsi.hasSegments()){
				isFound=true;
				break;
			}
		}
		if(!isFound)
			fail("Slice segments are not created.");
		
		System.out.println("START UP FS2");
		//start up the slice server 2 again so recovery can take place
		fsvr2=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_noverify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr2.startup(fsvr2_id, fsvr2_port, fsvr2_port+1, "localhost", fsvr2_path, 0);
		
		//close the stream to allow the slices to be sent over to master server for recovery
		out.close();
		f=null;
		
		System.out.println("WRITE DONE");
		
		//check to see if reovery is done
		int cnt=0, retryLimit=10;
		boolean passed=true;
		do{
			if(!passed)
				Thread.sleep(1000 * 10);
			
			f=new sg.edu.nyp.sit.svds.client.File(testFilePath, owner);
			passed=true;
			
			fi=(FileInfo)i.get(f);
			
			for(FileSliceInfo fsi: fi.getSlices()){
				if(fsi.isSliceRecovery()){
					System.out.println("Slice still in recovery.");
					passed=false;
					break;
				}
			}
			
			cnt++;
		}while(!passed && cnt<retryLimit);
		
		if(!passed)
			fail("Slice recovery fail.");
		
		System.out.println("Test succeed.");
		
		f.deleteFile();
	}
	
	private void changeFSMode(String id, int port, FileIOMode mode) throws Exception{
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
}
