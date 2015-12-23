package sg.edu.nyp.sit.svds.client;


import static org.junit.Assert.*;

import java.io.File;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class FileLoadTest {
	private static final Log LOG = LogFactory.getLog(FileLoadTest.class);
			
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr[]=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private static int noOfSliceServers=3;
	private long approxFileSize=1 * 1024 * 1024; //in bytes (in mb, x * 1024 *1024)
	private User user=new User("moeif_usrtest", "p@ssw0rd");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main[noOfSliceServers];
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		deleteFiles();
		
		clearSlices(basePath+"/filestore/storage");
	}
	
	private static void clearSlices(String path){
		java.io.File root=new java.io.File(path);
		for(java.io.File f:root.listFiles()){
			if(f.isDirectory())
				continue;
			
			f.delete();
		}
	}

	@Before
	public void setUp() throws Exception {
		deleteFiles();
		
		int masterFilePort=9010, masterNSPort=9011;
		String masterNSHost="localhost", masterFileHost="localhost";
		
		msvr=new sg.edu.nyp.sit.svds.master.Main(basePath+"/resource/IDAProp.properties",
				basePath+"/resource/MasterConfig_unitTest.properties");
		MasterProperties.set("master.directory", masterFilePath);
		MasterProperties.set("master.file.port", masterFilePort);
		MasterProperties.set("master.namespace.port", masterNSPort);
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
		
		for(int i=0; i<noOfSliceServers; i++){
			fsvr[i]=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
			SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
			SliceStoreProperties.set("slicestore.namespace", namespace);
			fsvr[i].startup("TESTFS"+i, (8010+i), (7010+i), "localhost", basePath+"/filestore/storage", 0);
		}
	}

	@After
	public void tearDown() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(fsvr!=null){
			for(int i=0; i<noOfSliceServers; i++){
				if(fsvr[i]!=null)
					fsvr[i].shutdown();
					fsvr[i]=null;
			}
		
		}
		if(msvr!=null)msvr.shutdown();
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
	
	@Test
	public void dummyTest(){
		assertTrue(true);
	}

	//Note: Don't want the test to run at the server cos it might take some time
	//@Test
	public void streamingLoadTest() throws Exception{
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"loadTestFile";
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(testFilePath, user);

		testFile.createNewFile();
		
		long writeTime=writeLoadTest(testFile, true);
		
		long readTime=readLoadTest(testFile, true);
		
		testFile.deleteFile();
		
		testFile=null;
		
		String result="STREAMING (file approx "
				+ ((float)(approxFileSize)/(float)(1024*1024)) 
				+ "MB):";
		result+=" write=" + writeTime + "ms ";
		result+=" read="+readTime+"ms. ";
		
		LOG.info(result);
	}
	
	//Note: Don't want the test to run at the server cos it might take some time
	//@Test
	public void nonStreamingLoadTest() throws Exception{
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"loadTestFile";
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(testFilePath, user);

		testFile.createNewFile();
		
		long writeTime=writeLoadTest(testFile, false);
		
		long readTime=readLoadTest(testFile, false);
		
		testFile.deleteFile();
		
		testFile=null;
		
		String result="NON-STREAMING (file approx "
				+ ((float)(approxFileSize)/(float)(1024*1024)) 
				+ "MB):";
		result+=" write=" + writeTime + "ms ";
		result+=" read="+readTime+"ms. ";
		LOG.info(result);
	}
	
	private long readLoadTest(sg.edu.nyp.sit.svds.client.File testFile, boolean streaming) throws Exception{
		Date timeStart=new Date();
		
		SVDSInputStream in=new SVDSInputStream(testFile, streaming);

		while(in.read()!=-1){}
		
		in.close();
		
		Date timeEnd=new Date();

		in=null;
		
		return (timeEnd.getTime()-timeStart.getTime());
	}
	
	private long writeLoadTest(sg.edu.nyp.sit.svds.client.File testFile, boolean streaming) throws Exception{
		Date timeStart=new Date();
		byte[] data=new Long(timeStart.getTime()).toString().getBytes();
		int iterCnt=(int)(approxFileSize/data.length);
		
		//write big file
		SVDSOutputStream out=new SVDSOutputStream(testFile, streaming);
		
		for(int i=0; i<iterCnt; i++){
			out.write(data);
		}
		
		out.close();
		
		Date timeEnd=new Date();
		
		out=null;

		return (timeEnd.getTime()-timeStart.getTime());
	}
}
