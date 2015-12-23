package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.master.filestore.AzureSliceStoreRegistration;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

@SuppressWarnings("unused")
public class AzureFileTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private static User owner=new User("moeif_usrtest", "p@ssw0rd");
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
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

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		deleteFiles();
		
		SVDSStreamingPool.shutdown();
	}

	@Before
	public void setUp() throws Exception {
		deleteFiles();
		
		int masterNSPort=9011, masterFilePort=9010;
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
		
		AzureSliceStoreRegistration.main(new String[]{basePath+"/client/test/sg/edu/nyp/sit/svds/client/azureFile.txt", masterNSHost+":"+masterNSPort, "http"});
		
		/*
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup("TESTFS1", 8010, 8011, "localhost", basePath+"/filestore/storage");
		*/
	}

	@After
	public void tearDown() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(fsvr!=null)fsvr.shutdown();
		if(msvr!=null)msvr.shutdown();
	}

	@Test
	public void testStreamingFile() throws Exception{
		testFile(true);
	}
	
	@Test
	public void testNonStreamingFile() throws Exception{
		testFile(false);
	}
	
	private void testFile(boolean streaming) throws Exception{
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"test.txt";
		
		String newFileData="New file created on " + (new Date()).toString() + " ";
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		//newFileData="ABCDEF";
		System.out.println("Length of data: " + newFileData.getBytes().length);
		
		String updFileData="File updated on " + (new Date()).toString();
		//updFileData="GH";
		
		//because update starts writing at 0, have to merge 
		//the newFileData and updFileData together
		String verifyData=null;
		int offset=100;
		if(updFileData.length()>=newFileData.length())
			throw new Exception("Update content longer than original content");
		else
			verifyData=newFileData.substring(0, offset)+updFileData+newFileData.substring(offset+updFileData.length());
		
		testCreateNewTxtFile(testFilePath, newFileData, streaming);
		System.out.println("AFTER CREATE NEW FILE");
		
		testGetTxtFile(testFilePath, newFileData, 0, streaming);
		System.out.println("AFTER VERIFY NEW FILE");
		
		testUpdateTxtFile(testFilePath, updFileData, 100, streaming);
		System.out.println("AFTER UPDATE FILE");
		
		testGetTxtFile(testFilePath, verifyData, 0, streaming);
		System.out.println("AFTER VERIFY UPDATED FILE");
		
		testGetTxtFile(testFilePath, verifyData.substring(1540), 1540, streaming);

		testDeleteFile(testFilePath);
	}
	
	private void testCreateNewTxtFile(String filePath, String data, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);

		testFile.createNewFile();
		
		SVDSOutputStream out=new SVDSOutputStream(testFile, streaming);
		out.write(data.getBytes());
		out.close();
		
		out=null;
		testFile=null;
	}
	
	private void testGetTxtFile(String filePath, String data, long offset, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
		
		if(!testFile.exist())
			fail("File is not created.");
		
		if(testFile.isDirectory())
			return;
		
		/*
		Field f=sg.edu.nyp.sit.svds.client.File.class.getDeclaredField("fInfo");
		f.setAccessible(true);
		FileInfo fi=(FileInfo)f.get(testFile);
		List<FileSliceInfo>slices=fi.getSlices();
		String tmp="";
		int cnt=0;
		//TEST
		for(FileSliceInfo fss:slices){
			tmp+=fss.getSliceName()+"="+fss.isSliceRecovery()+"|";
			if(!fss.isSliceRecovery())
				cnt++;
		}
		if(cnt<3)
			throw new Exception(tmp);
		*/

		SVDSInputStream in=new SVDSInputStream(testFile, streaming);
		in.seek(offset);
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		int b;
		while((b=in.read())!=-1){
			out.write(b);
		}
		in.close();
		
		String retrievedData=new String(out.toByteArray());
		
		System.out.println("Offset: " + offset + ", file len: "+testFile.getFileSize());
		System.out.println("Data from file  (len="+retrievedData.length()+"):\"" + retrievedData+"\"");
		System.out.println("Data to verifiy (len="+data.length()+"):\"" + data+"\"");		

		if(!data.equals(retrievedData))
			fail("File is not saved/retrieved correctly.");
		
		in=null;
		testFile=null;
		out=null;
	}
	
	private void testUpdateTxtFile(String filePath, String data, long offset, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
	
		SVDSOutputStream out=new SVDSOutputStream(testFile, streaming);
		out.seek(offset);
		
		System.out.print("Upd data (byte): ");
		Resources.printByteArray(data.getBytes());
		System.out.println("Upd data (string): " + data);
		out.write(data.getBytes());

		out.close();
		
		out=null;
		testFile=null;
	}
	
	private void testDeleteFile(String filePath) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
		
		testFile.deleteFile();
		
		testFile=null;
		
		testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
		
		if(testFile.exist())
			fail("File is not deleted.");
		
		testFile=null;
	}
}
