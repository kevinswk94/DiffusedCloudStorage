package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

@SuppressWarnings("unused")
public class FileTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	private static String binaryFilePath=null;
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private int quorum=0;
	private static User owner=new User("moeif_usrtest", "p@ssw0rd");
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		binaryFilePath=basePath+"/resource/testimage.jpg";

		java.io.File f=new java.io.File(binaryFilePath);
		if(!f.exists())
			throw new NullPointerException("Binary file does not exist at " + binaryFilePath);	
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		deleteFiles();
		
		clearSlices(basePath+"/filestore/storage");
		
		SVDSStreamingPool.shutdown();
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
		
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterNSHost+":"+masterNSPort);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup("TESTFS1", 8010, 8011, "localhost", basePath+"/filestore/storage", 0);
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
	public void testStreamingBinaryFile() throws Exception{
		String filePath=namespace+FileInfo.PATH_SEPARATOR+"sImg.jpg";
		
		String fileChecksum=testCreateNewBinaryFile(filePath, true);

		String getFileChecksum=testGetBinaryFile(filePath, true);
		
		testDeleteFile(filePath);
		
		System.out.println("Stored checksum: " + fileChecksum);
		System.out.println("Get checksum: " + getFileChecksum);
		
		if(!fileChecksum.equals(getFileChecksum))
			fail("Binary file is not stored/retrieved correctly.");
	}
	
	@Test
	public void testNonStreamingBinaryFile() throws Exception{
		String filePath=namespace+FileInfo.PATH_SEPARATOR+"nsImg.jpg";
		
		String fileChecksum=testCreateNewBinaryFile(filePath, false);
		
		String getFileChecksum=testGetBinaryFile(filePath, false);
		
		testDeleteFile(filePath);
		
		System.out.println("Stored checksum: " + fileChecksum);
		System.out.println("Get checksum: " + getFileChecksum);
		
		if(!fileChecksum.equals(getFileChecksum))
			fail("Binary file is not stored/retrieved correctly.");
	}
	
	private String testGetBinaryFile(String filePath, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
		
		if(!f.exist())
			fail("Binary file does not exist.");
		
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		SVDSInputStream in=new SVDSInputStream(f, streaming);
		
		/*
		java.io.File iof=new java.io.File(basePath+"/t.jpg");
		if(iof.exists())
			iof.delete();
		iof.createNewFile();
		FileOutputStream out=new FileOutputStream(iof);
		*/
		
		byte[] data=new byte[512];
		int len;
		while((len=in.read(data))!=-1){
			//out.write(data, 0, len);
			md.update(data, 0, len);
		}

		/*
		out.flush();
		out.close();
		out=null;
		iof=null;
		*/

		in.close();
		
		return Resources.convertToHex(md.digest());
	}
	
	private String testCreateNewBinaryFile(String filePath, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File f=new sg.edu.nyp.sit.svds.client.File(filePath, owner);
		
		f.createNewFile();
		
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		
		SVDSOutputStream out=new SVDSOutputStream(f, streaming);
		FileInputStream in=new FileInputStream(binaryFilePath);
		byte[] data=new byte[512];
		int len;
		len=in.read(data);
		while((len=in.read(data))!=-1){
			out.write(data, 0, len);
			md.update(data, 0, len);
		}
		in.close();
		out.close();
		System.out.println("CLOSE WRITE");
		
		return Resources.convertToHex(md.digest());
	}
	
	//@Test
	public void testBlankFile() throws Exception{
		String filePath=namespace+FileInfo.PATH_SEPARATOR+"blank.txt";
		testNewBlankFile(filePath, true);
		testNewBlankFile(filePath, false);
	}
	
	@Test
	public void testNonStreamingFile() throws Exception{
		String testFolderPath=namespace+FileInfo.PATH_SEPARATOR+"abc";
		String testFilePath=testFolderPath+FileInfo.PATH_SEPARATOR+"test.txt";
		
		testListDirectory(namespace+FileInfo.PATH_SEPARATOR, 0);
		
		String newFileData="New file created on " + (new Date()).toString() + " ";
		//newFileData+=newFileData;
		//newFileData+=newFileData;
		//newFileData+=newFileData;
		//newFileData="ABCDEF";
		
		String updFileData="File updated on " + (new Date()).toString();
		//updFileData="GH";
		
		//because update starts writing at 0, have to merge 
		//the newFileData and updFileData together
		String verifyData=null;
		if(updFileData.length()>=newFileData.length())
			verifyData=updFileData;
		else
			verifyData=updFileData + newFileData.substring(updFileData.length());
		
		testCreateNewDirectory(testFolderPath);
		
		testCreateNewTxtFile(testFilePath, newFileData, false);
		
		testListDirectory(namespace+FileInfo.PATH_SEPARATOR, 1);
		testListDirectory(testFolderPath, 1);

		System.out.println("AFTER CREATE NEW FILE");
		testGetTxtFile(testFilePath, newFileData, 0, false);
		System.out.println("AFTER VERIFY NEW FILE");
		
		testUpdateTxtFile(testFilePath, updFileData, 0, false);
		System.out.println("AFTER UPDATE FILE");
		
		testGetTxtFile(testFilePath, verifyData, 0, false);
		System.out.println("AFTER VERIFY UPDATED FILE");

		testDeleteFile(testFilePath);
		
		testDeleteDirectory(testFolderPath);
	}
	
	@Test
	public void testStreamingFile() throws Exception{
		String testFolderPath=namespace+FileInfo.PATH_SEPARATOR+"abc";
		String testFilePath=testFolderPath+FileInfo.PATH_SEPARATOR+"test.txt";
		
		String newFileData="New file created on " + (new Date()).toString() + " ";
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		//newFileData="ABCDEFZ";
		
		String updFileData="File updated on " + (new Date()).toString();
		updFileData="GH";
		
		//because update starts writing at 0, have to merge 
		//the newFileData and updFileData together
		String verifyData=null;
		if(updFileData.length()>=newFileData.length())
			verifyData=updFileData;
		else
			verifyData=updFileData + newFileData.substring(updFileData.length());
		
		testCreateNewDirectory(testFolderPath);
		
		testCreateNewTxtFile(testFilePath, newFileData, true);
		
		testGetTxtFile(testFilePath, newFileData, 0, true);

		testUpdateTxtFile(testFilePath, updFileData, 0, true);

		testGetTxtFile(testFilePath, verifyData, 0, true);
		
		//testGetTxtFile(testFilePath, verifyData.substring(4700), 4700, true);

		testDeleteFile(testFilePath);
		
		testDeleteDirectory(testFolderPath);
	}
	
	@Test
	public void testStreamingRandomFile() throws Exception{
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"randomTest.txt";
		
		String newFileData="New file created on " + (new Date()).toString() + " ";
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		newFileData+=newFileData;
		//newFileData="ABCDEFGH";
		
		String updFileData="File updated on " + (new Date()).toString();
		updFileData="YZ";
		
		String verifyData=null;
		
		Random r=new Random();
		int offset1=0, offset2=0, offset3=0;

		//create a new file
		testCreateNewTxtFile(testFilePath, newFileData, true);
		
		//finds a random offset
		do{
			offset1=r.nextInt(newFileData.length());
		}while(offset1%quorum>0);
		//offset1=6;
		//offset1=0;
		//offset1=147;
		System.out.println("Offset1: " + offset1);
		
		do{
			offset2=r.nextInt(newFileData.length());
		}while(offset2%quorum==0);
		//offset2=2;
		//offset2=1;
		//offset2=161;
		System.out.println("Offset2: " + offset2);

		System.out.println("RANDOM READ 1");
		//random read test (where offset mod quorum is zero)
		verifyData=newFileData.substring(offset1);
		
		testGetTxtFile(testFilePath, verifyData, offset1, true);
		
		System.out.println("RANDOM READ 2");
		//random read test (where offset mod quorum is NOT zero)
		verifyData=newFileData.substring(offset2);
		
		testGetTxtFile(testFilePath, verifyData, offset2, true);

		//random write test (where offset mod quroum is zero)
		verifyData=newFileData.substring(0, offset1)+updFileData
			+((offset1+updFileData.length()>=newFileData.length()? ""
					:newFileData.substring(offset1+updFileData.length())));
		
		System.out.println("RANDOM WRITE 1");
		testUpdateTxtFile(testFilePath, updFileData, offset1, true);
		
		System.out.println("RANDOM WRITE READ 1");
		testGetTxtFile(testFilePath, verifyData, 0, true);
		
		newFileData=verifyData;
		
		//random write test (where offset mod quorum is NOT zero)
		verifyData=newFileData.substring(0, offset2)+updFileData
		+((offset2+updFileData.length()>=newFileData.length()? ""
				:newFileData.substring(offset2+updFileData.length())));
		
		System.out.println("RANDOM WRITE 2");
		testUpdateTxtFile(testFilePath, updFileData, offset2, true);

		System.out.println("RANDOM WRITE READ 2");
		testGetTxtFile(testFilePath, verifyData, 0, true);

		newFileData=verifyData;
		
		//random write test (where offset is bigger than file length)
		offset3=newFileData.length()+r.nextInt(20)+1;
		//offset3=235;
		System.out.println("Offset3: " + offset3);
		//offset3=newFileData.length()+10;
		verifyData=updFileData;
	
		System.out.println("RANDOM WRITE 3");
		testUpdateTxtFile(testFilePath, updFileData, offset3, true);
		
		System.out.println("RANDOM WRITE READ 3");
		testGetTxtFile(testFilePath, verifyData, offset3, true);
		
		//deletes the file
		testDeleteFile(testFilePath);
	}

	@Test
	public void testNonStreamingFileChecksumOption() throws Exception{
		testFileChecksumOption(false);
	}
	
	@Test
	public void testStreamingFileChecksumOption() throws Exception{
		testFileChecksumOption(true);
	}
	
	private void testFileChecksumOption(boolean streaming) throws Exception{
		String testFilePath=namespace+FileInfo.PATH_SEPARATOR+"test.txt";
		//String keyHash="123";
		String newFileData="New file created on " + (new Date()).toString() + " ";
		
		//turn on checksum feature
		ClientProperties.set(ClientProperties.PropName.FILE_VERIFY_CHECKSUM.value(), "on");
		
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(testFilePath, owner);

		testFile.createNewFile();

		Field f=testFile.getClass().getDeclaredField("fInfo");
		f.setAccessible(true);
		FileInfo fInfo=(FileInfo)f.get(testFile);
		
		/*
		//turn on the checksum feature (should be on by default, just in case it has been set to off in prop file)
		fInfo.setBlkSize(Integer.parseInt(Resources.getClientInfo(Resources.SYS_SLICE_BLK_SIZE)));
		fInfo.setKeyHash(keyHash);
		*/

		//create a stream to write some data
		SVDSOutputStream out =new SVDSOutputStream(testFile, streaming);
		out.write(newFileData.getBytes());
		out.close();
		out=null;
		
		//wait a while for write operation to finish and the files to be created
		Thread.sleep(1000*3);
		
		//check if the checksum files are created
		for(FileSliceInfo fsi: fInfo.getSlices()){
			java.io.File s=new File(basePath+"/filestore/storage/"+fsi.getSliceName()+".chk");
			if(!s.exists())
				fail("Checksum file is not created.");
		}

		//create a stream to read some data in non-streaming mode
		//make sure no problem reading when verifying checksum
		SVDSInputStream in=new SVDSInputStream(testFile, streaming);
		int d;
		ByteArrayOutputStream tmp=new ByteArrayOutputStream();
		while((d=in.read())!=-1){
			tmp.write(d);
		}
		in.close();
		in=null;
		
		String retrievedData=new String(tmp.toByteArray());
		if(!retrievedData.equals(newFileData))
			fail("Retrieved contents does not match");
		
		//turn off the checksum feature
		ClientProperties.set(ClientProperties.PropName.FILE_VERIFY_CHECKSUM.value(), "off");
		
		fInfo.setBlkSize(0);
		fInfo.setKeyHash(null);
		for(FileSliceInfo fsi: fInfo.getSlices()){
			fsi.setSliceChecksum(null);
		}
		
		//attempt to write to the file again, this time the checksum file will be deleted
		out=new SVDSOutputStream(testFile, streaming);
		String updFileData="Hello World";
		newFileData=updFileData+newFileData.substring(updFileData.length());
		out.write(updFileData.getBytes());
		out.close();
		out=null;
		
		//wait a while for write operation to finish
		Thread.sleep(1000*3);
		
		//check if the checksum files are deleted
		for(FileSliceInfo fsi: fInfo.getSlices()){
			java.io.File s=new File(basePath+"/filestore/storage/"+fsi.getSliceName()+".chk");
			if(s.exists())
				fail("Checksum file is not deleted.");
		}
		
		//deletes the file
		testDeleteFile(testFilePath);
	}
	
	private void testCreateNewDirectory(String directoryPath) throws Exception{
		(new sg.edu.nyp.sit.svds.client.File(directoryPath, owner)).createNewDirectory();

		sg.edu.nyp.sit.svds.client.File testFolder=new sg.edu.nyp.sit.svds.client.File(directoryPath, owner);
		if(!testFolder.exist())
			fail("Folder is not created.");
		if(!testFolder.isDirectory())
			fail("Path is not a directory.");
	}
	
	private void testListDirectory(String directoryPath, int noOfRecords) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFolder=new sg.edu.nyp.sit.svds.client.File(directoryPath, owner);
		if(!testFolder.exist())
			fail("Path does not exist.");
		if(!testFolder.isDirectory())
			fail("Path is not a directory.");
		
		List<String> records=testFolder.listFilesPath();

		System.out.println("Records found: " + records.size());
		System.out.println("No to compare: " + noOfRecords);
		for(String n: records)
			System.out.println(n);
		
		if(records.size()!=noOfRecords)
			fail("Folder does not contain correct number of folder(s) and/or file(s).");
	}
	
	private void testNewBlankFile(String filePath, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);

		testFile.createNewFile();
		
		//create a output stream but write nothing to it
		System.out.println("WRITE BLANK");
		SVDSOutputStream out=new SVDSOutputStream(testFile, streaming);
		out.close();
		System.out.println("END WRITE BLANK");
		
		System.out.println("READ BLANK");
		//create a input stream and try to read
		SVDSInputStream in=new SVDSInputStream(testFile, streaming);
		if(in.read()!=-1){
			in.close();
			fail("File should be empty.");
		}
		in.close();
		System.out.println("END READ BLANK");
		
		testFile.deleteFile();
	}
	
	private void testCreateNewTxtFile(String filePath, String data, boolean streaming) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, owner);

		testFile.createNewFile();
		
		Field f=testFile.getClass().getDeclaredField("fInfo");
		f.setAccessible(true);
		quorum=((FileInfo)f.get(testFile)).getIda().getQuorum();
		
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
	
	private void testDeleteDirectory(String directoryPath) throws Exception{
		sg.edu.nyp.sit.svds.client.File testFolder=new sg.edu.nyp.sit.svds.client.File(directoryPath, owner);
		
		testFolder.deleteDirectory();
		
		testFolder=null;
		
		testFolder=new sg.edu.nyp.sit.svds.client.File(directoryPath, owner);
		
		if(testFolder.exist())
			fail("Directory is not deleted.");
		
		testFolder=null;
	}
}