package sg.edu.nyp.sit.svds.client.filestore.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.*;
import java.security.MessageDigest;
import java.util.*;

import org.junit.*;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class RestletSliceStoreTest {
	private static String basePath=null;
	private static String sliceStorePath=null;
	private static String sampleGetFilePath=null;
	private static String sampleGetFileName=null;
	private static String sliceFileName="FileSliceStoreTestSlice";
	
	private static String namespace="urn:sit.nyp.edu.sg";
	private static String fserverId="TESTFS1";
	private static int fsPort=8010;
	private static int masterPort=9011;
	private static String masterHost="localhost:"+masterPort;
	
	IFileSliceStore fss;
	
	private static String digestKey="abc";
	private static int blkSize=5;
	private static String sampleGetFileData="Test Test Test\nFile File File";
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		if(sampleGetFileData.length()<(blkSize*2))
			throw new Exception("Sample file data length must be at least 2 times larger than " + blkSize);
		
		basePath=System.getProperty("user.dir");
		sliceStorePath=basePath+"/filestore/storage";
		sampleGetFileName="FileStoreGetTest";
		sampleGetFilePath=sliceStorePath+"/"+sampleGetFileName;
		
		createSampleFile();
		
		java.io.File f=new java.io.File(sliceStorePath+"/"+sliceFileName);
		if(f.exists())
			f.delete();
		
		f=new File(basePath+"/svds.img");
		if(f.exists())
			f.delete();
		
		f=new File(basePath+"/svdsTrans.log");
		if(f.exists())
			f.delete();
		
		f=new File(basePath+"/namespaceTrans.log");
		if(f.exists())
			f.delete();
		
		msvr=new sg.edu.nyp.sit.svds.master.Main(basePath+"/resource/IDAProp.properties",
				basePath+"/resource/MasterConfig_unitTest.properties");
		MasterProperties.set("master.directory", basePath);
		MasterProperties.set("master.namespace.port", masterPort);
		//do not turn on 2 way ssl
		MasterProperties.set("master.namespace.ssl.clientauth", "off");
		MasterProperties.set("master.maintainence.ssl.clientauth", "off");
		msvr.startupMain();
		
		if(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("on")){
			masterHost=MasterProperties.getString("master.namespace.ssl.address")+":"+masterPort;
		}

		IFileSliceStore.updateServerMapping(fserverId, "localhost:"+fsPort, 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null, null, null);
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
	}
	
	private static void createSampleFile() throws Exception{
		java.io.File f=new java.io.File(sampleGetFilePath);
		if(f.exists())
			f.delete();
		f.createNewFile();
		
		
		FileOutputStream out=new FileOutputStream(f);
		out.write(sampleGetFileData.getBytes());
		out.flush();
		out.close();
		out=null;
		
		java.io.File fChk=new java.io.File(sampleGetFilePath+".chk");
		if(fChk.exists())
			fChk.delete();
		fChk.createNewFile();
		out=new FileOutputStream(fChk);
		
		MessageDigest md= MessageDigest.getInstance(Resources.HASH_ALGO);
		byte[] hash;
		for(int i=0; i<sampleGetFileData.length(); i+=blkSize){
			hash=md.digest((sampleGetFileData.substring(i, 
					(i+blkSize>=sampleGetFileData.length()?sampleGetFileData.length():i+blkSize))
					+digestKey).getBytes());
			
			out.write(hash);
			out.flush();
		}
		out.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(msvr!=null)msvr.shutdown();
		
		java.io.File f=new java.io.File(sampleGetFilePath);
		if(f.exists())
			f.delete();
		
		clearSlices(sliceStorePath);
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
	}

	@After
	public void tearDown() throws Exception {
		Thread.sleep(1000*3);
		
		if(fsvr!=null)fsvr.shutdown();
		
		java.io.File f=new java.io.File(sliceStorePath+"/"+sliceFileName);
		if(f.exists())
			f.delete();
		f=null;
		f=new java.io.File(sliceStorePath+"/"+sliceFileName+".chk");
		if(f.exists())
			f.delete();
	}
	
	@Test
	public void testSharedAccessSliceStore() throws Exception{
		//start up slice store with authentication
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterHost);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup(fserverId, fsPort, fsPort+1, "localhost", sliceStorePath, 0);
		
		//checks that there is key in the properties
		if(!SliceStoreProperties.exist(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId))
			fail("Slice store key is not generated.");
		
		IFileSliceStore.updateServerMapping(fserverId, "localhost:"+fsPort, 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null, null, null);
		
		fss=new RestletSharedAccessSliceStore(fserverId);
		IMasterNamespaceTable mns=MasterTableFactory.getNamespaceInstance();
		
		String[] urls=mns.getSharedAccessURL(fserverId, "SliceHashTest", null);
		testStoreRetrieveHashes(urls);
		
		urls=mns.getSharedAccessURL(fserverId, "FileStoreDelTest", null);
		testDelete(urls);
		
		urls=mns.getSharedAccessURL(fserverId, sampleGetFileName, null);
		testRetrieveByteArrayNoOffset(urls);
		testRetrieveByteArrayWithOffset(urls);
		testRetrieveByteArrayWithOffsetAndLength(urls);
		
		deleteTestStoreFiles();
		deleteTestStoreFiles();
		urls=mns.getSharedAccessURL(fserverId, sliceFileName, null);
		testStoreByteArray(urls);
		deleteTestStoreFiles();
		testStoreStreamingWithOffset(urls);
		deleteTestStoreFiles();
	}
	
	@Test
	public void testSliceStoreWithAuthentication() throws Exception{
		//start up slice store with authentication
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterHost);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup(fserverId, fsPort, fsPort+1, "localhost", sliceStorePath, 0);
		
		//checks that there is key in the properties
		if(!SliceStoreProperties.exist(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId))
			fail("Slice store key is not generated.");
		
		IFileSliceStore.updateServerMapping(fserverId, "localhost:"+fsPort, 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null,
				SliceStoreProperties.getString(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId),
				null);
		
		fss=new RestletSliceStore(fserverId);
		
		testSliceStore();
	}
	
	@Test
	public void testSliceStoreWithoutAuthentication() throws Exception{
		//start up slice store with no authentication
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_noverify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterHost);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup(fserverId, fsPort, fsPort+1, "localhost", sliceStorePath,0);
		
		//check that there is no key
		if(SliceStoreProperties.exist(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId))
			fail("Slice store key still exist.");
		
		IFileSliceStore.updateServerMapping(fserverId, "localhost:"+fsPort, 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null, null, null);
		
		fss=new RestletSliceStore(fserverId);
		
		testSliceStore();
	}
	
	private void testSliceStore() throws Exception{
		testStoreRetrieveHashes("SliceHashTest");
		
		testDelete("FileStoreDelTest");
		
		testRetrieveByteArrayNoOffset(sampleGetFileName);
		testRetrieveByteArrayWithOffset(sampleGetFileName);
		testRetrieveByteArrayWithOffsetAndLength(sampleGetFileName);
		
		deleteTestStoreFiles();
		deleteTestStoreFiles();
		testStoreByteArray(sliceFileName);
		deleteTestStoreFiles();
		testStoreStreamingWithOffset(sliceFileName);
		deleteTestStoreFiles();
	}
	
	private void deleteTestStoreFiles(){
		java.io.File f=new java.io.File(sliceStorePath+"/"+sliceFileName);
		if(f.exists())
			f.delete();
		f=null;
		f=new java.io.File(sliceStorePath+"/"+sliceFileName+".chk");
		if(f.exists())
			f.delete();
	}
	
	private void testStoreRetrieveHashes(Object sliceName) throws Exception{
		List<byte[]> arr=new ArrayList<byte[]>();
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		
		//put in random hash generated
		String inputHash="";
		byte[] digest;
		for(int i=0; i<3; i++){
			digest=md.digest(("test"+i).getBytes());
			arr.add(digest);
			inputHash+=Resources.convertToHex(digest);
			md.reset();
		}
		md=null;
		
		fss.storeHashes(arr, sliceName);
		
		arr=fss.retrieveHashes(sliceName);
		
		String retrievedHash="";
		for(byte[] tmp :arr){
			retrievedHash+=Resources.convertToHex(tmp);
		}
		
		assertEquals("Contents retrieved does not match.", inputHash, retrievedHash);
	}
	
	private void testDelete(Object sliceName)throws Exception {
		//for testing delete, create a temp file
		java.io.File f=new java.io.File(sliceStorePath+"/FileStoreDelTest");
		if(!f.exists()){
			f.createNewFile();
		
			FileOutputStream out=new FileOutputStream(f);
			out.write("To be deleted...".getBytes());
			out.flush();
			out.close();
		}
		
		fss.delete(sliceName);
		assertTrue(true);
	}

	private void printByteArray(String caption, byte[] arr){
		System.out.print(caption+" ");
		for(int i=0; i<arr.length; i++)
			System.out.print(arr[i] + " ");
		System.out.println();
	}
	
	private void testRetrieveByteArrayNoOffset(Object sliceName) throws Exception {
		byte[] data = fss.retrieve(sliceName, blkSize);
		
		if(data==null)
			fail("Retrieved data is null");
		
		SliceDigest md=new SliceDigest(new SliceDigestInfo(blkSize, digestKey));
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		byte[] checksum=new byte[Resources.HASH_BIN_LEN];
		
		if(data.length<Resources.HASH_BIN_LEN)
			fail("Retrieved content is corrupted.");
		
		System.arraycopy(data, 0, checksum, 0, Resources.HASH_BIN_LEN);
		
		for(int i=Resources.HASH_BIN_LEN; i<data.length; i++){
			md.update(data[i]);
			out.write(data[i]);
		}
		md.finalizeDigest();
		
		String retrievedData=out.toString();
		System.out.println("Retrieved data: " + retrievedData);
		System.out.println("Verified data: " + sampleGetFileData);
		
		printByteArray("Retrieved checksum:", checksum);
		printByteArray("Cal checksum:", md.getSliceChecksum());
		
		if(!Arrays.equals(checksum, md.getSliceChecksum()))
			fail("Checksum retrieved does not match.");
		
		assertEquals("Content retrieved does not match.", sampleGetFileData, retrievedData);
	}
	
	private void testRetrieveByteArrayWithOffset(Object sliceName) throws Exception{
		//offset is the same as blk size
		long offset=blkSize;

		byte[] data = fss.retrieve(sliceName, offset, blkSize);
		if(data==null)
			fail("Retrieved data is null");
		
		if(data.length<Resources.HASH_BIN_LEN)
			fail("Retrieved content is corrupted.");

		SliceDigest md=new SliceDigest(new SliceDigestInfo(blkSize, digestKey));
		md.setOffset(offset);
		
		byte[] checksum=new byte[Resources.HASH_BIN_LEN];
		System.arraycopy(data, 0, checksum, 0, Resources.HASH_BIN_LEN);
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		for(int i=Resources.HASH_BIN_LEN; i<data.length; i++){
			md.update(data[i]);
			out.write(data[i]);
		}
		md.finalizeDigest();
		
		String retrievedData=out.toString();
		System.out.println("Retrieved data: " + retrievedData);
		System.out.println("Verified data: " + sampleGetFileData.substring((int)offset));
		
		printByteArray("Retrieved checksum:", checksum);
		printByteArray("Cal checksum:", md.getSliceChecksum());
		
		assertEquals("Content retrieved does not match.", sampleGetFileData.substring((int)offset), retrievedData);
		
		if(!Arrays.equals(checksum, md.getSliceChecksum()))
			fail("Checksum retrieved does not match.");
	}

	private void testRetrieveByteArrayWithOffsetAndLength(Object sliceName) throws Exception{
		//offset is the same as blk size
		long offset=blkSize;
		int dataLen=blkSize;

		byte[] data = new byte[dataLen+Resources.HASH_BIN_LEN];
		int size=fss.retrieve(sliceName, offset, dataLen, blkSize, data, 0);
		if(size==0)
			fail("Retrieved input is null");
		if(size<Resources.HASH_BIN_LEN+dataLen)
			fail("Retrieved content is corrupted.");

		SliceDigest md=new SliceDigest(new SliceDigestInfo(blkSize, digestKey));
		md.setOffset(offset);

		byte[] checksum=Arrays.copyOf(data, Resources.HASH_BIN_LEN);
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		for(int i=Resources.HASH_BIN_LEN; i<data.length; i++){
			md.update(data[i]);
			out.write(data[i]);
		}
		md.finalizeDigest();
		
		String retrievedData=out.toString();
		System.out.println("Retrieved data: " + retrievedData);
		System.out.println("Verified data: " + sampleGetFileData.substring((int)offset, (int)(offset+dataLen)));

		printByteArray("Retrieved checksum:", checksum);
		printByteArray("Cal checksum:", md.getSliceChecksum());
		
		if(!Arrays.equals(checksum, md.getSliceChecksum()))
			fail("Checksum retrieved does not match.");
		
		assertEquals("Content retrieved does not match.", sampleGetFileData.substring((int)offset, (int)(offset+dataLen)), retrievedData);
	}

	private void testStoreByteArray(Object sliceName) throws Exception{
		String testData="test data";
		SliceDigest md= new SliceDigest(new SliceDigestInfo(blkSize, digestKey));
		md.update(testData.getBytes());
		md.finalizeDigest();
		md.getSliceDigestInfo().setChecksum(md.getSliceChecksum());
		
		fss.store(testData.getBytes(), sliceName, md.getSliceDigestInfo());
		
		java.io.File f=new java.io.File(sliceStorePath+"/"+sliceFileName+".chk");
		if(!f.exists())
			fail("Checksum file cannot be found.");
		f=new java.io.File(sliceStorePath+"/"+sliceFileName);
		if(!f.exists())
			fail("Store file failed.");
		
		BufferedReader in = new BufferedReader(new FileReader(f));
		String readData=in.readLine();
		in.close();
		
		System.out.println("Retrieved data: " + readData);
		System.out.println("Verified data: " + testData);
		assertEquals("Contents retrieved does not match.", testData, readData);
	}
	
	private void testStoreStreamingWithOffset(Object sliceName) throws Exception{
		String testData="lots and lots of data";
		long offset=blkSize;
		SliceDigest md= new SliceDigest(new SliceDigestInfo(blkSize, digestKey));
		md.setOffset(offset);
		md.update(testData.getBytes());
		md.finalizeDigest();
		md.getSliceDigestInfo().setChecksum(md.getSliceChecksum());
		
		fss.store(testData.getBytes(), sliceName, offset, testData.length(), md.getSliceDigestInfo());
		
		java.io.File f=new java.io.File(sliceStorePath+"/"+sliceFileName+".chk");
		if(!f.exists())
			fail("Checksum file cannot be found.");
		f=null;
		f=new java.io.File(sliceStorePath+"/"+sliceFileName);
		if(!f.exists())
			fail("Store file failed.");
		
		RandomAccessFile in=new RandomAccessFile(f, "r");
		in.seek(offset);
		String readData=in.readLine();
		in.close();
		
		System.out.println("Retrieved data: " + readData);
		System.out.println("Verified data: " + testData);
		assertEquals("Contents retrieved does not match.", testData, readData);
	}
}
