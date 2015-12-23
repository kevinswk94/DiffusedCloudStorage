package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.FileSlice;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.filestore.PingBackMaster;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class FileSliceTest {
	private static String basePath=null;
	private static String sliceStorePath=null;
	private static java.io.File sampleFile=null;
	private static String sliceName="fsData";
	
	private static String namespace="urn:sit.nyp.edu.sg";
	private static String fserverId="TESTFS1";
	private static int fsPort=8010;
	private static int masterPort=9011;
	private static String masterUrl="localhost";
	
	private static String data = "test data test data";
	private static String dataChecksum=null;
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	private User user=new User("moeif_usrtest", "p@ssw0rd");
	
	private FileSlice fs=null;
	private FileSliceInfo fsi=null;
	private List<byte[]> blkHashes=null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=System.getProperty("user.dir");
		sliceStorePath=basePath+"/filestore/storage";
		sampleFile=new java.io.File(sliceStorePath + "/" + sliceName);
		
		java.io.File f=new java.io.File(basePath+"/svds.img");
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
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
		
		if(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("on")){
			masterUrl=MasterProperties.getString("master.namespace.ssl.address");
		}
		
		ClientProperties.set("client.master.rest.namespace.host", masterUrl);
		ClientProperties.set("client.master.rest.namespace.port", masterPort);
		masterUrl+=":"+masterPort;

		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_verify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterUrl);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup(fserverId, fsPort, fsPort+1, "localhost", sliceStorePath, 0);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);
		
		if(fsvr!=null)fsvr.shutdown();
		
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

	private static void forceUpdateMapping(){
		IFileSliceStore.removeServerMapping(fserverId);
		IFileSliceStore.updateServerMapping(fserverId, "localhost:"+fsPort, 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null,
				SliceStoreProperties.getString(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId), null);
	}
	
	@Before
	public void setUp() throws Exception {
		createSampleFile();
		
		//put in random hash generated
		blkHashes=new ArrayList<byte[]>();
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		
		byte[] digest;
		for(int i=0; i<3; i++){
			digest=md.digest(("test"+i).getBytes());
			blkHashes.add(digest);
			md.reset();
		}
		md=null;
		
		createSampleChecksumFile(blkHashes);
		
		dataChecksum=Resources.convertToHex((MessageDigest.getInstance(Resources.HASH_ALGO).digest(data.getBytes())));
		
		fsi=new FileSliceInfo(sliceName, fserverId, data.getBytes().length, dataChecksum, 0);
		
		fs=new FileSlice(fsi, user);
	}

	@After
	public void tearDown() throws Exception {
		blkHashes.clear();
		blkHashes=null;
	}
	
	private static void changeSliceStoreKey(){
		//ensure the require verification property is turn on
		SliceStoreProperties.set(SliceStoreProperties.PropName.REQ_VERIFY, "on");
		//register slice store again so that the master can generate the key
		PingBackMaster pbm=new PingBackMaster(masterUrl, ClientProperties.getString("client.master.rest.namespace.connector"), 
				fserverId, namespace, "localhost:"+fsPort);
		Properties p=new Properties();
		p.put(FileSliceServerInfo.RestletPropName.STATUS_HOST.value(), "localhost:"+(fsPort+1));
		p.put(FileSliceServerInfo.RestletPropName.STATUS_SSL.value(), 
				SliceStoreProperties.getString(FileSliceServerInfo.RestletPropName.STATUS_SSL.value()));
		pbm.registerFileSliceServer(p);
		
		//checks that there is key in the properties
		if(!SliceStoreProperties.exist(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fserverId))
			fail("Slice store key is not generated.");
	}

	@Test
	public void testRead() throws Exception{
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "off");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		read();
		
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		read();
	}
	
	private void read() throws Exception{
		//NOTE: verifying hash is not done here
		Random rand=new Random();
		
		//change slice store key every time so that each method will hit the exception codes
		changeSliceStoreKey();
		byte[] r=fs.read(0);
		System.out.println(r.length);
		assertTrue("Read fail.", r.length!=0);
		
		changeSliceStoreKey();
		r=fs.read(rand.nextInt(data.length()), 0);
		assertTrue("Read fail.", r.length!=0);
		
		changeSliceStoreKey();
		int len=fs.read(rand.nextInt(data.length()), 1, 0, r, 0);
		assertTrue("Read fail.", len!=0);
	}

	@Test
	public void testWrite() throws Exception{
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "off");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		write();
		
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		write();
	}
	
	private void write() throws Exception{
		//NOTE: verifying hash is not done here
		changeSliceStoreKey();
		FileSlice.Codes lastWriteStatus=fs.write(data.getBytes(), null);
		assertEquals("Write fail.", FileSlice.Codes.OK, lastWriteStatus);
		
		changeSliceStoreKey();
		lastWriteStatus=fs.write(data.getBytes(), null);
		assertEquals("Write fail.", FileSlice.Codes.OK, lastWriteStatus);
		
		changeSliceStoreKey();
		Random rand=new Random();
		lastWriteStatus=fs.write(data.getBytes(), rand.nextInt(data.length()), rand.nextInt(data.length())+1, null);
		assertEquals("Write fail.", FileSlice.Codes.OK, lastWriteStatus);
	}

	@Test
	public void testDelete() throws Exception{
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "off");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		delete();
		
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		delete();
	}
	
	private void delete() throws Exception{
		changeSliceStoreKey();
		
		fs.delete();
	}
	
	@Test
	public void testBlkHashes() throws Exception{
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "off");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		blkHashes();
		
		forceUpdateMapping();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		//force the file slice to change the slice store impl
		fs.setFileSliceInfo(fsi);
		
		blkHashes();
	}
	
	private void blkHashes() throws Exception{
		changeSliceStoreKey();
		
		//test the retrieval of blk hashes
		List<byte[]> arr=fs.getBlkHashes();
		
		for(int i=0; i<arr.size(); i++){		
			if(!Arrays.equals(blkHashes.get(i), arr.get(i)))
					fail("Retrieved blk hashes fail.");
		}
	}
	
	private void createSampleChecksumFile(List<byte[]> in ) throws Exception{
		//create checksum file
		java.io.File f=new java.io.File(sampleFile.getAbsolutePath()+".chk");
		if(f.exists())
			f.delete();
		
		FileOutputStream out=new FileOutputStream(f);
		
		for(byte[] b: in)
			out.write(b);
		
		out.flush();
		out.close();
		out=null;
		f=null;
	}
	
	private void createSampleFile() throws Exception{
		if(sampleFile.exists())
			sampleFile.delete();
		
		//create a dummy file that acts as file slice
		sampleFile.createNewFile();
		FileOutputStream fos = new FileOutputStream(sampleFile);
		fos.write(data.getBytes());
		fos.close();
	}
}
