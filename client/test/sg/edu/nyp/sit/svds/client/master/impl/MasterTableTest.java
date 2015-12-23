package sg.edu.nyp.sit.svds.client.master.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.filestore.PingBackMaster;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class MasterTableTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	
	IMasterFileTable mft;
	IMasterNamespaceTable mnt;
	
	private static String namespace="urn:sit.nyp.edu.sg";
	private static String fsId="TESTFS1";
	private static int fsPort=8010;
	private static String fsUrl="localhost:"+fsPort;
	private static int masterFilePort=9010, masterNSPort=9011;
	private static String masterNSUrl="localhost", masterFileUrl="localhost";
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private String filename1=FileInfo.PATH_SEPARATOR+"test1.txt";
	private int noOfSlice=2;

	private User user=new User("moeif_usrtest", "p@ssw0rd");
	
	private static String fsPropName="pname";
	private static String fsPropValue="pvalue";

	@BeforeClass
	public static void init() throws Exception {	
		basePath=masterFilePath=System.getProperty("user.dir");
		
		File f=new File(masterFilePath+"/svds.img");
		if(f.exists())
			f.delete();
		
		f=new File(masterFilePath+"/svdsTrans.log");
		if(f.exists())
			f.delete();
		
		f=new File(masterFilePath+"/namespaceTrans.log");
		if(f.exists())
			f.delete();

		msvr=new sg.edu.nyp.sit.svds.master.Main(basePath+"/resource/IDAProp.properties",
				basePath+"/resource/MasterConfig_unitTest.properties");
		MasterProperties.set("master.directory", masterFilePath);
		MasterProperties.set("master.file.port", masterFilePort);
		MasterProperties.set("master.namespace.port", masterNSPort);
		//do not turn on 2 way ssl
		MasterProperties.set("master.namespace.ssl.clientauth", "off");
		MasterProperties.set("master.maintainence.ssl.clientauth", "off");
		msvr.startupMain();
		
		if(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("on")){
			masterNSUrl=MasterProperties.getString("master.namespace.ssl.address");
		}
		if(MasterProperties.getString("master.file.ssl").equalsIgnoreCase("on")){
			masterFileUrl=MasterProperties.getString("master.file.ssl.address");
		}
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
		ClientProperties.set("client.master.rest.file.host", masterFileUrl);
		ClientProperties.set("client.master.rest.namespace.host", masterNSUrl);
		ClientProperties.set("client.master.rest.file.port", masterFilePort);
		ClientProperties.set("client.master.rest.namespace.port", masterNSPort);
		
		masterFileUrl+=":"+masterFilePort;
		masterNSUrl+=":"+masterNSPort;

		//before starting the test, the file slice server must first register with the master table
		//as the namespace urn:sit.nyp.edu.sg
		fsvr=new sg.edu.nyp.sit.svds.filestore.Main(basePath+"/resource/SliceStoreConfig_noverify_unitTest.properties");
		SliceStoreProperties.set("master.address", masterNSUrl);
		SliceStoreProperties.set("slicestore.namespace", namespace);
		fsvr.startup(fsId, fsPort, fsPort+1, "localhost", basePath+"/filestore/storage", 0);
		//add extra properties so can test as restlet fs by default does not have any prop
		registerSliceServerExtraProps();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		//sleep for 5 seconds so as to wait for all request to be completed
		//at either the master or slice server side
		Thread.sleep(1000*5);

		if(msvr!=null) msvr.shutdown();
		if(fsvr!=null) fsvr.shutdown();
	}
	
	@Before
	public void setUp() throws Exception {
		mft=MasterTableFactory.getFileInstance();
		mnt=MasterTableFactory.getNamespaceInstance();
	}
	
	@After
	public void tearDown() throws Exception{
		mft=null;
	}
	
	@Test
	public void testMasterTable() throws Exception{
		//get any slice stores that support any mode
		List<FileSliceInfo> slices = testGenerateFileSliceInfo(FileIOMode.BOTH);
		
		testFileSliceServerKey();
		
		testAddFileInfo(filename1, slices);

		FileInfo fi = testGetFileInfo(filename1);
		
		Date dt=new Date(0);
		testRefreshDirectory(dt, fi, 1);
		
		testMoveRenameFileInfo(fi, FileInfo.PATH_SEPARATOR+"testa.txt");
		
		testLockFileInfo(fi);
		
		testUpdateFileInfo(fi);
		
		testRefreshDirectory(dt, fi, 1);
		
		unlockFileInfo(fi);
		
		testAccessFile(fi);
		
		testNamespaceMemory();
		
		testDeleteFileInfo(fi);
		
		Date dt1=new Date(dt.getTime());
		testRefreshDirectory(dt, fi, 0);
		
		if(dt1.getTime()==dt.getTime())
			fail("Refresh directory check time is the same");
		
		testAddBlankFileInfo();
		
		slices.clear();
		slices=null;
		
		System.out.println("DONE");
	}
	
	private static void registerSliceServerExtraProps(){
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl=ClientProperties.getString("client.master.rest.namespace.connector")+"://" 
			+ masterNSUrl + "/namespace/register?"
			+RestletMasterQueryPropName.Namespace.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
			+"&"+RestletMasterQueryPropName.Namespace.SVR_ID.value()+"="+URLEncoder.encode(fsId, "UTF-8")
			+"&"+RestletMasterQueryPropName.Namespace.SVR_HOST.value()+"="+URLEncoder.encode(fsUrl, "UTF-8")
			+"&"+RestletMasterQueryPropName.Namespace.SVR_TYPE.value()+"="+FileSliceServerInfo.Type.RESTLET.value()
			+"&"+RestletMasterQueryPropName.Namespace.SVR_REQ_VERIFY.value()+"=" + SliceStoreProperties.getString(SliceStoreProperties.PropName.REQ_VERIFY)
			+"&"+RestletMasterQueryPropName.Namespace.SVR_MODE.value()+"="+FileIOMode.STREAM.value();
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			fsConn.setDoOutput(true);
			
			OutputStream out=fsConn.getOutputStream();
			out.write((Resources.encodeKeyValue(fsPropName)+"="+Resources.encodeKeyValue(fsPropValue)+"\n").getBytes());
			out.flush();
			out.close();
			
			int resp=fsConn.getResponseCode();
			if(resp!=HttpURLConnection.HTTP_OK)
				throw new Exception(resp+":"+fsConn.getResponseMessage());
		}catch(Exception ex){
			ex.printStackTrace();
			fail("Error adding extra options to slice server. " + ex.getMessage());
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	private void testFileSliceServerKey() throws Exception{
		//since we are not writing to the actual slice server, the property
		//can be set during runtime
		
		//ensure the require verification property is turn on
		SliceStoreProperties.set(SliceStoreProperties.PropName.REQ_VERIFY, "on");
		//register slice store again so that the master can generate the key
		PingBackMaster pbm=new PingBackMaster(masterNSUrl, 
				ClientProperties.getString("client.master.rest.namespace.connector"), 
				fsId, namespace, "localhost"+fsPort);
		Properties p=new Properties();
		p.put(FileSliceServerInfo.RestletPropName.STATUS_HOST.value(), "localhost:"+(fsPort+1));
		p.put(FileSliceServerInfo.RestletPropName.STATUS_SSL.value(), SliceStoreProperties.getString(FileSliceServerInfo.RestletPropName.STATUS_SSL.value()));
		pbm.registerFileSliceServer(p);
		
		//checks that there is key in the properties
		if(!SliceStoreProperties.exist(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fsId))
			fail("Slice store key is not generated.");
		
		String genKey=SliceStoreProperties.getString(SliceStoreProperties.PropName.SLICESTORE_KEY.value()+"."+fsId);
		
		//deliberately set slice store key in mapping to empty
		IFileSliceStore.updateServerKey(fsId, null, null);
		
		mnt.refreshRestletSliceServerKey(fsId, user);
		
		if(!genKey.equals(IFileSliceStore.getServerMapping(fsId).getKey()))
			fail("Refreshed slice store key does not match");
	}
	
	private void testAccessFile(FileInfo fi) throws Exception{
		mft.accessFile(fi, user);
	}
	
	private Date testRefreshDirectory(Date dt, FileInfo fi, int expectedRec) throws Exception{
		String parentDirectory=(fi.getFullPath().indexOf(FileInfo.PATH_SEPARATOR)>0 ?
				fi.getFullPath().substring(0, fi.getFullPath().lastIndexOf(FileInfo.PATH_SEPARATOR))
				: FileInfo.PATH_SEPARATOR);
		
		List<FileInfo> files=mft.refreshDirectoryFiles(fi.getNamespace(), parentDirectory, dt, user);
		
		System.out.println("expected="+expectedRec+", retrieved="+(files==null?-1: files.size()));
		
		if(expectedRec<0 && files!=null)
			fail("Should have no changes in the directory.");
		else if(expectedRec>=0 && files==null)
			fail("No changes is detected in the directory.");
		else if(files!=null && files.size()!=expectedRec)
			fail("Retrieved no of file records does not match.");
		
		return dt;
	}
	
	private void testNamespaceMemory() throws Exception{
		NamespaceInfo ni=mnt.getNamespaceMemory(namespace, user);
		
		if(ni==null)
			fail("Unable to retrieve namespace memory usage.");
		
		if(ni.getTotalMemory()<=0)
			fail("Total memory retrieved is empty.");
		
		System.out.println(namespace+ " memory usage:");
		System.out.println("Total: " + ni.getTotalMemory() + " bytes");
		System.out.println("Available: " + ni.getMemoryAvailable() + " bytes");
		System.out.println("Used: " + ni.getMemoryUsed() + " bytes");
	}
	
	private void testLockFileInfo(FileInfo fi) throws Exception{
		fi.setLockBy(user);
		mft.lockFileInfo(fi, user);
	}
	
	private void unlockFileInfo(FileInfo fi) throws Exception{
		fi.setLockBy(user);
		mft.unlockFileInfo(fi, user);
	}
	
	private void testAddBlankFileInfo() throws Exception{
		FileInfo fi=new FileInfo(filename1, namespace, FileInfo.Type.FILE);
		
		fi.setOwner(user);
		fi.setIdaVersion(1);
		fi.setFileSize(0);
		fi.setBlkSize(102400);
		fi.setKeyHash("abc");
		
		mft.addFileInfo(fi, user);
		
		if(fi.getCreationDate()==null)
			fail("Creation date not returned by master application.");
		
		mft.deleteFileInfo(fi, user);
	}

	private void testAddFileInfo(String filename, List<FileSliceInfo> slices)
		throws Exception {
		FileInfo fi=new FileInfo(filename, namespace, 
				FileInfo.Type.FILE);
		fi.setOwner(user);
		fi.setIdaVersion(1);
		fi.setFileSize(100);
		fi.setBlkSize(102400);
		fi.setKeyHash("abc");

		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		for(int i=0; i<slices.size(); i++){
			slices.get(i).setSliceChecksum(Resources.convertToHex(md.digest((i+"").getBytes())));
			slices.get(i).setSliceSeq(i);
			md.reset();
		}

		fi.setSlices(slices);

		mft.addFileInfo(fi, user);
		
		if(fi.getCreationDate()==null)
			fail("Creation date not returned by master application.");
	}

	private void testDeleteFileInfo(FileInfo fi) throws Exception {
		mft.deleteFileInfo(fi, user);
	}

	private FileInfo testGetFileInfo(String filename) throws Exception {
		//clear away any properties
		if(IFileSliceStore.getServerMapping(fsId)!=null){
			IFileSliceStore.getServerMapping(fsId).clearAllProperties();
		}
		
		FileInfo fi=mft.getFileInfo(namespace, filename, user);
		if(fi==null)
			fail("Cannot retrieve file info.");
		
		//when master table funct returns the mapping should be updated, check
		//if it contains properties
		if(!IFileSliceStore.getServerMapping(fsId).hasProperties())
			fail("Slice server(s) from generated file slice(s) does not contains any extra options.");

		assertEquals("Retrieved file size does not match.", 100L, fi.getFileSize());

		if(fi.getSlices().size()!=noOfSlice)
			fail("Retrieve file does not have correct num of file slice.");

		List<String> sliceChecksums=new ArrayList<String>();
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		for(int i=0; i<noOfSlice; i++){
			sliceChecksums.add(Resources.convertToHex(md.digest((i+"").getBytes())));
			md.reset();
		}
		
		//the order of slice returned may not be the same
		boolean isFound;
		for(String c: sliceChecksums){
			isFound=false;
			for(FileSliceInfo i: fi.getSlices()){
				if(i.getSliceChecksum().equals(c)){
					isFound=true;
					break;
				}
			}
			if(!isFound)
				fail("Retrieved file slice checksum does not match.");
		}
		sliceChecksums.clear();
		sliceChecksums=null;
		
		return fi;
	}

	private void testUpdateFileInfo(FileInfo fi)  throws Exception  {
		fi.setFileSize(150);
		fi.getSlices().remove(0); //attempt to remove 1 slice
		fi.setLastModifiedDate(new Date());
		fi.setLockBy(user);

		mft.updateFileInfo(fi, user);
	}
	
	private void testMoveRenameFileInfo(FileInfo fi, String new_path) throws Exception{
		fi.setLastModifiedDate(new Date());
		mft.moveFileInfo(fi, fi.getNamespace(), new_path, user);
		fi.setFullPath(new_path);
	}

	private List<FileSliceInfo> testGenerateFileSliceInfo(FileIOMode pref) throws Exception{
		//clear away any properties
		if(IFileSliceStore.getServerMapping(fsId)!=null){
			IFileSliceStore.getServerMapping(fsId).clearAllProperties();
		}
		
		List<FileSliceInfo> slices=mft.generateFileSliceInfo(namespace,
			noOfSlice, pref, user);
		if(slices==null)
			fail("Cannot generate file slice metadata.");

		//when master table funct returns the mapping should be updated, check
		//if it contains properties
		if(!IFileSliceStore.getServerMapping(fsId).hasProperties())
			fail("Slice server(s) from generated file slice(s) does not contains any extra options.");
		
		/*
		System.out.println("-------------------------------------------------");
		System.out.println("GENERATED FILE SLICE INFO");
		System.out.println();
		for(FileSliceInfo i: slices){
			System.out.println("Slice name: " + i.getSliceName());
			System.out.println("Server Id: " + i.getServerId());
			System.out.println("Server host: " + i.getServerHost());
			System.out.println();
		}
		System.out.println("-------------------------------------------------");
		*/

		return slices;
	}
}
