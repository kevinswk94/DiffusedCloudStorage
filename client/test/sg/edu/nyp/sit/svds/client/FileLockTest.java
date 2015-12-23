package sg.edu.nyp.sit.svds.client;

import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Date;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.LockedSVDSException;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class FileLockTest {
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String namespace="urn:sit.nyp.edu.sg";
	
	private static sg.edu.nyp.sit.svds.filestore.Main fsvr=null;
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	private static User user1=new User("moeif_usrtest", "p@ssw0rd");
	private static User user2=new User("moeif_usrtest2", "p@ssw0rd");
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		basePath=masterFilePath=System.getProperty("user.dir");
		
		ClientProperties.init();
		ClientProperties.load(new java.io.File(basePath+"/resource/svdsclient_unitTest.properties"));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		deleteFiles();
		
		clearSlices(basePath+"/filestore/storage");
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

	@Before
	public void setUp() throws Exception {
		deleteFiles();
		
		int masterNSPort=9011, masterFilePort=9010;
		String masterNSHost="localhost", masterFileHost="localhost";
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
	
	private static void clearSlices(String path){
		java.io.File root=new java.io.File(path);
		for(java.io.File f:root.listFiles()){
			if(f.isDirectory())
				continue;
			
			f.delete();
		}
	}
	
	@Test
	public void testLockFile() throws Exception{
		String folder1="abc", subfolder1="def";
		String file1="i.txt", subfile1="ii.txt";
		
		//create directories and file in the following structure:
		// root/abc
		// root/i.txt
		// root/abc/def
		// root/abc/ii.txt
		
		sg.edu.nyp.sit.svds.client.File f1=null, f2=null;
		SVDSOutputStream out1=null, out2=null;
		boolean hasError;
		
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1, user1)).createNewDirectory();
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfolder1, user1)).createNewDirectory();
		
		f1=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+file1, user1);
		f1.createNewFile();
		out1=new SVDSOutputStream(f1, false);
		out1.write("Hello Word".getBytes());
		out1.close();
		out1=null;
		f1=null;
		
		f1=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfile1, user1);
		f1.createNewFile();
		out1=new SVDSOutputStream(f1, false);
		out1.write("Good day".getBytes());
		out1.close();
		out1=null;
		f1=null;
		
		//open subfile1 by user1 in writing
		f1=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfile1, user1);
		if(!f1.exist())
			fail("File does not exist.");
		out1=new SVDSOutputStream(f1, false);
		
		//attempt to open subfile1 by user2 in writting, should get exception
		f2=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfile1, user2);
		if(!f2.exist())
			fail("File does not exist.");
		try{
			out2=new SVDSOutputStream(f2, false);
		}catch(LockedSVDSException ex){
			out2=null;
		}
		if(out2!=null)
			fail("File is not locked.");
		
		//close subfile1 by user1
		out1.close();
		out1=null;
		f1=null;
		
		//attempt to open subfile1 by user2 in writing, should get through now
		try{
			out2=new SVDSOutputStream(f2, false);
		}catch(LockedSVDSException ex){
			out2=null;
		}
		if(out2==null)
			fail("File is not unlocked.");
		
		//attempt to delete subfile1 while open for writing, should get exception
		try{
			hasError=false;
			f2.deleteFile();
		}catch(LockedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("File is deleted while opened for writing.");
		
		//attempt to rename/move subfile1 while open for writing, should get exception
		try{
			hasError=false;
			f2.moveTo(FileInfo.PATH_SEPARATOR+"iii.txt");
		}catch(LockedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("File is renamed/moved while opened for writing.");
		
		
		//attempt to rename/move folder1 (where subfile1 resides in), should get exception
		f1=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1, user2);
		try{
			hasError=false;
			f1.moveTo(FileInfo.PATH_SEPARATOR+folder1+"1");
		}catch(LockedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Folder is renamed/move while file that resides in it is opened for writing.");
		
		//attempt to rename/move subfolder1 (resides in same directory as subfile1)
		f1=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfolder1, user2);
		subfolder1=subfolder1+"1";
		f1.moveTo(FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfolder1);
		f1=null;
		
		//close subfile1
		out2.close();
		out2=null;
		f2=null;
		
		//delete all files and folders
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfolder1, user1)).deleteDirectory();
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1+FileInfo.PATH_SEPARATOR+subfile1, user1)).deleteFile();
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+file1, user1)).deleteFile();
		(new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+folder1, user1)).deleteDirectory();
	}

	@Test
	public void testLockRefresher() throws Exception{
		String fname="filelck";
		sg.edu.nyp.sit.svds.client.File f=null;
		SVDSOutputStream out=null;
		long interval, intervalToCheck;
		IMasterFileTable mt=null;
		FileInfo fi=null;
		boolean errFlag;
		
		//load the interval prop
		/*
		Properties prop = new Properties();
		prop.load(this.getClass().getResourceAsStream("/MasterConfig.properties"));
		if(!prop.containsKey(Resources.SYS_LOCK_INTERVAL))
			fail("Cannot find property.");
		
		interval=(new Long(prop.get(Resources.SYS_LOCK_INTERVAL).toString()))*1000;
		intervalToCheck=interval+(2*1000);
		*/
		ClientProperties.set(ClientProperties.PropName.FILE_LOCK_INTERVAL.value(), 10L);
		MasterProperties.set(MasterProperties.PropName.FILE_LOCK_INTERVAL.value(), 10L);
		interval=10*1000;
		intervalToCheck=interval+(2*1000);
		
		//some setup
		mt=MasterTableFactory.getFileInstance();
		Field i= sg.edu.nyp.sit.svds.client.File.class.getDeclaredField("fInfo");
		i.setAccessible(true);
		
		//create a sample file
		f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+fname, user1);
		f.createNewFile();
		out=new SVDSOutputStream(f, false);
		out.write("Hello Word".getBytes());
		out.close();
		out=null;
		f=null;
		
		//open the file again so the lock refresher will be invoke
		f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+fname, user1);
		if(!f.exist())
			fail("File is not created.");
		
		out=new SVDSOutputStream(f, false);
		
		System.out.println("check if file is locked after the interval expires, sleep for " + intervalToCheck);
		//check if file is locked after the interval expires
		Thread.sleep(intervalToCheck);
		
		fi=(FileInfo)i.get(f);
		
		//attempt to lock the file by another user
		System.out.println("attempt to lock the file by another user");
		errFlag=false;
		fi.setLockBy(user2);
		try{
			mt.lockFileInfo(fi, user2);
		}catch(LockedSVDSException ex){
			errFlag=true;
		}
		if(!errFlag)
			fail("File is not locked.");
		
		System.out.println("close the stream (which will unlock the file automatically)");
		//close the stream (which will unlock the file automatically)
		fi.setLockBy(user1);
		out.close();
		out=null;

		f.deleteFile();
		
		mt=null;
	}
	
	@Test
	public void testLockExpiry() throws Exception{
		String fname="filelckexp";
		sg.edu.nyp.sit.svds.client.File f=null;
		SVDSOutputStream out=null;
		long interval, intervalToCheck;
		IMasterFileTable mt=null;
		FileInfo fi=null;
		boolean errFlag;
		
		//load the interval prop
		/*
		Properties prop = new Properties();
		prop.load(this.getClass().getResourceAsStream("/MasterConfig.properties"));
		if(!prop.containsKey(Resources.SYS_LOCK_INTERVAL))
			fail("Cannot find property.");
		
		interval=(new Long(prop.get(Resources.SYS_LOCK_INTERVAL).toString()))*1000;
		*/
		interval=10*1000;
		ClientProperties.set(ClientProperties.PropName.FILE_LOCK_INTERVAL, 10L);
		MasterProperties.set(MasterProperties.PropName.FILE_LOCK_INTERVAL, 10L);
		intervalToCheck=interval+(2*1000);
		
		//some setup
		mt=MasterTableFactory.getFileInstance();
		Field i= sg.edu.nyp.sit.svds.client.File.class.getDeclaredField("fInfo");
		i.setAccessible(true);
		
		//create a sample file
		f=new sg.edu.nyp.sit.svds.client.File(namespace+FileInfo.PATH_SEPARATOR+fname, user1);
		f.createNewFile();
		out=new SVDSOutputStream(f, false);
		out.write("Hello Word".getBytes());
		out.close();
		out=null;
	
		fi=(FileInfo)i.get(f);
		
		//1. check if file can be lock by another user after the interval expires
		//manually lock the file
		mt.lockFileInfo(fi, user1);
		
		//test lock
		fi.setLockBy(user2);
		errFlag=false;
		try{
			mt.lockFileInfo(fi, user2);
		}catch(LockedSVDSException ex){
			errFlag=true;
		}
		if(!errFlag)
			fail("File is not locked.");
		
		//wait for lock to expire
		Thread.sleep(intervalToCheck);
		
		//attempt to lock again
		mt.lockFileInfo(fi, user2);
		
		//2. check if file can be updated by another user after interval expires
		//update some file info
		fi.setOwner(user2);
		fi.setLastModifiedDate(new Date());
		
		//test lock
		fi.setLockBy(user1);
		errFlag=false;
		try{
			mt.updateFileInfo(fi, user1);
		}catch(LockedSVDSException ex){
			errFlag=true;
		}
		if(!errFlag)
			fail("File is not locked.");
		
		//wait for lock to expire
		Thread.sleep(intervalToCheck);
		
		//attempt to update again
		mt.updateFileInfo(fi, user1);
		
		//3. check if file can be rename or move after interval expires
		//lock the file again because if the file is updated without pre-locking,
		//lock will be release after update is complete
		mt.lockFileInfo(fi, user1);
		
		//test lock
		fi.setLockBy(user2);
		errFlag=false;
		fname=fname+"1";
		try{
			f.moveTo(FileInfo.PATH_SEPARATOR+fname);
		}catch(LockedSVDSException ex){
			errFlag=true;
		}
		if(!errFlag)
			fail("File is not locked.");
		
		//wait for lock to expire
		Thread.sleep(intervalToCheck);
		
		//attempt to update again
		f.moveTo(FileInfo.PATH_SEPARATOR+fname);

		//4. check if file can be deleted after interval expires
		//lock the file again because if the file is unlocked after rename/move
		mt.lockFileInfo(fi, user2);
		
		//test lock
		fi.setLockBy(user1);
		errFlag=false;
		try{
			f.deleteFile();
		}catch(LockedSVDSException ex){
			errFlag=true;
		}
		if(!errFlag)
			fail("File is not locked.");
		
		//wait for lock to expire
		Thread.sleep(intervalToCheck);
		
		//attempt to delete again
		f.deleteFile();
		
		mt=null;
	}
}
