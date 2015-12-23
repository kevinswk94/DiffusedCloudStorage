package sg.edu.nyp.sit.svds.client.filestore.impl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.filestore.impl.S3SliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.master.filestore.S3SliceStoreRegistration;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class S3SliceStoreTest {
	private static IFileSliceStore s3=null;
	private static Properties props=null;
	
	private String sliceName1="slice1";
	private String sliceName2="slice2";
	
	private String txtData="Hello, Good Day!";
	private String hashKey="abc";
	private int blkSize=10;
	
	private static String keyId="AKIAJ6UFCGZVW7YUKYPA";
	private static String key="kZH8kK/5S92AZlsskv4dp4WjXjtFkxIuL+lcHQhB";
	
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String svrId="ts3fs";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		props=new Properties();
		props.put(FileSliceServerInfo.S3PropName.CONTAINER.value(), "nypmoeif-ts3f");
		
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
	}
	
	private void setUpMaster() throws Exception{
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
		
		MasterProperties.set("slicestore.sharedaccess", "on");
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
		
		S3SliceStoreRegistration.skipConnect=true;
		S3SliceStoreRegistration.main(new String[]{basePath+"/client/test/sg/edu/nyp/sit/svds/client/s3File.txt", masterNSHost+":"+masterNSPort, "http"});
	}
	
	private static void checkS3() throws Exception{
		AmazonS3Client s3c=new AmazonS3Client(new BasicAWSCredentials(keyId, key));
		
		if(!s3c.doesBucketExist(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString())){
			s3c.createBucket(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString());
		}
	}
	
	//@Test
	public void testSharedAccessS3() throws Exception{
		setUpMaster();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		
		IFileSliceStore.updateServerMapping(svrId, "US_Standard", 
				FileSliceServerInfo.Type.S3, FileIOMode.NON_STREAM, null, null 
				, props);
		
		s3 = new S3SharedAccessSliceStore(svrId);
		IMasterNamespaceTable mns=MasterTableFactory.getNamespaceInstance();
		
		String[] urls1=mns.getSharedAccessURL(svrId, sliceName1, null);
		String[] urls2=mns.getSharedAccessURL(svrId, sliceName2, null);
		
		//STORING
		java.io.File f=new java.io.File(this.getClass().getResource("/testimage.jpg").getPath());

		InputStream in=new FileInputStream(f);
		
		//cal a digest to compare later
		byte[] tmp=new byte[512];
		byte[] data=new byte[(int)f.length()];
		int len, offset=0;
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		while((len=in.read(tmp))!=-1){
			md.update(tmp, 0, len);
			System.arraycopy(tmp, 0,data, offset, len);
			offset+=len;
		}
		in.close();
		String imgDigest=Resources.convertToHex(md.digest());

		System.out.println("WRITE IMAGE DATA");
		s3.store(data, urls1, null);
		System.out.println("END WRITE IMAGE DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());

		System.out.println("WRITE TEXT DATA");
		s3.store(txtData.getBytes(), urls2, smd.getSliceDigestInfo());
		System.out.println("END WRITE TEXT DATA");
		
		boolean hasError=false;
		try{
			s3.store(txtData.getBytes(), "tt", 0, 1, null);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");

		//RETRIEVING
		System.out.println("GET IMAGE DATA");
		byte[] r=s3.retrieve(urls1, 0);
		System.out.println("END IMAGE DATA");
		md.reset();
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		md.update(r);

		String getImgDigest=Resources.convertToHex(md.digest());
		
		System.out.println("Img digest: " + imgDigest);
		System.out.println("Get img digest: " + getImgDigest);
		assertTrue("Image retrieved is not correct", getImgDigest.equals(imgDigest));
		
		System.out.println("GET TEXT DATA");
		r=s3.retrieve(urls2, blkSize);
		System.out.println("END GET TEXT DATA");
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		//1st few bytes are the checksum
		byte[] btxtDigest=new byte[Resources.HASH_BIN_LEN];
		if(r.length<Resources.HASH_BIN_LEN)
			fail("Failed to retrieve slice hash.");
		System.arraycopy(r, 0, btxtDigest, 0, Resources.HASH_BIN_LEN);

		String getTxtDigest=Resources.convertToHex(btxtDigest);
		
		System.out.println("Text digest: " + txtDigest);
		System.out.println("Get Text digest: " + getTxtDigest);
		
		assertTrue("Text digest retrieved is not correct.", getTxtDigest.equals(txtDigest));
		
		byte[] btxtData=new byte[txtData.getBytes().length];
		System.arraycopy(r, Resources.HASH_BIN_LEN, btxtData, 0, r.length-Resources.HASH_BIN_LEN);
		String getTxtData=new String(btxtData);
		
		System.out.println("Get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData));
		
		hasError=false;
		try{ 
			s3.retrieve(urls1, 0, 1, 0, r, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		hasError=false;
		try{ 
			s3.retrieve(urls1, 0, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");

		//DELETE
		s3.delete(urls1);
		s3.delete(urls2);

		System.out.println("ENDED");
	}
	
	@Test
	public void testS3() throws Exception{
		IFileSliceStore.updateServerMapping(svrId, "US_Standard", 
				FileSliceServerInfo.Type.S3, FileIOMode.NON_STREAM, keyId, key 
				, props);
		
		checkS3();
		
		s3 = new S3SliceStore(svrId);
		
		//STORING
		java.io.File f=new java.io.File(this.getClass().getResource("/testimage.jpg").getPath());

		InputStream in=new FileInputStream(f);
		
		//cal a digest to compare later
		byte[] tmp=new byte[512];
		byte[] data=new byte[(int)f.length()];
		int len, offset=0;
		MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		while((len=in.read(tmp))!=-1){
			md.update(tmp, 0, len);
			System.arraycopy(tmp, 0,data, offset, len);
			offset+=len;
		}
		in.close();
		String imgDigest=Resources.convertToHex(md.digest());

		System.out.println("WRITE IMAGE DATA");
		s3.store(data, sliceName1, null);
		System.out.println("END WRITE IMAGE DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());

		System.out.println("WRITE TEXT DATA");
		s3.store(txtData.getBytes(), sliceName2, smd.getSliceDigestInfo());
		System.out.println("END WRITE TEXT DATA");
		
		boolean hasError=false;
		try{
			s3.store(txtData.getBytes(), "tt", 0, 1, null);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");

		//RETRIEVING
		System.out.println("GET IMAGE DATA");
		byte[] r=s3.retrieve(sliceName1, 0);
		System.out.println("END IMAGE DATA");
		md.reset();
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		md.update(r);

		String getImgDigest=Resources.convertToHex(md.digest());
		
		System.out.println("Img digest: " + imgDigest);
		System.out.println("Get img digest: " + getImgDigest);
		assertTrue("Image retrieved is not correct", getImgDigest.equals(imgDigest));
		
		System.out.println("GET TEXT DATA");
		r=s3.retrieve(sliceName2, blkSize);
		System.out.println("END GET TEXT DATA");
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		//1st few bytes are the checksum
		byte[] btxtDigest=new byte[Resources.HASH_BIN_LEN];
		if(r.length<Resources.HASH_BIN_LEN)
			fail("Failed to retrieve slice hash.");
		System.arraycopy(r, 0, btxtDigest, 0, Resources.HASH_BIN_LEN);

		String getTxtDigest=Resources.convertToHex(btxtDigest);
		
		System.out.println("Text digest: " + txtDigest);
		System.out.println("Get Text digest: " + getTxtDigest);
		
		assertTrue("Text digest retrieved is not correct.", getTxtDigest.equals(txtDigest));
		
		byte[] btxtData=new byte[txtData.getBytes().length];
		System.arraycopy(r, Resources.HASH_BIN_LEN, btxtData, 0, r.length-Resources.HASH_BIN_LEN);
		String getTxtData=new String(btxtData);
		
		System.out.println("Get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData));
		
		hasError=false;
		try{ 
			s3.retrieve(sliceName1, 0, 1, 0, r, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		hasError=false;
		try{ 
			s3.retrieve(sliceName1, 0, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");

		//DELETE
		s3.delete(sliceName1);
		s3.delete(sliceName2);
		
		System.out.println("ENDED");
	}

}
