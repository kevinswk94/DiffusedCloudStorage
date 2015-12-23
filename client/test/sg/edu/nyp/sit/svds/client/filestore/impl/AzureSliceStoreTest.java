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

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.SliceDigest;
import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.filestore.impl.AzurePageBlobSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.master.filestore.AzureSliceStoreRegistration;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.SliceDigestInfo;

public class AzureSliceStoreTest {
	private static IFileSliceStore az = null;
	
	private static String sliceName1="slice1";
	private static String sliceName2="slice2";
	private static String sliceName3="slice3";
	
	private static Properties props=new Properties();
	
	private String txtData="Hello, Good Day!";
	private String hashKey="abc";
	private int blkSize=10;
	
	private static String acct="moeifazuretest";
	private static String key="WnPVqUCo+wkE5XYZCdeLb5WziYjbkjZlN4xWYFcMLUJiWOyiav/3rg7RSGpiiPF1dBdAsPG1GOiY7/689Nap5g==";
	
	private static sg.edu.nyp.sit.svds.master.Main msvr=null;
	private static String basePath=null;
	private static String masterFilePath=null;
	private static String svrId="afs1";
	
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
	}
	
	/*
	@Test
	public void clearBlobs() throws Exception{
		IFileSliceStore.updateServerMapping("afs1", "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.STREAM, acct, key, props);
		
		AzurePageBlobSliceStore a=new AzurePageBlobSliceStore("afs1");
		
		a.clearBlobs();
	}
	*/
	
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
		
		AzureSliceStoreRegistration.main(new String[]{basePath+"/client/test/sg/edu/nyp/sit/svds/client/azureFile.txt", masterNSHost+":"+masterNSPort, "http"});
	}
	
	//@Test
	public void testSharedAccessNonStreamingAzure() throws Exception{
		setUpMaster();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		
		IFileSliceStore.updateServerMapping(svrId, "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.NON_STREAM, null, null, props);
		
		az=new AzureSharedAccessBlockBlobSliceStore(svrId);
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
		az.store(data, urls1, null);
		System.out.println("END WRITE IMAGE DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());
		
		System.out.println("WRITE TEXT DATA");
		az.store(txtData.getBytes(), urls2, smd.getSliceDigestInfo());
		System.out.println("END WRITE TEXT DATA");
		
		boolean hasError=false;
		try{
			az.store(txtData.getBytes(), "tt", 0, 1, null);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		//RETRIEVING
		System.out.println("GET IMAGE DATA");
		byte[] r=az.retrieve(urls1, 0);
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
		r=az.retrieve(urls2, blkSize);
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
			az.retrieve(urls1, 0, 1, 0, r, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		hasError=false;
		try{ 
			az.retrieve(urls1, 0, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		//DELETE
		az.delete(urls1);
		az.delete(urls2);

		System.out.println("ENDED");
	}
	
	@Test
	public void testNonStremaingAzure() throws Exception{
		IFileSliceStore.updateServerMapping("testazurestore", "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.NON_STREAM, acct, key, props);

		az=new AzureBlockBlobSliceStore("testazurestore");
		
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
		az.store(data, sliceName1, null);
		System.out.println("END WRITE IMAGE DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());
		
		System.out.println("WRITE TEXT DATA");
		az.store(txtData.getBytes(), sliceName2, smd.getSliceDigestInfo());
		System.out.println("END WRITE TEXT DATA");
		
		boolean hasError=false;
		try{
			az.store(txtData.getBytes(), "tt", 0, 1, null);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		//RETRIEVING
		System.out.println("GET IMAGE DATA");
		byte[] r=az.retrieve(sliceName1, 0);
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
		r=az.retrieve(sliceName2, blkSize);
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
			az.retrieve(sliceName1, 0, 1, 0, r, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		hasError=false;
		try{ 
			az.retrieve(sliceName1, 0, 0);
		}catch(NotSupportedSVDSException ex){
			hasError=true;
		}
		if(!hasError)
			fail("Method should not be supported.");
		
		//DELETE
		az.delete(sliceName1);
		az.delete(sliceName2);

		System.out.println("ENDED");
	}

	//@Test
	public void testSharedAccessStreamingAzure() throws Exception{
		setUpMaster();
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		
		IFileSliceStore.updateServerMapping(svrId, "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.STREAM, null, null, props);
		
		az=new AzureSharedAccessPageBlobSliceStore(svrId);
		IMasterNamespaceTable mns=MasterTableFactory.getNamespaceInstance();
		
		String[] urls1=mns.getSharedAccessURL(svrId, sliceName1, null);
		String[] urls2=mns.getSharedAccessURL(svrId, sliceName2, null);
		String[] urls3=mns.getSharedAccessURL(svrId, sliceName3, null);
		
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

		System.out.println("WRITE IMG DATA");
		az.store(data, urls1, null);
		System.out.println("Store img byte size: " + data.length);
		System.out.println("END WRITE IMG DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());
		
		System.out.println("WRITE FULL TEXT DATA");
		az.store(txtData.getBytes(), urls2, smd.getSliceDigestInfo());
		System.out.println("END WRITE FULL TEXT DATA");
		
		int slice3Len=5;
		System.out.println("WRITE PARTIAL TEXT DATA");
		az.store(txtData.getBytes(), urls3, 0, slice3Len, null);
		System.out.println("END WRITE PARTIAL TEXT DATA");

		//RETRIEVING
		System.out.println("GET IMG DATA");
		byte[] r=az.retrieve(urls1, 0);
		System.out.println("END GET IMG DATA");
		md.reset();
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		//need to know the actual length containing data cos azure store data in mutliples of 512 bytes
		md.update(r, 0, data.length);

		String getImgDigest=Resources.convertToHex(md.digest());
		
		System.out.println("Img digest: " + imgDigest);
		System.out.println("Get img digest: " + getImgDigest);
		assertTrue("Image retrieved is not correct", getImgDigest.equals(imgDigest));
		
		System.out.println("GET FULL TEXT DATA");
		r=az.retrieve(urls2, blkSize);
		System.out.println("END GET FULL TEXT DATA");
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
		System.arraycopy(r, Resources.HASH_BIN_LEN, btxtData, 0, btxtData.length);
		String getTxtData=new String(btxtData);
		
		System.out.println("Get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData));
		 
		btxtData=new byte[8];
		System.out.println("GET RANDOM TEXT DATA 1");
		len=az.retrieve(urls2, 7, 8, 0, btxtData, 0);
		System.out.println("END GET RANDOM TEXT DATA 1");
		System.out.println("Len retrieved: " + len);
		getTxtData=new String(btxtData);
		System.out.println("Random get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData.substring(7, 15)));

		System.out.println("GET RANDOM TEXT DATA 2");
		btxtData=az.retrieve(urls2, 12, 0);
		System.out.println("GET RANDOM TEXT DATA 2");
		getTxtData=new String(btxtData,0, 4);
		System.out.println("Random get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData.substring(12)));
		
		az.delete(urls1);
		az.delete(urls2);
		az.delete(urls3);
		
		System.out.println("ENDED");
	}
	
	//@Test
	public void testStreamingAzure() throws Exception {	
		IFileSliceStore.updateServerMapping("testazurestore", "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.STREAM, acct, key, props);
		
		az=new AzurePageBlobSliceStore("testazurestore");
		
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

		System.out.println("WRITE IMG DATA");
		az.store(data, sliceName1, null);
		System.out.println("Store img byte size: " + data.length);
		System.out.println("END WRITE IMG DATA");

		//create a hash for the text data
		SliceDigest smd=new SliceDigest(new SliceDigestInfo(blkSize, hashKey));
		smd.update(txtData.getBytes());
		smd.finalizeDigest();
		String txtDigest=Resources.convertToHex(smd.getSliceChecksum());
		
		System.out.println("WRITE FULL TEXT DATA");
		az.store(txtData.getBytes(), sliceName2, smd.getSliceDigestInfo());
		System.out.println("END WRITE FULL TEXT DATA");
		
		int slice3Len=5;
		System.out.println("WRITE PARTIAL TEXT DATA");
		az.store(txtData.getBytes(), sliceName3, 0, slice3Len, null);
		System.out.println("END WRITE PARTIAL TEXT DATA");

		//RETRIEVING
		System.out.println("GET IMG DATA");
		byte[] r=az.retrieve(sliceName1, 0);
		System.out.println("END GET IMG DATA");
		md.reset();
		if(r==null || r.length==0)
			fail("Unable to retrieve file slice.");
		//need to know the actual length containing data cos azure store data in mutliples of 512 bytes
		md.update(r, 0, data.length);

		String getImgDigest=Resources.convertToHex(md.digest());
		
		System.out.println("Img digest: " + imgDigest);
		System.out.println("Get img digest: " + getImgDigest);
		assertTrue("Image retrieved is not correct", getImgDigest.equals(imgDigest));
		
		System.out.println("GET FULL TEXT DATA");
		r=az.retrieve(sliceName2, blkSize);
		System.out.println("END GET FULL TEXT DATA");
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
		System.arraycopy(r, Resources.HASH_BIN_LEN, btxtData, 0, btxtData.length);
		String getTxtData=new String(btxtData);
		
		System.out.println("Get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData));
		 
		btxtData=new byte[8];
		System.out.println("GET RANDOM TEXT DATA 1");
		len=az.retrieve(sliceName2, 7, 8, 0, btxtData, 0);
		System.out.println("END GET RANDOM TEXT DATA 1");
		System.out.println("Len retrieved: " + len);
		getTxtData=new String(btxtData);
		System.out.println("Random get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData.substring(7, 15)));

		System.out.println("GET RANDOM TEXT DATA 2");
		btxtData=az.retrieve(sliceName2, 12, 0);
		System.out.println("GET RANDOM TEXT DATA 2");
		getTxtData=new String(btxtData,0, 4);
		System.out.println("Random get Text: " + getTxtData);
		assertTrue("Text retrieved is not correct.", getTxtData.equals(txtData.substring(12)));
		
		az.delete(sliceName1);
		az.delete(sliceName2);
		az.delete(sliceName3);
		
		System.out.println("ENDED");
	}

}
