package sg.edu.nyp.sit.svds.client.filestore;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import sg.edu.nyp.sit.svds.client.ClientProperties;
import sg.edu.nyp.sit.svds.client.filestore.FileSliceStoreFactory;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.filestore.impl.*;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class FileSliceStoreFactoryTest {

	@Test
	public void testGetInstance() throws Exception {
		ClientProperties.init();
		ClientProperties.load(new java.io.File(System.getProperty("user.dir")+"/resource/svdsclient_unitTest.properties"));
		
		IFileSliceStore store=null;
		
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "off");

		IFileSliceStore.updateServerMapping("trestletfs", "localhost:8010", 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null, null, null);
		store = FileSliceStoreFactory.getInstance("trestletfs");
		IFileSliceStore.removeServerMapping("trestletfs");
		assertTrue(store instanceof RestletSliceStore);
		
		IFileSliceStore.updateServerMapping("tazurefs", "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.STREAM, "moeifazuretest",
				"IOpp4021LWmjaBJrIoHzxrUvHp2r0GxK7OG6Mot99jXEtcNFGOOdHwuND48BOl8hJEw/OH7wKD+XoLgNun6Okw==", null);
		store = FileSliceStoreFactory.getInstance("tazurefs");
		IFileSliceStore.removeServerMapping("tazurefs");
		assertTrue(store instanceof AzurePageBlobSliceStore || store instanceof AzureBlockBlobSliceStore);
		
		Properties propsS3=new Properties();
		propsS3.put(FileSliceServerInfo.S3PropName.CONTAINER.value(), "nypmoeif-ts3f");
		IFileSliceStore.updateServerMapping("ts3fs", "US_Standard", 
				FileSliceServerInfo.Type.S3, FileIOMode.NON_STREAM, "AKIAIDRIHF67XJXDTWSA",
				"XuZea6kk+WnnjLYzO17Zu6kem7TO3A4eSa8/Ogvy", propsS3);
		store = FileSliceStoreFactory.getInstance("ts3fs");
		IFileSliceStore.removeServerMapping("ts3fs");
		assertTrue(store instanceof S3SliceStore);
		
		
		ClientProperties.set(ClientProperties.PropName.SLICESTORE_USE_SHARED_ACCESS, "on");
		
		IFileSliceStore.updateServerMapping("trestletfs", "localhost:8010", 
				FileSliceServerInfo.Type.RESTLET, FileIOMode.STREAM, null, null, null);
		store = FileSliceStoreFactory.getInstance("trestletfs");
		IFileSliceStore.removeServerMapping("trestletfs");
		assertTrue(store instanceof RestletSharedAccessSliceStore);
		
		IFileSliceStore.updateServerMapping("tazurefs", "http://blob.core.windows.net/", 
				FileSliceServerInfo.Type.AZURE, FileIOMode.STREAM, null, null, null);
		store = FileSliceStoreFactory.getInstance("tazurefs");
		IFileSliceStore.removeServerMapping("tazurefs");
		assertTrue(store instanceof AzureSharedAccessPageBlobSliceStore || store instanceof AzureSharedAccessBlockBlobSliceStore);
		
		propsS3.put(FileSliceServerInfo.S3PropName.CONTAINER.value(), "nypmoeif-ts3f");
		IFileSliceStore.updateServerMapping("ts3fs", "US_Standard", 
				FileSliceServerInfo.Type.S3, FileIOMode.NON_STREAM, null, null, propsS3);
		store = FileSliceStoreFactory.getInstance("ts3fs");
		IFileSliceStore.removeServerMapping("ts3fs");
		assertTrue(store instanceof S3SharedAccessSliceStore);	
		
	}

}
