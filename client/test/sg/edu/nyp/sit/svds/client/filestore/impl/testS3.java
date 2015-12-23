package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.util.Properties;

import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

public class testS3 {
	public static void main(String[] args){
		System.out.println(System.getProperty("java.class.path"));
		System.out.println(System.getProperty("java.library.path"));
		
		Properties props=new Properties();
		String keyId = "AKIAIDRIHF67XJXDTWSA";
		String key="XuZea6kk+WnnjLYzO17Zu6kem7TO3A4eSa8/Ogvy";
		props.put(FileSliceServerInfo.S3PropName.CONTAINER.value(), "nypmoeif-ts3f");
		
		AmazonS3Client s3c=new AmazonS3Client(new BasicAWSCredentials(keyId, key));
		s3c.setEndpoint("s3.amazonaws.com");
		if(!s3c.doesBucketExist(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString())){
			//System.out.println("bucket does not exist");
			s3c.createBucket(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString());
		}
		s3c.putObject(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString(), "testf", new java.io.File("D:\\Projects\\CloudComputing\\Development\\DiffusedCloudStorage\\resource\\testimage.jpg"));
		System.out.println("WRITE DONE");
		s3c.deleteObject(props.get(FileSliceServerInfo.S3PropName.CONTAINER.value()).toString(), "testf");
		
		System.out.println("DONE");
	}
}
