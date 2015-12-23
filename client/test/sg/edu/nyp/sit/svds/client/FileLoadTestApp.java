package sg.edu.nyp.sit.svds.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Hashtable;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class FileLoadTestApp {		
	private static String masterFilePath=null;
	private static String masterConfigPath=null;
	private static String sliceStoreRootPath=null;
	private static String sliceStoreConfigPath=null;
	private String namespace="urn:sit.nyp.edu.sg";
	
	private sg.edu.nyp.sit.svds.filestore.Main fsvr[]=null;
	private sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private static int noOfSliceServers=0;
	private static int noOfClients=0;
	private static String masterURL=null;
	private static int masterPort=9010;
	private static boolean startMaster=false;
	private static boolean fileStreaming=false;
	private static long approxFileSize=0; //in bytes (in mb, x * 1024 *1024)
	
	private static String mode=null; 
	private static String nfsPath=null;
	
	private User user=new User("moeif_usrtest", "p@ssw0rd");
	private String sliceStorePrefix="TESTFS";
	
	//Run properties
	//-clients <no of clients threads to run>
	//-filesize <size of the file to create in bytes>
	//-streaming <yes to test file load in streaming mode or no>
	//-mode <SVDS to run load test using SVDS implementation or NFS to run load test using
	//		direct IO read/write>
	//-nfspath <if running in NFS mode, must specify directory to create the file>
	//-localmasterconfig <absolute path where the config files are located>
	//-master <ip addr & port no of the master server, either this or localmasterport must exist>
	//-slicestores <no of slice store servers to run> (optional, default is 0)
	//-slicestorespath <absolute path where the slices will be created>
	//-slicestoreconfig <absolute path where the config file is located>
	public static void main(String[] args) throws Exception{
		Hashtable<String, String> prop=Resources.transformValues(args);
		
		if(!prop.containsKey("CLIENTS") || !prop.containsKey("FILESIZE") 
				|| !prop.containsKey("MODE"))
			throw new Exception("Clients, file size, streaming and mode parameter must exist.");
		
		mode=prop.get("MODE").toString();
		if(!mode.equalsIgnoreCase("SVDS") && !mode.equalsIgnoreCase("NFS"))
			throw new Exception("Invalid mode. Accepts SVDS or NFS");
		if(mode.equalsIgnoreCase("NFS")){
			if(!prop.containsKey("NFSPATH"))
				throw new Exception("Path must be supplied in NFS mode.");
			else
				nfsPath=prop.get("NFSPATH").toString();
		}
		
		approxFileSize=Long.parseLong(prop.get("FILESIZE").toString());
		noOfClients=Integer.parseInt(prop.get("CLIENTS").toString());
		
		if(mode.equalsIgnoreCase("SVDS")){
			if(!prop.containsKey("STREAMING"))
				throw new Exception("Streaming parameter must exist.");
			
			fileStreaming=prop.get("STREAMING").equalsIgnoreCase("NO")?false:true;
			
			if(prop.containsKey("MASTER") && prop.containsKey("LOCALMASTERCONFIG"))
				throw new Exception("Only one master parameter to exist.");
	
			if(prop.containsKey("LOCALMASTERCONFIG")){
				masterConfigPath=prop.get("LOCALMASTERCONFIG").toString();
				startMaster=true;
			}else if(prop.containsKey("MASTER")){
				masterURL=prop.get("MASTER").toString();
				startMaster=false;
		
				masterPort=Integer.parseInt(masterURL.substring(masterURL.indexOf(":")+1));
			}
				
			if(prop.containsKey("SLICESTORES")){
				if(!prop.containsKey("SLICESTORESPATH") || !prop.containsKey("SLICESTORECONFIG")
						|| (!prop.containsKey("MASTER") && !prop.containsKey("LOCALMASTERCONFIG")))
					throw new Exception("Local slice store server and/or config path must be provided.");
	
				sliceStoreRootPath=prop.get("SLICESTORESPATH").toString();
				sliceStoreConfigPath=prop.get("SLICESTORECONFIG").toString();
	
				noOfSliceServers=Integer.parseInt(prop.get("SLICESTORES").toString());
				if(noOfSliceServers<=0)
					throw new Exception("Min slice store server is 1.");
			}
		}

		System.out.println("Starting in 2 seconds...");
		try{Thread.sleep(2*1000);}catch(InterruptedException ex){}
		
		System.out.println("-----START-----");
		
		Date timeStart=new Date();
		
		FileLoadTestApp ta=null;
		try{
			ta=new FileLoadTestApp();
			ta.setup();
			if(mode.equalsIgnoreCase("SVDS")) 
				ta.SVDSLoadTest();
			else
				ta.NFSLoadTest();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(ta!=null)
				try{ta.shutdown();}catch(Exception ex){ex.printStackTrace();}
		}
		
		Date timeEnd=new Date();
		
		System.out.println("\nTotal time taken: " + ((timeEnd.getTime()-timeStart.getTime())/1000) + " seconds.");
		
		System.out.println("-----END-----");
		
		//shut down the pool
		SVDSStreamingPool.shutdown();
	}
	
	private class NFSFileClient extends Thread{
		private String path=null;
		private long writeTime=0, readTime=0;
		
		public NFSFileClient(String path){
			this.path=path;
		}
		
		public void run(){
			try{
				java.io.File testFile=new java.io.File(path);
				if(testFile.exists())
					testFile.delete();
				testFile.createNewFile();
				
				writeTime=writeLoadTest(testFile);
				readTime=readLoadTest(testFile);
				
				testFile.delete();
				testFile=null;
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		private long readLoadTest(java.io.File testFile) throws Exception{
			Date timeStart=new Date();
			
			FileInputStream in=new FileInputStream(testFile);
			
			byte[] data=new byte[Resources.DEF_BUFFER_SIZE];
			while(in.read(data)!=-1){}
			data=null;
			
			in.close();
			
			Date timeEnd=new Date();
			
			return (timeEnd.getTime()-timeStart.getTime());
		}

		private long writeLoadTest(java.io.File testFile) throws Exception{
			Date timeStart=new Date();
			byte[] data=new Long(timeStart.getTime()).toString().getBytes();
			int iterCnt=(int)(approxFileSize/data.length);
			
			FileOutputStream out=new FileOutputStream(testFile);
			for(int i=0; i<iterCnt; i++){
				out.write(data);
			}
			
			out.close();
			
			Date timeEnd=new Date();
			
			out=null;

			return (timeEnd.getTime()-timeStart.getTime());
		}
	}
	
	private class SVDSFileClient extends Thread{
		private String path=null;
		private long writeTime=0, readTime=0;
		//private String writeChecksum=null, readChecksum=null;
		
		public SVDSFileClient(String path){
			this.path=path;
		}
		
		public void run(){
			try{
				sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(path, user);
				if(testFile.exist())
					testFile.deleteFile();
				testFile.createNewFile();
				
				writeTime=writeLoadTest(testFile);
				
				/*
				System.out.println("Start reading in 10 seconds...");
				Thread.sleep(10*1000);
				System.out.println("Start read");
				*/
				
				readTime=readLoadTest(testFile);
				
				/*
				if(writeChecksum.equals(readChecksum))
					System.out.println("Checksum match.");
				else
					System.out.println("Checksum does not match");
				*/
				//testFile.deleteFile();
				testFile=null;
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		private long readLoadTest(sg.edu.nyp.sit.svds.client.File testFile) throws Exception{
			Date timeStart=new Date();
			
			SVDSInputStream in=new SVDSInputStream(testFile, fileStreaming);
			
			//MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
			
			byte[] tmp=new byte[Resources.DEF_BUFFER_SIZE];
			int tmpLen;
			while((tmpLen=in.read(tmp))!=-1){
			//	md.update(tmp, 0, tmpLen);
			}
			
			in.close();
			//readChecksum=Resources.convertToHex(md.digest());
			//md=null;
			
			Date timeEnd=new Date();

			in=null;
			
			return (timeEnd.getTime()-timeStart.getTime());
		}
		
		private long writeLoadTest(sg.edu.nyp.sit.svds.client.File testFile) throws Exception{
			Date timeStart=new Date();
			byte[] data=new Long(timeStart.getTime()).toString().getBytes();
			int iterCnt=(int)(approxFileSize/data.length);
			
			//MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
			
			//write big file
			SVDSOutputStream out=new SVDSOutputStream(testFile, fileStreaming);
			
			for(int i=0; i<iterCnt; i++){
				out.write(data);
			//	md.update(data);
			}
			
			out.close();
			//writeChecksum=Resources.convertToHex(md.digest());
			//md=null;
			
			Date timeEnd=new Date();
			
			out=null;

			return (timeEnd.getTime()-timeStart.getTime());
		}
	}
	
	public void NFSLoadTest() throws Exception{
		NFSFileClient[] clients=new NFSFileClient[noOfClients];
		
		//start all the clients in parallel
		for(int i=0; i<noOfClients; i++){
			clients[i]=new NFSFileClient(nfsPath+"/loadTestFile"+i);
			clients[i].start();
		}
		
		//wait for all clients to finish
		for(int i=0; i<noOfClients; i++)
			clients[i].join();
		
		//display the results
		for(int i=0; i<noOfClients; i++)
			System.out.println("Thread " + i + " "+(fileStreaming?"":"NON-")+"STREAMING (file approx "
				+ ((float)(approxFileSize)/(float)(1024*1024)) 
				+ "MB): write=" + clients[i].writeTime + "ms. read="+clients[i].readTime+"ms.");
	}
	
	public void SVDSLoadTest() throws Exception{
		SVDSFileClient[] clients=new SVDSFileClient[noOfClients];
		
		//start all the clients in parallel
		for(int i=0; i<noOfClients; i++){
			clients[i]=new SVDSFileClient(namespace+FileInfo.PATH_SEPARATOR+"loadTestFile"+i);
			clients[i].start();
		}
		
		//wait for all clients to finish
		for(int i=0; i<noOfClients; i++)
			clients[i].join();
		
		//display the results
		for(int i=0; i<noOfClients; i++)
			System.out.println("Thread " + i + " "+(fileStreaming?"":"NON-")+"STREAMING (file approx "
				+ ((float)(approxFileSize)/(float)(1024*1024)) 
				+ "MB): write=" + clients[i].writeTime + "ms. read="+clients[i].readTime+"ms.");
	}
	
	public void setup() throws Exception{
		if(startMaster){
			msvr=new sg.edu.nyp.sit.svds.master.Main( masterConfigPath+"/IDAProp.properties",
					masterConfigPath+"/MasterConfig.properties");
			
			masterPort=MasterProperties.getInt("master.namespace.port");
			masterURL=(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("off")? "localhost":
				MasterProperties.getString("master.namespace.ssl.address"))+":"+masterPort;
			
			masterFilePath=MasterProperties.getString("master.directory");
			deleteMasterFiles();
			
			msvr.startupMain();
		}
		
		//create files for slice store and delete them when done
		if(noOfSliceServers>0){
			deleteFileStoreFiles();
			fsvr=new sg.edu.nyp.sit.svds.filestore.Main[noOfSliceServers];
			File f=null;
			for(int i=0; i<noOfSliceServers; i++){
				f=new File(sliceStoreRootPath+"/"+sliceStorePrefix+i);
				f.mkdir();
				
				fsvr[i]=new sg.edu.nyp.sit.svds.filestore.Main(sliceStoreConfigPath);
				SliceStoreProperties.set("master.address", masterURL);
				SliceStoreProperties.set("slicestore.namespace", namespace);
				fsvr[i].startup(sliceStorePrefix+i, (8010+i), (7010+i), 
						((startMaster && SliceStoreProperties.getString("slicestore.status.ssl").equalsIgnoreCase("off")) 
						? "localhost" : InetAddress.getLocalHost().getHostName()), 
						sliceStoreRootPath+"/"+sliceStorePrefix+i, 0);
			}
		}
	}
	
	public void shutdown() throws Exception{
		Thread.sleep(10*1000);
		
		if(fsvr!=null){
			for(int i=0; i<noOfSliceServers; i++){
				if(fsvr[i]!=null)
					fsvr[i].shutdown();
					fsvr[i]=null;
			}
		
			deleteFileStoreFiles();
		}
		
		if(msvr!=null){
			msvr.shutdown();
			deleteMasterFiles();
		}
	}
	
	private void deleteMasterFiles(){
		File f=null;
		
		if(startMaster){
			f=new File(masterFilePath+"/svds.img");
			if(f.exists())
				f.delete();
			f=null;
			
			f=new File(masterFilePath+"/svdsTrans.log");
			if(f.exists())
				f.delete();
			f=null;
			
			f=new File(masterFilePath+"/namespaceTrans.log");
			if(f.exists())
				f.delete();
			f=null;
		}	
	}
	
	private void deleteFileStoreFiles(){
		File f=null;
		
		for(int i=0; i<noOfSliceServers; i++){
			f=new File(sliceStoreRootPath+"/"+sliceStorePrefix+i);
			if(f.exists()){
				for(File s: f.listFiles())
					s.delete();
				
				f.delete();
			}
			f=null;
		}
	}
}
