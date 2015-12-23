package sg.edu.nyp.sit.svds.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.Date;
import java.util.Hashtable;
import java.util.Scanner;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.filestore.SliceStoreProperties;
import sg.edu.nyp.sit.svds.master.MasterProperties;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

@SuppressWarnings("unused")
public class FileProfileTestApp {
	private static String masterFilePath=null;
	private static String masterConfigPath=null;
	private static String sliceStoreRootPath=null;
	private static String sliceStoreConfigPath=null;
	private String namespace="urn:sit.nyp.edu.sg";
	
	private sg.edu.nyp.sit.svds.filestore.Main fsvr[]=null;
	private sg.edu.nyp.sit.svds.master.Main msvr=null;
	
	private static int noOfSliceServers=0;
	private static String masterURL=null;
	private static int masterPort=9010;
	private static boolean startMaster=false;
	private static boolean fileStreaming=false;
	private static long approxFileSize=0; //in bytes (in mb, x * 1024 *1024)
	
	private static String mode=null; 
	private static String nfsPath=null;
	private static String op=null;
	
	private User user=new User("moeif_usrtest", "p@ssw0rd");
	private String sliceStorePrefix="TESTFS";
	
	//Run properties
	//-op <READ to run the read operation for profiling, the file must exist or WRITE to run the write operation for profiling>
	//-run <no of times to run the operation, if not provided, def is 1>
	//-filesize <size of the file to create in bytes for write op>
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
		
		if(!prop.containsKey("OP") || !prop.containsKey("MODE"))
			throw new Exception("Operation, streaming and mode parameter must exist.");
		
		mode=prop.get("MODE").toString();
		if(!mode.equalsIgnoreCase("SVDS") && !mode.equalsIgnoreCase("NFS"))
			throw new Exception("Invalid mode. Accepts SVDS or NFS");
		if(mode.equalsIgnoreCase("NFS")){
			if(!prop.containsKey("NFSPATH"))
				throw new Exception("Path must be supplied in NFS mode.");
			else
				nfsPath=prop.get("NFSPATH").toString();
		}

		op=prop.get("OP").toString();
		
		if(op.equalsIgnoreCase("WRITE")){
			if(!prop.containsKey("FILESIZE"))
				throw new Exception("Missing filesize parameter for write operation.");
			else
				approxFileSize=Long.parseLong(prop.get("FILESIZE").toString());
		}
		
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
		
		int tries=(!prop.containsKey("RUN")? 1 : Integer.parseInt(prop.get("RUN").toString()));
		
		System.out.println("-----START-----");
		
		FileProfileTestApp pa=null;
		try{
			pa=new FileProfileTestApp();
			pa.setup();
			
			System.out.println("Setup complete: Profile can be started now.\nContinue [y|n]?.");
			Scanner s = new Scanner(System.in);
			String input=s.nextLine();
			while (input.isEmpty() || (!input.equalsIgnoreCase("Y") && 
					!input.equalsIgnoreCase("N"))){
				System.out.println("Invalid input. Continue [y|n]?");
				input=s.nextLine();
			}
			
			if(input.equalsIgnoreCase("Y")){
				if(mode.equalsIgnoreCase("SVDS")) 
					pa.SVDSLoadTest(tries);
				else
					pa.NFSLoadTest(tries);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(pa!=null)
				try{pa.shutdown();}catch(Exception ex){ex.printStackTrace();}
		}
		
		System.out.println("-----END-----");
	}
	
	public void NFSLoadTest(int tries) throws Exception{
		String filePath=nfsPath+"/loadTestFile";
		java.io.File testFile=new java.io.File(filePath);		
		
		long timeTaken=0;
		
		if(op.equalsIgnoreCase("READ")){
			if(!testFile.exists())
				throw new Exception("File " + filePath + " does not exist.");
			
			for(int i=0; i<tries; i++ ) {
				//System.out.println("Start run " + i);
				timeTaken+=NFSReadTest(testFile);
			}
		}else if(op.equalsIgnoreCase("WRITE")){
			for(int i=0; i<tries; i++ ) {
				//System.out.println("Start run " + i);
				timeTaken+=NFSWriteTest(testFile);
			}
		}
		
		System.out.println("Avg time taken to complete " + op + " operation using NFS in " 
				+ (fileStreaming?"":"NON-") + "STREAMING mode for "+tries+" time(s) on approx " 
				+ ((float)(approxFileSize)/(float)(1024*1024))
				+ " MB file size = " + (timeTaken/tries) + "ms.");
	}
	
	private long NFSReadTest(java.io.File testFile) throws Exception{
		if(testFile.exists())
			testFile.delete();
		
		testFile.createNewFile();
		
		Date timeStart=new Date();
		
		FileInputStream in=new FileInputStream(testFile);
		
		byte[] data=new byte[Resources.DEF_BUFFER_SIZE];
		while(in.read(data)!=-1){}
		data=null;
		
		in.close();
		
		Date timeEnd=new Date();
		
		return (timeEnd.getTime()-timeStart.getTime());
	}
	
	private long NFSWriteTest(java.io.File testFile) throws Exception{
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
	
	public void SVDSLoadTest(int tries) throws Exception{
		String filePath=namespace+FileInfo.PATH_SEPARATOR+"loadTestFile";
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, user);
		
		long timeTaken=0;
		
		if(op.equalsIgnoreCase("READ")){
			if(!testFile.exist())
				throw new Exception("File " + filePath + " does not exist.");
			
			approxFileSize=testFile.getFileSize();
			
			for(int i=0; i<tries; i++ ) {
				//System.out.println("Start run " + i);
				timeTaken+=SVDSReadTest(testFile);
			}
		}else if(op.equalsIgnoreCase("WRITE")){
			for(int i=0; i<tries; i++ ) {
				//System.out.println("Start run " + i);
				timeTaken+=SVDSWriteTest(testFile);
			}
		}
		
		SVDSStreamingPool.shutdown();
		
		System.out.println("Avg time taken to complete " + op + " operation using SVDS in " 
				+ (fileStreaming?"":"NON-") + "STREAMING mode for "+tries+" time(s) on approx " 
				+ ((float)(approxFileSize)/(float)(1024*1024))
				+ " MB file size = " + (timeTaken/tries) + "ms.");
	}
	
	private long SVDSWriteTest(sg.edu.nyp.sit.svds.client.File testFile) throws Exception{
		if(testFile.exist())
			testFile.deleteFile();
		
		testFile.createNewFile();
		
		Date timeStart=new Date();
		byte[] data=new Long(timeStart.getTime()).toString().getBytes();
		int iterCnt=(int)(approxFileSize/data.length);
		
		//MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		
		//write big file
		SVDSOutputStream out=new SVDSOutputStream(testFile, fileStreaming);
		
		//long totalLen=0;
		for(int i=0; i<iterCnt; i++){
			out.write(data);
			//md.update(data);
			//totalLen+=data.length;
		}
		
		out.close();
		
		//System.out.println("Total len written: " + totalLen + ", Digest: " + Resources.convertToHex(md.digest()));
		//md=null;
		
		Date timeEnd=new Date();
		
		out=null;

		return (timeEnd.getTime()-timeStart.getTime());
	}
	
	private long SVDSReadTest(sg.edu.nyp.sit.svds.client.File testFile) throws Exception{
		Date timeStart=new Date();
		
		//MessageDigest md=MessageDigest.getInstance(Resources.HASH_ALGO);
		
		SVDSInputStream in=new SVDSInputStream(testFile, fileStreaming);
		
		byte[] tmp=new byte[512];
		int len;
		//long totalLen=0;
		while((len=in.read(tmp))!=-1){
			//md.update(tmp, 0, len);
			//totalLen+=len;
		}
		
		in.close();
		tmp=null;
		
		//System.out.println("Total len read: " + totalLen + ", Digest: " + Resources.convertToHex(md.digest()));
		//md=null;
		
		Date timeEnd=new Date();

		in=null;
		
		return (timeEnd.getTime()-timeStart.getTime());
	}
	
	public void setup() throws Exception{
		if(startMaster){
			msvr=new sg.edu.nyp.sit.svds.master.Main( masterConfigPath+"/IDAProp.properties",
					masterConfigPath+"/MasterConfig.properties");
			
			masterPort=MasterProperties.getInt("master.namespace.port");
			masterURL=(MasterProperties.getString("master.namespace.ssl").equalsIgnoreCase("off")? "localhost":
				MasterProperties.getString("master.namespace.ssl.address"))+":"+masterPort;
			
			masterFilePath=MasterProperties.getString("master.directory");
			//deleteMasterFiles();
			
			msvr.startupMain();
		}
		
		//create files for slice store and delete them when done
		if(noOfSliceServers>0){
			//deleteFileStoreFiles();
			fsvr=new sg.edu.nyp.sit.svds.filestore.Main[noOfSliceServers];
			File f=null;
			for(int i=0; i<noOfSliceServers; i++){
				f=new File(sliceStoreRootPath+"/"+sliceStorePrefix+i);
				if(!f.exists()) f.mkdir();
				
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
		if(fsvr!=null || msvr!=null) Thread.sleep(10*1000);
		
		if(fsvr!=null){
			for(int i=0; i<noOfSliceServers; i++){
				if(fsvr[i]!=null)
					fsvr[i].shutdown();
					fsvr[i]=null;
			}
		
			//deleteFileStoreFiles();
		}
		
		if(msvr!=null){
			msvr.shutdown();
			//deleteMasterFiles();
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
