package sg.edu.nyp.sit.pvfs.virtualdisk.dokan.sample;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;

import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.client.SVDSInputStream;
import sg.edu.nyp.sit.svds.client.SVDSOutputStream;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class VirtualFile {
	public static final long serialVersionUID = 1L;
	
	private static final String OWNER_NAME = "owner_vdisk";
	private static String namespace="urn:sit.nyp.edu.sg";
	String masterFilePath=System.getProperty("user.dir");
	String basePath =System.getProperty("user.dir");
	User user = new User("owner_vdisk", "");
	
	public static void main(String[] args)
	{
		try {
			
			String directoryPath = namespace+FileInfo.PATH_SEPARATOR+OWNER_NAME;
			String FileName = directoryPath + FileInfo.PATH_SEPARATOR + "Testing123.txt";
			VirtualFile vf = new VirtualFile();
			
			//vf.createNewDirectory(directoryPath);
			
			vf.createNewFile(FileName, "Testing123Testing123Testing123Testing123Testing123Testing123", false);
			//vf.readFile(FileName, "", 0 ,false);

			vf.listDirectory(directoryPath, 100);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
			
	
	public VirtualFile()
	{
//		try {
//			sg.edu.nyp.sit.svds.master.Main msvr=new sg.edu.nyp.sit.svds.master.Main();
//			msvr.startupMain(masterFilePath, 9010);
			
//			sg.edu.nyp.sit.svds.filestore.Main fsvr=new sg.edu.nyp.sit.svds.filestore.Main();
//			fsvr.startup("TESTFS1", 8010, "localhost:9010", basePath+"/filestore/storage", namespace);
//		}
//		catch(Exception ex) {
//			ex.printStackTrace();
//		}
	}
	
	public void createNewDirectory(String directoryPath) throws Exception{
		(new sg.edu.nyp.sit.svds.client.File(directoryPath, user)).createNewDirectory();

		sg.edu.nyp.sit.svds.client.File testFolder=new sg.edu.nyp.sit.svds.client.File(directoryPath, user);
		if(!testFolder.exist())
			System.out.println("Folder is not created.");
		if(!testFolder.isDirectory())
			System.out.println("Path is not a directory.");
	}

	public String createNewFile(String filePath, String data, boolean streaming) throws Exception{
		
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, user);

		testFile.createNewFile();
		
		Field f=testFile.getClass().getDeclaredField("fInfo");
		f.setAccessible(true);
		//int quorum=((FileInfo)f.get(testFile)).getIda().getQuorum();
		
		SVDSOutputStream out=new SVDSOutputStream(testFile, streaming);
		out.write(data.getBytes());
		out.close();
		
		
		return "";
	}

	public void listDirectory(String directoryPath, int noOfRecords) throws Exception{
		
		System.out.println("listDirectory:" + directoryPath);
		File testFolder= new File(directoryPath, user);
		if(!testFolder.exist())
			System.out.println("Path does not exist.");
		if(!testFolder.isDirectory())
			System.out.println("Path is not a directory.");
		
		List<String> records=testFolder.listFilesPath();

		System.out.println("Records found: " + records.size());
		System.out.println("No to compare: " + noOfRecords);
		for(String n: records)
			System.out.println(n);
		
	}
	
	@SuppressWarnings("unused")
	private void readFile(String filePath, String data, long offset, boolean streaming) throws Exception{
		
		System.out.println("filePath:" + filePath);
		sg.edu.nyp.sit.svds.client.File testFile=new sg.edu.nyp.sit.svds.client.File(filePath, user);
		
		if(!testFile.exist())
			System.out.println("File is not created.");
		
		if(testFile.isDirectory())
			return;
		
		SVDSInputStream in=new SVDSInputStream(testFile, streaming);
		in.seek(offset);
		
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		int b;
		while((b=in.read())!=-1){
			out.write(b);
		}
		in.close();
		
		String retrievedData=new String(out.toByteArray());
		
		System.out.println(retrievedData);
		
		in=null;
		testFile=null;
		out=null;
	}

}
