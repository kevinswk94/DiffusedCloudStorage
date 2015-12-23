package sg.edu.nyp.sit.svds.client;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.ChangeModeSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class File {
	public static final long serialVersionUID = 3L;
	
	private static final Log LOG = LogFactory.getLog(File.class);
			
	private IMasterFileTable mt=MasterTableFactory.getFileInstance();;
	
	FileInfo fInfo = null;
	
	//Indicate if the file or directory is new (which mean
	//no data can be retrieved from the metadata store
	boolean isCreated=false;
	
	//An array of FileSlice objects for the actual file
	List<FileSlice> slices = Collections.synchronizedList(new ArrayList<FileSlice>());;
	
	private String absFilePath=null;
	private User currUser=null;

	//filePath refers to the absolute path of the file, 
	//including the namespace, directory(s) if any and file name
	//eg. urn:nyp.edu.sg/secret.doc
	public File(String absFilePath, User user) throws SVDSException{
		if(absFilePath==null || absFilePath.trim().length()==0)
			throw new SVDSException("File path cannot be empty.");
		
		if(user==null)
			throw new SVDSException("User cannot be empty.");
		
		absFilePath=absFilePath.trim();
		this.currUser=user;
		
		if(absFilePath.indexOf(FileInfo.PATH_SEPARATOR)==-1)
			throw new SVDSException("Must specify namespace.");
		
		if(absFilePath.indexOf(FileInfo.PATH_SEPARATOR)!=absFilePath.lastIndexOf(FileInfo.PATH_SEPARATOR)
				&& absFilePath.endsWith(FileInfo.PATH_SEPARATOR))
			throw new SVDSException("File path cannot end with " + FileInfo.PATH_SEPARATOR);
		
		getFileInfo(null, absFilePath);
	}
	
	//directory refers to the a file object pointing to a directory 
	//on the namespace and relFilePathis the extension from that 
	//directory to the file
	public File(File parent, String relFilePath, User user) throws SVDSException{
		if(relFilePath==null || relFilePath.trim().length()==0 || parent==null)
			throw new SVDSException("Parent or file path cannot be empty.");
		
		if(user==null)
			throw new SVDSException("User cannot be empty.");
		
		relFilePath=relFilePath.trim();
		this.currUser=user;
		
		if(parent.isDirectory()==false)
			throw new SVDSException("Parent must be a directory object.");
		
		if(relFilePath.startsWith(FileInfo.PATH_SEPARATOR) || relFilePath.endsWith(FileInfo.PATH_SEPARATOR))
			throw new SVDSException("Relative file path cannot start or end with "+FileInfo.PATH_SEPARATOR+".");
		
		getFileInfo(parent.getFullPath(), relFilePath);
	}
	
	//absDirectoryPath refers to the directory with its namespace 
	//and relFilePathis the extension from that directory to the file
	public File(String absDirectoryPath, String relFilePath, User user) throws SVDSException{
		if(absDirectoryPath==null || absDirectoryPath.trim().length()==0 
				|| relFilePath==null || relFilePath.trim().length() == 0)
			throw new SVDSException("Directory or file path cannot be empty.");
		
		if(user==null)
			throw new SVDSException("User cannot be empty.");
		
		absDirectoryPath=absDirectoryPath.trim();
		relFilePath=relFilePath.trim();
		this.currUser=user;
		
		if(absDirectoryPath.indexOf(FileInfo.PATH_SEPARATOR)==-1)
			throw new SVDSException("Must specify namespace.");
		
		if(absDirectoryPath.indexOf(FileInfo.PATH_SEPARATOR)!=absDirectoryPath.lastIndexOf(FileInfo.PATH_SEPARATOR)
				&& absDirectoryPath.endsWith(FileInfo.PATH_SEPARATOR))
			throw new SVDSException("Directory path cannot end with " + FileInfo.PATH_SEPARATOR);
		
		if(relFilePath.startsWith(FileInfo.PATH_SEPARATOR) || relFilePath.endsWith(FileInfo.PATH_SEPARATOR))
			throw new SVDSException("Relative file path cannot start or end with "+FileInfo.PATH_SEPARATOR+".");
		
		getFileInfo(absDirectoryPath, relFilePath);
	}
	
	private void getFileInfo(String absDirectoryPath, String relFilePath) throws SVDSException{
		int tmpPos=relFilePath.lastIndexOf(FileInfo.PATH_SEPARATOR);
		String name=(tmpPos>=0 ? relFilePath.substring(tmpPos+1) : relFilePath);
		if(name.equals(""))
			name=FileInfo.PATH_SEPARATOR;
		
		String namespace=null, relPath="";
		if(absDirectoryPath!=null && absDirectoryPath.length()>0){
			tmpPos=absDirectoryPath.indexOf(FileInfo.PATH_SEPARATOR);
			namespace=absDirectoryPath.substring(0, tmpPos);
			
			if(tmpPos!=absDirectoryPath.length()-1)
				relPath=absDirectoryPath.substring(tmpPos-1);
			
			relPath+=FileInfo.PATH_SEPARATOR+relFilePath;
		}else{
			tmpPos=relFilePath.indexOf(FileInfo.PATH_SEPARATOR);

			namespace=relFilePath.substring(0, tmpPos);
			
			relPath=relFilePath.substring(tmpPos);
		}
		
		//the master table class will get the file slice info too, and inside
		//each file slice info object, the checksum is retrieved from the metadata repository
		//so that when the file slices are retrieved from the physical servers, they can be
		//check for validity
		fInfo=mt.getFileInfo(namespace, relPath, currUser);
		absFilePath=namespace+relPath;
		fInfo.setLockBy(currUser);
		
		if (fInfo.isEmpty){
			//file does not exist
			isCreated=false;
			fInfo.setType(null);
			fInfo.setFilename(name);
			fInfo.setNamespace(namespace);
			fInfo.setFullPath(relPath);
		}else{
			boolean removeSliceChecksum=false;
			//if checksum feature turn off, reset the info related to checksumming
			if(!ClientProperties.getBool(ClientProperties.PropName.FILE_VERIFY_CHECKSUM)){
				fInfo.setBlkSize(0);
				fInfo.setKeyHash(null);
				removeSliceChecksum=true;
			}

			isCreated=true;
			if(fInfo.getType()==FileInfo.Type.FILE){
				slices.clear();
				
				for(FileSliceInfo fsi : fInfo.getSlices()){
					if(removeSliceChecksum) fsi.setSliceChecksum(null);
					slices.add(new FileSlice(fsi, currUser));
				}
			}
			//set the file size to the ida info obj too
			fInfo.getIda().setDataSize(fInfo.getFileSize());
		}
	}
	
	//methods from FileRecord class
	public Date getCreationDate(){
		if(fInfo==null)
			return null;
		
		return fInfo.getCreationDate();
	}
	
	public Date getLastModifiedDate(){
		if(fInfo==null)
			return null;
		
		return fInfo.getLastModifiedDate();
	}
	
	public Date getLastAccessedDate(){
		if(fInfo==null)
			return null;
		
		return fInfo.getLastAccessedDate();
	}
	
	public String getFilename(){
		if(fInfo==null)
			return null;
		
		return fInfo.getFilename();
	}

	public long getFileSize(){
		if(fInfo==null)
			return -1L;
		
		return fInfo.getFileSize();
	}
	
	public User getOwner(){
		if(fInfo==null)
			return null;
		
		return fInfo.getOwner();
	}
	
	public void setOwner(User owner){
		if(fInfo==null)
			return;
		
		fInfo.setOwner(owner);
	}
	
	public String getNamespace(){
		if(fInfo==null)
			return null;
		
		return fInfo.getNamespace();
	}
	
	public String getFullPath(){
		if(fInfo==null)
			return null;
		
		return fInfo.getFullPath();
	}
	
	public FileInfo getFileInfo(){
		return fInfo;
	}
	
	//Creates a directory entry in the metadatastore
	public void createNewDirectory() throws SVDSException{
		if(fInfo!=null && fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.DIRECTORY)
			throw new SVDSException("Invalid operation, object is not directory type.");
		
		if(exist())
			throw new SVDSException("Directory already exist.");
		
		slices.clear();
		fInfo.setType(FileInfo.Type.DIRECTORY);
		fInfo.setSlices(null);
		fInfo.setOwner(currUser);
		
		mt.addFileInfo(fInfo, currUser);

		isCreated=true;
	}
	
	public void createNewFile() throws SVDSException{		
		if(fInfo!=null && fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.FILE)
			throw new SVDSException("Invalid operation, object is not file type.");
		
		if(exist())
			throw new SVDSException("File already exist.");
		
		//LOG.debug(fInfo.getFullPath());
		
		fInfo.setOwner(currUser);
		
		slices.clear();
		fInfo.setType(FileInfo.Type.FILE);
		fInfo.setSlices(null);
		fInfo.setSlices(new ArrayList<FileSliceInfo>());
		
		if(ClientProperties.getBool(ClientProperties.PropName.FILE_VERIFY_CHECKSUM)){
			//generate a random number to store as the key hash
			//SecureRandom seRand=null;
			//try{
			//	seRand=SecureRandom.getInstance(ClientProperties.getString(ClientProperties.PropName.FILE_RANDOM_ALGO));
			//}catch(Exception ex){
			//	LOG.error(ex);
			//	throw new SVDSException(ex);
			//}
			
			//byte randValue[]=new byte[ClientProperties.getInt(ClientProperties.PropName.FILE_RANDOM_SIZE)];
			//seRand.nextBytes(randValue);
			//fInfo.setKeyHash(Resources.convertToHex(randValue));
			try{
				fInfo.setKeyHash(Resources.generateRandomValue(ClientProperties.getString(ClientProperties.PropName.FILE_RANDOM_ALGO), 
						ClientProperties.getInt(ClientProperties.PropName.FILE_RANDOM_SIZE)));
			}catch(Exception ex){
				LOG.error(ex);
				throw new SVDSException(ex);
			}
			//set the block size to be retrieved from properties file
			//the value may change when user starts to write to the file but will be updated
			//to the master server
			fInfo.setBlkSize(ClientProperties.getInt(ClientProperties.PropName.FILE_SLICE_BLK_SIZE));
		}else{
			fInfo.setBlkSize(0);
			fInfo.setKeyHash(null);
		}
		
		mt.addFileInfo(fInfo, currUser);

		isCreated=true;
	}
	
	//Deletes the associated file slices and its metadata entry of 
	//the file
	public boolean deleteFile() throws SVDSException{
		if(fInfo==null)
			return false;
		
		if(fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.FILE)
			throw new SVDSException("Invalid operation, object is not file type.");

		mt.deleteFileInfo(fInfo, currUser);
		
		for(final FileSlice fs : slices){
			new Thread(
					new Runnable(){
						public void run(){
							fs.delete();
						}
					}
			).start();
		}

		fInfo.setSlices(null);
		fInfo=null;
		slices.clear();
		
		return true;
	}
	
	//Deletes the directory information from metadata store and returns
	//status of operation
	public boolean deleteDirectory() throws SVDSException{
		if(fInfo==null)
			return false;
		
		if(fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.DIRECTORY)
			return false;
		
		/*
		List<String> files=listFilesPath();
		if(files!=null && files.size()>0)
			throw new SVDSException("Unable to delete directory containing files.");
		*/
		
		mt.deleteFileInfo(fInfo, currUser);
		
		fInfo=null;
		slices.clear();
		
		return true;
	}
		
	//To check from the metadata store if the file indicated in the 
	//filePath and fileName exist
	public boolean exist(){
		if(fInfo==null){
			//LOG.debug(absFilePath);
			try{ getFileInfo(null, absFilePath); }
			catch (SVDSException e){
				LOG.error(e);
				return false;
			}
		}

		if (!isCreated)
			return false;
		else		
			return true;
	}
	
	//Retrieves the directory file object that the current file/
	//directory is contained in
	public File getParent(){
		String parent=getParentPath();
		if(parent==null)
			return null;
		
		try{
			File p = null;
			if(parent.equals(FileInfo.PATH_SEPARATOR))
				p=new File(fInfo.getNamespace()+FileInfo.PATH_SEPARATOR, this.currUser);
			else
				p=new File(fInfo.getNamespace(), parent, this.currUser);
			return p;
		}catch(SVDSException e){
			LOG.error(e);
			return null;
		}
	}
	
	//Retrieves the absolute directory path (excluding the namespace) 
	//that the current file/directory is contained in
	public String getParentPath(){
		if(fInfo==null)
			return null;
		
		if(fInfo.getFullPath().indexOf(FileInfo.PATH_SEPARATOR)==0)
			return FileInfo.PATH_SEPARATOR; //the root directory
		
		return fInfo.getFullPath().substring(0, fInfo.getFullPath().lastIndexOf(FileInfo.PATH_SEPARATOR));
	}
	
	//Determines if the current file object is a directory
	public boolean isDirectory(){
		if(fInfo==null)
			return false;
			
		if(fInfo.getType()!=null && fInfo.getType()==FileInfo.Type.DIRECTORY)
			return true;
		else
			return false;
	}
	
	//Determines if the current file object is a file
	public boolean isFile(){
		if(fInfo==null)
			return false;
		
		if(fInfo.getType()!=null && fInfo.getType()==FileInfo.Type.FILE)
			return true;
		else
			return false;
	}
	
	//Retrieves all the files (including directories) contained in the 
	//directory that is contain in the file object, as an array of file 
	//objects. If the file object is not an directory, exception will 
	//be thrown
	public List<File> listFiles() throws SVDSException{
		if(fInfo==null)
			return null;
		
		if(fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.DIRECTORY)
			throw new SVDSException("Invalid operation on non-directory file.");
		
		List<File> files=new ArrayList<File>();
		
		for(String f: listFilesPath()){
			files.add(new File(fInfo.getNamespace()+f, currUser));
		}

		return files;
	}
	
	//Retrieves all the absolute file path (excluding namespace) and names of the files 
	//(including directories) contained in the directory that is contain 
	//in the file object. If the file object is not an directory, 
	//exception will be thrown
	public List<String> listFilesPath() throws SVDSException{
		if(fInfo==null)
			return null;
		
		if(fInfo.getType()!=null && fInfo.getType()!=FileInfo.Type.DIRECTORY)
			throw new SVDSException("Invalid operation on non-directory file.");
		
		return mt.listFiles(fInfo.getNamespace(), fInfo.getFullPath(), currUser);
	}
	
	//rename or move the file or directory
	//the input parameter is the absoulte new path exlcuding the namespace
	//does not support changing namespace because of some slice servers
	//may not support the new namespace
	public boolean moveTo(String new_absPath) throws SVDSException{
		if(new_absPath==null || new_absPath.trim().length()==0)
			throw new SVDSException("Path cannot be empty.");
		
		new_absPath=new_absPath.trim();
		new_absPath=fInfo.getNamespace()+new_absPath;
		
		if(new_absPath.indexOf(FileInfo.PATH_SEPARATOR)==-1)
			throw new SVDSException("Invalid path.");
		
		int pos=new_absPath.indexOf(FileInfo.PATH_SEPARATOR);
		String namespace=new_absPath.substring(0, pos);
		if(namespace.length()==0 || new_absPath.length()==pos+1)
			throw new SVDSException("Invalid path.");
		
		String path=new_absPath.substring(new_absPath.indexOf(FileInfo.PATH_SEPARATOR));

		if(fInfo==null)
			return false;
		
		if(fInfo.getFilename().equals(FileInfo.PATH_SEPARATOR))
			throw new SVDSException("Cannot move or rename root directory.");
		
		mt.moveFileInfo(fInfo, namespace, path, currUser);
				
		fInfo.setFullPath(path);
		fInfo.setNamespace(namespace);
		absFilePath=new_absPath;
		
		return true;
	}
	
	public void changeMode(FileIOMode mode) throws SVDSException{
		if(ClientProperties.getString(ClientProperties.PropName.CLIENT_MODE).equals(ClientProperties.CLIENT_MODE_PVFS)){
			throw new NotSupportedSVDSException("Not supported in PVFS");
		}
		
		if(mode==null || mode==FileIOMode.BOTH || mode==FileIOMode.NONE)
			throw new SVDSException("Invalid mode.");
		
		mt.refreshChangeFileMode(fInfo, currUser);
		
		if(fInfo.isChgMode())
			throw new ChangeModeSVDSException("File is in the midst of mode change.");
		
		for(FileSlice fs: slices){
			if(!fs.isComplete())
				throw new SVDSException("Cannot change mode while file is in recovery mode.");
		}
		
		mt.changeFileMode(fInfo, mode, currUser);
		
		fInfo.setChgMode(mode);
	}
}
