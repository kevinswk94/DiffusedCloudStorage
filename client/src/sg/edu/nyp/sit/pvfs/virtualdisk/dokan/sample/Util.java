package sg.edu.nyp.sit.pvfs.virtualdisk.dokan.sample;

import sg.edu.nyp.sit.svds.metadata.FileInfo;


public class Util {
	public static final long serialVersionUID = 1L;
	
	public static String getFileName(String fullPath)
	{
		int n = fullPath.lastIndexOf("/");
		if(n == -1) {
			n = fullPath.lastIndexOf("\\");
			if(n == -1)
				return fullPath;
		}
		return fullPath.substring(n+1);
	}
	
	/**
	 * Converts a file name to a file path for SVDS server
	 * @param filename (eg abc.txt)
	 * @return namespace + separator + owner + filename
	 */
	public static String toSvdsName(String namespace, String owner, String filename)
	{
		//Prefix namespace
		if(filename.equals("\\") || filename.equals("/"))
			return namespace + FileInfo.PATH_SEPARATOR;
		
		if(filename.startsWith("\\") || filename.startsWith("/"))
			filename = filename.substring(1);
		//return namespace + FileInfo.PATH_SEPARATOR + FileInfo.PATH_SEPARATOR +filename;
		return namespace + FileInfo.PATH_SEPARATOR +filename;
	}
	
	public static String fromSvdsName(String svdsName)
	{
		int n = svdsName.lastIndexOf("/");
		if(n == -1) {
			n = svdsName.lastIndexOf("\\");
			if(n == -1)
				return svdsName;
		}
		return svdsName.substring(n+1);
	}

}
