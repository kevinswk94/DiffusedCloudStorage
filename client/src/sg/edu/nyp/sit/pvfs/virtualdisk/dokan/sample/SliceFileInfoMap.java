package sg.edu.nyp.sit.pvfs.virtualdisk.dokan.sample;

import java.util.concurrent.ConcurrentHashMap;


public class SliceFileInfoMap extends ConcurrentHashMap<String, SliceFileInfo> {
	public static final long serialVersionUID = 1L;
	
	public SliceFileInfo create(String fileName, boolean isDirectory)
	{
		SliceFileInfo sfi = get(fileName);
		if(sfi == null)
		{
			sfi = new SliceFileInfo(fileName, isDirectory);
			sfi.createNewFile();
			this.putIfAbsent(fileName, sfi);
		}
		
		return sfi;
	}
}
