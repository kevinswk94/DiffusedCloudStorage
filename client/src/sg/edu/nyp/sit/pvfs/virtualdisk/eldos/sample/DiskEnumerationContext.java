package sg.edu.nyp.sit.pvfs.virtualdisk.eldos.sample;

import java.util.ArrayList;
import java.util.List;

public class DiskEnumerationContext {
	private List<VirtualFile> files=null;
	private int index=0;
	
	public DiskEnumerationContext(){
		files=new ArrayList<VirtualFile>();
	}
	
	public VirtualFile getNextFile(){
		if(index>=files.size())
			return null;
		
		return files.get(index);
	}
	
	public VirtualFile getFile(String name){
		for(VirtualFile f: files){
			if(f.getName().equals(name))
				return f;
		}
		
		return null;
	}
	
	public VirtualFile getFile(int index){
		if(index>=files.size())
			return null;
		
		return files.get(index);
	}

	public void addFile(VirtualFile f){
		if(files.contains(f))
			return;
		
		f.setContext(this);
		files.add(f);
		resetEnumeration();
	}
	
	public void removeFile(VirtualFile f){
		if(!files.contains(f))
			return;
		
		files.remove(f);
		resetEnumeration();
	}
	
	public boolean isEmpty(){
		return files.size()==0;
	}
	
	public void resetEnumeration(){
		index=0;
	}
}
