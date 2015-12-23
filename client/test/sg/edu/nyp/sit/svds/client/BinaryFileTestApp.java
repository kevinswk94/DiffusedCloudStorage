package sg.edu.nyp.sit.svds.client;

import java.io.*;
import java.util.*;

import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.InfoDispersalFactory;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

public class BinaryFileTestApp {
	private static int[][] m_nKeyMatrix = {
	        {1,        1,      1},
	        {1,        2,      4},
	        {1,        3,      9},
	        {1,        4,      16},
	        {1,        5,      25}
		};
	
	public static void main(String args[]) throws Exception{
		String rootPath="D:\\Projects\\CloudComputing\\Development\\DiffusedCloudStorage\\resource\\";
				
		java.io.File ori_file=new java.io.File(rootPath+"testimage.jpg");
		int qurom=3;
		
		IdaInfo idaI=new IdaInfo(5,qurom,m_nKeyMatrix);
		idaI.setDataSize(ori_file.length());
		
		IInfoDispersal ida=InfoDispersalFactory.getInstance();
		
		List<InputStream> ins=ida.split(new FileInputStream(ori_file), idaI);
		
		List<java.io.File> slices=new ArrayList<java.io.File>();
		
		byte[] data=new byte[512];
		int len;
		
		//write to slices
		for(int i=0; i<ins.size(); i++){
			java.io.File s=new java.io.File(rootPath+"s"+i);
			if(s.exists())
				s.delete();
			
			s.createNewFile();
			
			FileOutputStream out=new FileOutputStream(s);
			InputStream in=ins.get(i);

			while((len=in.read(data))!=-1){
				out.write(data, 0, len);
			}
			out.flush();
			out.close();
			in.close();
			out=null;
			in=null;
			
			slices.add(s);
		}
		
		//read from slices, 1st 3
		List<InputStream> sin=new ArrayList<InputStream>();
		for(int i=0; i<qurom; i++){
			sin.add(new SequenceInputStream(
					new ByteArrayInputStream((new byte[]{(byte)i}))
					, new FileInputStream(slices.get(i))));
		}
		
		InputStream in = ida.combine(sin, idaI);
		
		java.io.File new_file=new java.io.File(rootPath+"combineImage.jpg");
		if(new_file.exists())
			new_file.delete();
		
		new_file.createNewFile();
		
		FileOutputStream out=new FileOutputStream(new_file);
		while((len=in.read(data))!=-1){
			out.write(data, 0, len);
		}
		out.flush();
		out.close();
		in.close();
		out=null;
		in=null;
		
		System.out.println("DONE");
	}
}
