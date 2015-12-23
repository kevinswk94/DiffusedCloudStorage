package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class EnumInfo implements Serializable{
	public static final long serialVersionUID = 1L;
	
	public boolean isExactMatch=false;
	public int index=0;
	
	public byte[] toBytes() throws IOException{
		ByteArrayOutputStream bOut=new ByteArrayOutputStream();
		ObjectOutputStream oOut=new ObjectOutputStream(bOut);
		
		oOut.writeObject(this);
		oOut.close();
		
		return bOut.toByteArray();
	}
	
	public static EnumInfo toObject(byte[] data) throws IOException, ClassNotFoundException{
		ByteArrayInputStream bIn=new ByteArrayInputStream(data);
		ObjectInputStream oIn=new ObjectInputStream(bIn);
		
		return (EnumInfo) oIn.readObject();
	}
}