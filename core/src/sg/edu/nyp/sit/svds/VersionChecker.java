package sg.edu.nyp.sit.svds;

import java.lang.reflect.Field;
import java.util.Scanner;

/**
 * Application to check the version of a certain class. The class must have a variable named serialVersionUID.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class VersionChecker {
	@SuppressWarnings("rawtypes")
	public static void main(String[] args){
		String clsName=null;
		Class cls;
		Field f;
		
		Scanner in=new Scanner(System.in);
		
		do{
			System.out.print("Enter fully qualified class name (q to quit): ");
			clsName=in.nextLine().trim();
		
			if(clsName.length()>0 && !clsName.equalsIgnoreCase("q")){
				try{
					cls=Class.forName(clsName);
					f=cls.getField("serialVersionUID");
					System.out.println("The version for the class " +clsName + " is " + f.getLong(null));
				}catch(Exception ex){
					System.out.println("Error accessing version of specfied class. It may not exist.");
				}
			}
		}while(!clsName.equalsIgnoreCase("q"));
	}
}
