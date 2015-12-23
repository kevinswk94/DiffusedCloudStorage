package sg.edu.nyp.sit.svds;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.security.SecureRandom;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.regex.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility class containing various static helper methods.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class Resources {
	public static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(Resources.class);
	
	/**
	 * Hashing algorithm used in checksum of file.
	 */
	public static final String HASH_ALGO="SHA";
	/**
	 * The length of bytes for the hashed value.
	 */
	public static final int HASH_BIN_LEN=20; //in bytes
	
	/**
	 * Name of the trust store system property. 
	 */
	public static final String TRUST_STORE="javax.net.ssl.trustStore";
	/**
	 * Name of the trust store password system property.
	 */
	public static final String TRUST_STORE_PWD="javax.net.ssl.trustStorePassword";
	/**
	 * Name of the trust store type system property.
	 */
	public static final String TRUST_STORE_TYPE="javax.net.ssl.trustStoreType";
	/**
	 * Name of the key store system property.
	 */
	public static final String KEY_STORE="javax.net.ssl.keyStore";
	/**
	 * Name of the key store password system property.
	 */
	public static final String KEY_STORE_PWD="javax.net.ssl.keyStorePassword";
	/**
	 * Name of the key password system property.
	 */
	public static final String KEY_PWD="javax.net.ssl.keyPassword";
	/**
	 * Name of the key store type system property.
	 */
	public static final String KEY_STORE_TYPE="javax.net.ssl.keyStoreType";
	
	/**
	 * Default size for the byte array to be used as buffer for IO.
	 */
	public static final int DEF_BUFFER_SIZE=4096; //def 4096
	
	private static final String HEXES = "0123456789ABCDEF";
	
	private static Pattern pEncode=Pattern.compile("=|\\:|#|\\!");
	
	/**
	 * Converts a given byte array to hexidecimal string.
	 * 
	 * @param value Given byte array.
	 * @return String containing the value in hexidecimal format.
	 */
	public static String convertToHex(byte[] value){
		if(value==null)
			return null;
		
		final StringBuilder hex = new StringBuilder( 2 * value.length );
		for ( final byte b : value ) {
			hex.append(HEXES.charAt((b & 0xF0) >> 4))
			.append(HEXES.charAt((b & 0x0F)));
		}
		return hex.toString();

		//BigInteger bi = new BigInteger(value);
		//return bi.toString(16);            
		//if (s.length() % 2 != 0) {
		    // Pad with 0
		//    s = "0"+s;
		//}
		//return s;
	}
	
	/**
	 * Converts a given byte array to hexidecimal string.
	 * 
	 * @param value Given byte array.
	 * @param offset Offset in the byte array to start the conversion.
	 * @param len Length from the offset in the byte array to convert.
	 * @return
	 */
	public static String convertToHex(byte[] value, int offset, int len){
		if(value==null)
			return null;
		
		final StringBuilder hex = new StringBuilder( 2 * value.length );
		for(int i=offset; i<offset+len; i++){
			hex.append(HEXES.charAt((value[i] & 0xF0) >> 4))
			.append(HEXES.charAt((value[i] & 0x0F)));
		}
		/*
		for ( final byte b : value ) {
			hex.append(HEXES.charAt((b & 0xF0) >> 4))
			.append(HEXES.charAt((b & 0x0F)));
		}
		*/
		return hex.toString();

		//BigInteger bi = new BigInteger(value);
		//return bi.toString(16);            
		//if (s.length() % 2 != 0) {
		    // Pad with 0
		//    s = "0"+s;
		//}
		//return s;
	}
	
	public static String generateRandomValue(String algo, int size) throws Exception{
		SecureRandom seRand=SecureRandom.getInstance(algo);
		byte randValue[]=new byte[size];
		seRand.nextBytes(randValue);
		return convertToHex(randValue);
	}
	
	/**
	 * Encodes the given value to be used as the value in the name-value pair properties.
	 * 
	 * @param value
	 * @return Encoded value.
	 */
	public static String encodeKeyValue(Object value){
		if(value==null)
			return null;

		StringBuilder str=new StringBuilder(value.toString());
		Matcher m = pEncode.matcher(str); 
		
		int lastMatchPos=0;
		while(m.find(lastMatchPos)){
			lastMatchPos=m.start();
			str.insert(lastMatchPos, "\\");
			//Increment the next find to start at after the added escape char and the match char
			lastMatchPos+=2;
		}
		m=null;
		
		return str.toString();
		
		//String tmp=value.toString();
		//if(tmp.length()==0)
		//	return tmp;
		
		//if(tmp.indexOf("=")==-1 && tmp.indexOf(":")==-1)
		//	return tmp;
		
		//prefix '=', ':' with '\' to escape
		//tmp=tmp.replaceAll("=", "\\=");
		//tmp=tmp.replaceAll(":", "\\:");
		//tmp=tmp.replaceAll("#", "\\#");
		//tmp=tmp.replaceAll("!", "\\!");
		//return tmp;
		
	}
	
	/*
	public static String storeKeyValue(Object value){
		if(value==null)
			return null;

		StringBuilder str=new StringBuilder(value.toString());
		Matcher m = pEncode.matcher(str); 
		
		int lastMatchPos=0;
		while(m.find(lastMatchPos)){
			lastMatchPos=m.start();
			str.insert(lastMatchPos, "\\\\");
			//Increment the next find to start at after the added escape char X2 and the match char
			lastMatchPos+=3;
		}
		m=null;
		
		return str.toString();
		
		//String tmp=value.toString();
		//if(tmp.length()==0)
		//	return tmp;
		
		//if(tmp.indexOf("=")==-1 && tmp.indexOf(":")==-1)
		//	return tmp;
		
		//prefix '=', ':' with '\' to escape
		//tmp=tmp.replaceAll("=", "\\\\=");
		//tmp=tmp.replaceAll(":", "\\\\:");
		//tmp=tmp.replaceAll("#", "\\\\#");
		//tmp=tmp.replaceAll("!", "\\\\!");
		//return tmp;
	}
	*/
	
	/**
	 * To transform given string array into a Hashtable object for easy processing by the application.
	 * 
	 * Given string array must be in the following format:
	 * str[0] - ID
	 * str[1] - 5
	 * str[2] - TYPE
	 * str[3] - non-stream
	 * ...
	 * 
	 * @param args String array containing key in even numbered index and value in odd numbered index.
	 * @return Hashtable object containing key value pairs or null if given string array is null or empty or does not contain even number of elements.
	 */
	public static Hashtable<String, String> transformValues(String[] args){
		Hashtable<String, String> prop=new Hashtable<String, String>();
		
		if(args==null || args.length==0 || args.length%2!=0)
			return null;
		
		for(int i=0; i<args.length; i=i+2) {
			//LOG.debug("found args["+i+"]="+args[i] + ",args["+(i+1)+ "]=" + args[i+1]);
			prop.put(args[i].substring(1).toUpperCase(), args[i+1]);
		}
		return prop;
	}
	
	/**
	 * Writes the given properties object to a given output stream. The value of each name-value pair will be encoded.
	 * 
	 * @param p Given properties object.
	 * @param out Given output stream; can be a file output stream etc. 
	 * @throws IOException
	 */
	public static void storeProperties(Properties p, OutputStream out) throws IOException{
		if(p==null || p.size()==0)
			return;
		
		for(@SuppressWarnings("rawtypes") Map.Entry k: p.entrySet()){
			out.write((k.getKey().toString()+"="+encodeKeyValue(k.getValue().toString())+"\n"
					).getBytes());
		}
		
		out.flush();
	}
	
	/**
	 * Check if the given file name/path exist in the class path.
	 * 
	 * @param fileName Relative or absolute path to the file.
	 * @return Absolute path to the file.
	 * @throws IOException If the given file name/path cannot be found.
	 */
	public static String findFile(String fileName) throws IOException{
		java.io.File f=new java.io.File(fileName);
		
		if(!f.exists()){
			URL p=Resources.class.getClassLoader().getResource(fileName);
			if(p==null)
				throw new IOException(fileName + " not found.");
			else
				return p.getPath();
		}else
			return f.getAbsolutePath();
	}
	
	/**
	 * Find the largest possible common multiple (given the value that it cannot exceed) of a given number.
	 * 
	 * Most often used in determining the buffer size and/or block size for IO.
	 * 
	 * @param num The value to find the multiple.
	 * @param limit The return value shall not exceed this given value.
	 * @return Largest common multiple of num.
	 */
	public static long findCommonMultiple(long num, long limit){
		return ((limit/num)*num);
	}
	
	/**
	 * Find the largest possible common multiple (given the value that it cannot exceed) of 2 given numbers.
	 * 
	 * @param num1 The value to find the multiple.
	 * @param num2 The value to find the multiple.
	 * @param limit The return value shall not exceed this given value.
	 * @return Largest common multiple of num1 and num2.
	 */
	public static long findCommonMultiple(long num1, long num2, long limit){
		long cm=lcm(num1, num2);
		
		return ((limit/cm)*cm);
	}
	
	private static long gcd(long a, long b) {      
	    if (b==0)
	        return a;
	    else
	        return gcd(b, a % b);
	}
	
	private static long lcm(long a, long b) {
	    return (a / gcd(a, b)) * b;
	}
	
	/**
	 * Debugging method to print out the value of the byte array to standard out.
	 * 
	 * @param a Byte array containing values to be printed.
	 */
	public static void printByteArray(byte[] a){
		System.out.print(concatByteArray(a, 0, a.length)+"\n");
	}
	
	/**
	 * Debugging method to print out the value of the byte array from given offset to standard out.
	 * 
	 * @param a Byte array containing values to be printed.
	 * @param offset Offset in the byte array to start.
	 * @param len Length in the byte array from the offset to be printed.
	 */
	public static void printByteArray(byte[] a, int offset, int len){
		System.out.print(concatByteArray(a, offset, len)+"\n");
	}
	
	/**
	 * Debugging method to concatenate the values in a byte array into a string separated by space.
	 * 
	 * @param a Byte array containing values to be concatenated.
	 * @return A string with the byte values separated by space.
	 */
	public static String concatByteArray(byte[] a){
		return concatByteArray(a, 0, a.length);
	}
	
	/**
	 * Debugging method to concatenate the values in a byte array from given offset to offset+len into a string separated by space.
	 * 
	 * @param a Byte array containing values to be concatenated.
	 * @param offset Offset in the byte array to start.
	 * @param len Length in the byte array from the offset to be concatenated.
	 * @return A string with the byte values separated by space.
	 */
	public static String concatByteArray(byte[] a, int offset, int len){
		StringBuilder str=new StringBuilder();
		for(int i=offset; i<offset+len; i++)
			str.append((int)a[i]+" ");
		return str.toString();
	}
}
