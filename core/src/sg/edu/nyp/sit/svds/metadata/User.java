package sg.edu.nyp.sit.svds.metadata;

/**
 * Metadata about the user accessing a file or making a request to the master application
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class User {
	public static final long serialVersionUID = 2L;
	
	private String id=null;
	private String pwd=null;
	private String name=null;
	private String email=null;
	
	/**
	 * Creates a user object using the ID
	 * 
	 * @param id ID of the user
	 */
	public User(String id){
		this.id=id;
	}
	
	/**
	 * Creates a user object using the ID and password
	 * 
	 * @param id ID of the user
	 * @param pwd Password of the user account
	 */
	public User(String id, String pwd){
		this.id=id;
		this.pwd=pwd;
	}
	
	/**
	 * Gets the ID of the user
	 * 
	 * @return ID of the user
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * Sets the password of the user account
	 * 
	 * @param pwd Password of the user account
	 */
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	/**
	 * Gets the password of the user account
	 * 
	 * @return Password of the user account
	 */
	public String getPwd() {
		return pwd;
	}
	
	public void setName(String name){
		this.name=name;
	}
	
	public String getName(){
		return name;
	}
	
	public void setEmail(String email){
		this.email=email;
	}
	
	public String getEmail(){
		return email;
	}
}
