package sg.edu.nyp.sit.pvfs.svc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import javax.bluetooth.BluetoothStateException;
import javax.bluetooth.RemoteDevice;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.Status;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;

/**
 * Class that runs as a thread to act as an finite state machine maintaining the bluetooth connection between the PC and mobile application.
 * Also acts as the initial contact point for request/reponse between the PC and mobile application.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public class PVFSSvc extends Thread {
	public static final long serialVersionUID = 4L;
	
	private static final Log LOG = LogFactory.getLog(PVFSSvc.class);
	
	private StreamConnection conn;
	private final RemoteDevice device;
	private DataInputStream in;
	private DataOutputStream out;
	
	//temporarily stores the authentication password to the mobile application so that user does not
	//need to type in the password during reconnection
	private String logonPwd=null;
	
	//only 1 thread, including the svr thread can access the input stream at any one time
	private Semaphore inLock=new Semaphore(0, true);
	//map to store the threads waiting for results
	HashMap<String, Semaphore> req=new HashMap<String, Semaphore>();
	
	private volatile int masterStatus=0;
	private volatile boolean isShutdown=false;
	
	//value that identifies the mobile that is connected to the PC
	private String C2DMRegId=null;
	
	/**
	 * Creates a PVFS service object using the bluetooth connection (already established) and the connecting bluetooth device (mobile)
	 *  
	 * @param c Bluetooth connection that is already established
	 * @param d onnecting bluetooth device (mobile)
	 */
	public PVFSSvc(StreamConnection c, RemoteDevice d){	
		this.conn=c;
		this.device=d;

		try{
			in=conn.openDataInputStream();
			out=conn.openDataOutputStream();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	//----------Finite State Machine Definition----------------------------------------------------------------------
	//initial state of the FSM
	private PVFSState currState=PVFSState.READY;
	
	//specify the possible transition from one state to another
	private static HashMap<PVFSState, EnumSet<PVFSState>> stateMap;
	static{
		stateMap=new HashMap<PVFSState, EnumSet<PVFSState>>();
		
		stateMap.put(PVFSState.READY, 
				EnumSet.of(PVFSState.READY, PVFSState.AUTH, PVFSState.RECONNECTING));
		
		stateMap.put(PVFSState.AUTH, EnumSet.of(PVFSState.AUTH, PVFSState.READY));
		
		stateMap.put(PVFSState.RECONNECTING, EnumSet.of(PVFSState.AUTH, PVFSState.READY));
	}

	private void setCurrentState(PVFSState desiredState){
		if(!stateMap.get(currState).contains(desiredState))
			throw new IllegalArgumentException("Invalid state.");
		
		currState=desiredState;
	}
	
	private void unhandledStateEvent(PVFSEvents event){
		LOG.error("Unhandled state event error. Event: " + event.toString() + ", State: " + currState.toString());
		shutdown(false);
	}
	
	/**
	 * Starts the finite state machine.
	 * This method should not be called directly by calling class.
	 * 
	 * @see java.lang.Thread#start()
	 */
	public void run(){
		if(in==null || out==null)
			return;

		try {
			//inform the mobile that this is a new connection
			out.writeUTF(PVFSEvents.NEW_CONN.toString());
			out.flush();
		} catch (IOException e) {
			LOG.error("Start IO Error");
			shutdown(true);
		}
		
		String event, reqId;
		C2DMThread ct=null;
		while(true && !isShutdown){
			try{
				event=in.readUTF();
				//System.out.println(tmp);
				
				if(event.equals(PVFSEvents.OP_FILE_REP.toString()) || event.equals(PVFSEvents.OP_NAMESPACE_REP.toString())){
					if(currState!=PVFSState.READY){
						unhandledStateEvent(PVFSEvents.get(event));
						break;
					}
					
					reqId=in.readUTF();
					LOG.debug("svc: " + event + " " + reqId);
					if(!req.containsKey(reqId)){
						LOG.debug("No need response");
						//cannot find any request waiting for response, so waste the input
						in.readInt();
						in.readUTF();
						continue;
					}
					
					//notify the thread waiting for reply and let the thread read the status code
					//from input stream
					req.get(reqId).release();
					req.remove(reqId);
				}else if(event.equals(PVFSEvents.AUTH_REQ.toString())){
					if(currState==PVFSState.READY){
						if (!getUserPassword())
							break;
					}else if(currState==PVFSState.RECONNECTING){
						synchronized(out){
							//sends last entered password
							out.writeUTF(PVFSEvents.AUTH_REP.toString());
							out.writeUTF(logonPwd);
							out.flush();
						}
					}else{
						unhandledStateEvent(PVFSEvents.AUTH_REQ);
						break;
					}
					
					setCurrentState(PVFSState.AUTH);
					
					continue;
				}else if(event.equals(PVFSEvents.AUTH_RETRY.toString())){
					if(currState!=PVFSState.AUTH){
						unhandledStateEvent(PVFSEvents.get(event));
						break;
					}
					
					Main.showError("Incorrect password. Please try again.");
					if (!getUserPassword())
						break;
					
					continue;
				}else if(event.equals(PVFSEvents.AUTH_IVD.toString())){
					if(currState==PVFSState.AUTH){
						Main.showError("Incorrect password. Connection will be closed.");
						shutdown(true);
					}else
						unhandledStateEvent(PVFSEvents.AUTH_IVD);
					
					break;
				}else if(event.equals(PVFSEvents.AUTH_OK.toString())){
					setCurrentState(PVFSState.READY);
					
					continue;
				}else if(event.equals(PVFSEvents.INIT_READY.toString())){
					if(currState==PVFSState.AUTH){
						unhandledStateEvent(PVFSEvents.INIT_READY);
						break;
					}
					
					if(C2DMRegId==null){
						if(ct!=null) ct.toStop=true;
						ct=new C2DMThread();
						ct.start();
					}
					
					if(masterStatus==0){
						LOG.debug("init master table");
						MasterTableFactory.initBluetoothFileInstance(this);
						MasterTableFactory.initBluetoothNamespaceInstance(this);
						//inform whichever object waiting on this that master init table has been done
						masterStatus=1;
					}

					setCurrentState(PVFSState.READY);
					isReconnectedReady=1;
					
					continue;
				}else if(event.equals(PVFSEvents.C2DM_REG_ID_REP.toString())){
					handleC2DMRegistration(in.readInt(), ct);
					
					continue;
				}else if(event.equals(PVFSEvents.PING.toString())){
					continue;
				}else if(event.equals(PVFSEvents.SHUTDOWN_REQ.toString())){
					shutdown(true);
					break;
				}else if(event.equals(PVFSEvents.NEW_CONN.toString())){
					if(currState==PVFSState.RECONNECTING){
						LOG.debug("receive new connection command, reconnect FAIL");
						isReconnectedReady=-1;
						shutdown(false);
					}else if(currState==PVFSState.AUTH){
						unhandledStateEvent(PVFSEvents.NEW_CONN);
						break;
					}
					
					setCurrentState(PVFSState.READY);
					
					continue;
				}else{
					unhandledStateEvent(PVFSEvents.get(event));
					break;
				}
				
				//acquires a permit from this semaphore, blocking until one is available.
				//means can only proceed when the task has finish reading all required inputs
				inLock.acquireUninterruptibly();
			}catch(IOException ex){
				//this is the STREAM_ERR event received
				LOG.debug("svc thread get IO error");
				ex.printStackTrace();
				
				flagIOError();
				if(!reconnect()) break;
			}catch(Exception ex){
				ex.printStackTrace();
				shutdown(false);
				break;
			}
		}
		
		Main.showTrayError("Connection with device is lost. Drive is unmounted.");
		
		//unmount the virutal drive when connection is closed or reconnect fail
		Main.driveUnmounted();
	}
	//----------END Finite State Machine Definition----------------------------------------------------------------------

	//----------Use by the bluetooth master implementation/UI------------------------------------------------------------ 
	/**
	 * Gets the bluetooth connection input stream
	 * 
	 * @return Bluetooth connection input stream
	 */
	public DataInputStream getInputStream(){
		return in;
	}
	/**
	 * Releases the lock on the input stream so that the input stream can be used by other threads to read responses from the mobile application
	 * It is important to release the lock when one thread has finish reading the input so as not to block other threads from waiting forever.
	 */
	public void releaseInputStream(){
		inLock.release();
	}
	/**
	 * Gets the bluetooth connection output stream.
	 * As a general guideline, threads who want to write responses should obtain the monitor on the output stream before writing.
	 * 
	 * Eg. synchronized (out){ ... }
	 * 
	 * @return Bluetooth connection output stream
	 */
	public DataOutputStream getOutputStream(){
		return out;
	}
	/**
	 * Informs the PVFS service that a response is required from the mobile application. 
	 * The PVFS service will add the request ID to a list and checks the list everytime a response is sent from the mobile application.
	 * This method is called after a request is send.
	 * 
	 * @param reqId Request ID to wait for the response
	 * @return Semaphore that the calling method can use to block itself until the response is received
	 */
	public Semaphore waitForResults(String reqId){
		Semaphore s=new Semaphore(0, true);
		LOG.debug("register reply");
		req.put(reqId, s);
		return s;
	}
	/**
	 * Informs the PVFS service that the response is no longer required for the request.
	 * The PVFS service will remove the request ID from the list.
	 * 
	 * @param reqId Request ID to remove
	 */
	public void removeFrmResultsList(String reqId){
		req.remove(reqId);
	}
	/**
	 * Gets the bluetooth address of the mobile running the application
	 * 
	 * @return Bluetooth address of the mobile running the application
	 */
	public String getConnectingDevice(){
		return device.getBluetoothAddress();
	}
	/**
	 * Updates own copy of the authentication password with the mobile application
	 * 
	 * @param pwd Password to update
	 */
	public void updateLoginPwd(String pwd){
		logonPwd=pwd;
	}
	/**
	 * Gets the status of the PC bluetooth master implementation
	 * 
	 * @return 0 - if PC bluetooth master implementation is pending.
	 * 			1 - if PC bluetooth master implementation is ready.
	 * 			-1 - if PVFS service is shutting down.
	 */
	public int getMasterStatus(){
		return masterStatus;
	}
	/**
	 * Gets the password to authenticate to the mobile application from the user
	 * 
	 * @return True if the user enters a password, false otherwise
	 * @throws IOException Occurs when the password cannot be send over to the mobile application via bluetooth
	 */
	private boolean getUserPassword() throws IOException{
		logonPwd=Main.getDirectConnectPassword("Connect to [" + device.getBluetoothAddress() + "]");
		if(logonPwd==null){
			shutdown(false);
			return false;
		}
		
		synchronized(out){
			//TODO: may need to encrypt before sending cos socket may not be secure
			out.writeUTF(PVFSEvents.AUTH_REP.toString());
			out.writeUTF(logonPwd);
			out.flush();
		}
		
		return true;
	}
	//----------END Use by the bluetooth master implementation/UI------------------------------------------------------------ 
	
	//----------C2DM------------------------------------------------------------ 
	/**
	 * Request the mobile application to get a registration ID to uniquely identify the mobile device from the C2DM server
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	private class C2DMThread extends Thread{
		private final int waitInterval=5;
		public boolean toStop=false;
		
		/**
		 * Periodically sends a request to the mobile application to get the registration ID. 
		 * It only stops when a reply is received or the PVFS service is shutdown.
		 * Not to be called directly, use the start method of the inherited Thread class.
		 * 
		 * @see java.lang.Thread#start()
		 */
		public void run(){
			while(!isShutdown && !toStop){
				try{
					synchronized(out){
						out.writeUTF(PVFSEvents.C2DM_REG_ID_REQ.toString());
						out.flush();
					}
				}catch(IOException ex){
					ex.printStackTrace();
				}
				
				try { sleep(1000 * waitInterval); } catch (InterruptedException e) {e.printStackTrace();}
			}
		}
	}
	
	/**
	 * Process the reply for the C2DM registration ID request
	 * 
	 * @param status Status of the request
	 * @param ct The thread object; so that the thread can be stopped
	 * @throws IOException Occurs when there is error reading from the bluetooth input stream
	 */
	public void handleC2DMRegistration(int status, C2DMThread ct) throws IOException{
		LOG.debug("C2DM reg status:"+status);
		switch(status){
			case Status.INFO_OK:
				C2DMRegId=in.readUTF();
				
				LOG.debug("Received C2DM reg id: " + C2DMRegId);
				
				ct.toStop=true;
				break;
			case Status.ERR_C2DM_ACCOUNT_MISSING:
				Main.showTrayMsg("There is no Google account registered on the phone. Please setup the account and re-mount the drive.");
				//if the master tables impl has not been setup yet, then shutdown, 
				//which will also tell the device connected class not to mount the virtual drive
				if(masterStatus==0) shutdown(false);
				
				break;
			case Status.ERR_C2DM_AUTHENTICATION_FAILED:
				Main.showTrayMsg("Password is invalid for registered google account on the phone. Please enter the correct password and re-mount the drive.");
				//if the master tables impl has not been setup yet, then shutdown, 
				//which will also tell the device connected class not to mount the virtual drive
				if(masterStatus==0) shutdown(false);
				
				break;
			case Status.ERR_C2DM_INVALID_SENDER:
				Main.showTrayMsg("The account registered with the application is incorrect. Please contact the support team.");
				//if the master tables impl has not been setup yet, then shutdown, 
				//which will also tell the device connected class not to mount the virtual drive
				if(masterStatus==0) shutdown(false);
				
				ct.toStop=true;
				break;
			case Status.ERR_C2DM_PHONE_REGISTRATION_ERROR:
				Main.showTrayMsg("Current phone does not support the application.");
				//if the master tables impl has not been setup yet, then shutdown, 
				//which will also tell the device connected class not to mount the virtual drive
				if(masterStatus==0) shutdown(false);
				
				ct.toStop=true;
				break;
			case Status.ERR_C2DM_TOO_MANY_REGISTRATIONS:
				Main.showTrayMsg("There are too many applications on the phone. Please uninstall some applications and re-mount the drive.");
				//if the master tables impl has not been setup yet, then shutdown, 
				//which will also tell the device connected class not to mount the virtual drive
				if(masterStatus==0) shutdown(false);
				
				break;
		}
	}
	/**
	 * Sends the remaining metadata over to the mobile application in the form of a C2DM message.
	 * This happens when the connection has been terminated and the files metadata has not been completed updated.
	 * Look at the C2DM documentation on the limits of a C2DM message
	 * 
	 * @param data Remaining file metadata
	 * @throws Exception Occurs when there is error sending the metadata
	 */
	public void sendMetadata(String data) throws Exception{
		C2DMAgent.sendMsg(C2DMRegId, data);
	}
	//----------END C2DM------------------------------------------------------------
	
	/**
	 * Shutdown the PVFS service
	 * 
	 * @param immed If the service should be shut down immediately. If no, a shutdown request will be send to the mobile application before shutting down.
	 * If yes, shut down immediately. This happens when the mobile application is the one that initate the shutdown
	 */
	public void shutdown(boolean immed){
		isShutdown=true;
		masterStatus=-1;
		logonPwd=null;
		
		try{
			//immed will be true is it's the phone that init the shutdown so no need to send the req
			if(!immed){
				synchronized(out){
					out.writeUTF(PVFSEvents.SHUTDOWN_REQ.toString());
					out.flush();
				}
				//after write, wait a while for the command to be send out
				try { Thread.sleep(1000*2); } catch (InterruptedException e) {}
			}
			
			in.close();
			out.close();
			conn.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	/**
	 * To test if the streams (input/output) is working by sending a ping request (will be ignored by the mobile) over.
	 * This is used by the handleIOError method in the master implementation to check if its local input/output variable
	 * should be refreshed instead of doing a reconnection.
	 * 
	 * @return True if the request can be send over to the mobile application, false otherwise 
	 */
	public boolean toRefreshStreams(){
		try{
			synchronized(out){
				out.writeUTF(PVFSEvents.PING.toString());
				out.flush();
			}
			
			return true;
		}catch(IOException ex){
			return false;	
		}
	}
	
	//----------Reconnection--------------------------------------------------------------------------------
	/**
	 * To indicate that an IO error has occurred and the reconnection has not started
	 */
	public void flagIOError(){
		hasReconnected=false;
	}

	private boolean hasReconnected=false;
	private volatile int isReconnectedReady=0;
	
	private static final long TIMEOUT_INTERVAL=1000*60*10;
	private Object reconnect=new Object();
	
	/**
	 * Check if reconnection has succeeded and is ready to proceed (authentication is already done)
	 * 
	 * @return 0 -  if reconnection is in the process.
	 * 			1 - if reconnection has succeeded and is ready to proceed.
	 * 			-1 - reconnection failed.
	 */
	public int isReconnectedReady(){
		return isReconnectedReady;
	}
	
	/**
	 * Attempt to re-establish the bluetooth connection with the mobile device.
	 * 
	 * @return True if the reconnection succeeds, false otherwise
	 */
	public boolean reconnect(){
		if(isShutdown) return false;

		LOG.debug("reconnect invoked");
		synchronized(reconnect){
			//if user has attempt to disconnect or previously has reconnected already, exit	
			if(hasReconnected) return true;
			
			setCurrentState(PVFSState.RECONNECTING);
			isReconnectedReady=0;
			
			//To cater for scenarios where one thread invoke flagIOError just after another thread has just successfully reconnected
			//and set the hasReconnected to true but have not exit the method (rarely happens but still have to cater). 
			//This means the connection has already been established so have to close off and re-establish again
			try{
				in.close();
				out.close();
				conn.close();
			}catch(Exception ex){ }
			
			ReconnectThread r=new ReconnectThread();
			r.start();
			
			//wait for it to reconnect for certain time
			try { r.join(TIMEOUT_INTERVAL); } catch (InterruptedException ex) { ex.printStackTrace(); }
			
			if(r.isAlive()) { r.interrupt(); }
			
			LOG.debug("reconnect timeout or ended");
			
			if(!(hasReconnected=r.isReconnected())){
				//if there is error reconnecting, shut down the service
				LOG.debug("reconnect fail");
				shutdown(true);
			}
			
			try{
				//send to mobile that this is a reconnection
				out.writeUTF(PVFSEvents.RE_CONN.toString());
				out.flush();
			}catch(IOException ex){
				shutdown(true);
				hasReconnected= false;
			}
			
			System.out.println("reconnect status: "+hasReconnected);
			
			return hasReconnected;
		}
	}
	
	/**
	 * Actual class that attempt the reconnection with the mobile device.
	 * 
	 * @author Victoria Chin
	 * @version %I% %G%
	 */
	private class ReconnectThread extends Thread{
		private boolean isReconnected=false;
		public boolean isReconnected(){ return isReconnected; }
		private static final int FIND_DEVICE_INTERVAL=1000*1;
		
		public void run(){
			LOG.debug("reconnect thread starting");
			BTAgent a;
			try { a=new BTAgent(); } catch (BluetoothStateException ex) { ex.printStackTrace(); return;}
			
			String connUrl=null;
			//if the svc is shutdown, this thread should stop as well
			while(!this.isInterrupted() && !isShutdown){
				connUrl=a.findDeviceService(device);
				
				if(connUrl==null) {
					LOG.debug("unable to find connect url");
					//wait for a interval before attempting to reconnect
					try { Thread.sleep(FIND_DEVICE_INTERVAL); } catch (InterruptedException e) { }
					continue;
				}
				
				try {
					conn=(StreamConnection) Connector.open(connUrl);
					
					LOG.debug("open connection");
					
					in=conn.openDataInputStream();
					out=conn.openDataOutputStream();
					
					isReconnected=true;
					break;
				} catch (IOException ex) {
					ex.printStackTrace();
					continue;
				}
			}
			
			LOG.debug("reconnect thread end");
		}
	}
	//----------END Reconnection--------------------------------------------------------------------------------
}
