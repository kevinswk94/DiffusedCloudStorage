package sg.edu.nyp.sit.svds.client.master.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.Status;
import sg.edu.nyp.sit.pvfs.svc.PVFSEvents;
import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.exception.LockedSVDSException;
import sg.edu.nyp.sit.svds.exception.NotFoundSVDSException;
import sg.edu.nyp.sit.svds.exception.NotSupportedSVDSException;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.NamespaceInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class BluetoothMasterNamespaceTable implements IMasterNamespaceTable  {
	public static final long serialVersionUID = 5L;
	
	private static final Log LOG = LogFactory.getLog(BluetoothMasterNamespaceTable.class);
	
	private PVFSSvc svc;
	private DataOutputStream out;
	private DataInputStream in;
	
	private boolean rejectRequest=false;
	
	public BluetoothMasterNamespaceTable(Runnable svc){
		this.svc=(PVFSSvc)svc;
		this.out=this.svc.getOutputStream();
		this.in=this.svc.getInputStream();
	}
	
	private boolean handleIOError(){
		//because when the bluetooth connection fail,reconnect can be invoked by the svc first
		//then this object would not know the input output stream has been changed and using the
		//old one will get IO error, so call the svc method to check if this object input output
		//stream should just be refreshed instead of doing a reconnection
		if(svc.toRefreshStreams()){
			//refresh the input and output stream
			out=svc.getOutputStream();
			in=svc.getInputStream();
			return true;
		}
		
		svc.flagIOError();
		if(!svc.reconnect()) {
			rejectRequest=true;
			
			return false;
		}
		
		//when reconnect succeeded, it only means the bluetooth connection has been established,
		//so still need to wait until auth (if any) is settle before can continue
		while(svc.isReconnectedReady()==0){
			Thread.yield();
		}
		if(svc.isReconnectedReady()==-1){
			return false;
		}
		
		//refresh the input and output stream
		out=svc.getOutputStream();
		in=svc.getInputStream();
		
		return true;
	}
	
	private void getResponse(String reqId){
		svc.waitForResults(reqId).acquireUninterruptibly();
	}
	
	@Override
	public NamespaceInfo getNamespaceMemory(String namespace, User usr)
			throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getNamespaceMemory_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("MEM");
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				long memAva=in.readLong();
				long memUsed=in.readLong();
				
				return new NamespaceInfo(namespace, memAva, memUsed);
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public void refreshRestletSliceServerKey(String svrId, User usr)
			throws SVDSException {
		//no need to implement this method because the authentication for stand alone 
		//slice store is not required in PVFS
		throw new NotSupportedSVDSException("Method not implemented in bluetooth version.");
	}

	@Override
	public String getNamespace(User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getNamespace_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GET_NS");
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				return in.readUTF();
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public void updateNamespace(String namespace, User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="updateNamespace_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UPD_NS");
					out.writeUTF(namespace);
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public long getReconTimeout(User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getReconTimeout_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GET_RECONN_TIMEOUT");
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				return in.readLong();
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public void updateReconTimeout(long interval, User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="updateReconTimeout_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UPD_RECONN_TIMEOUT");
					out.writeLong(interval);
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public boolean isAuthReq(User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="isAuthReq_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GET_REQ_AUTH");
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				return in.readBoolean();
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public void updateAuthReq(boolean req, String newPwd, User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="updateAuthReq_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UPD_REQ_AUTH");
					
					out.writeBoolean(req);
					if(newPwd!=null){
						out.writeBoolean(true);
						out.writeUTF(newPwd);
					}else
						out.writeBoolean(false);
					
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public List<FileSliceServerInfo> getAvailableSliceServers(String namespace, User usr)
			throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getAvailableSliceServers_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("AVAILABLE");
					out.flush();
				}
				
				//LOG.debug("request send");
				break;
			}catch(IOException ex){
				LOG.debug("Got IO exception");
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
			//LOG.debug("retry sending request");
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				//read response
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				int noOfServers=in.readInt();
				if(noOfServers==-1){
					LOG.debug("No servers found");
					return null;
				}
				
				List<FileSliceServerInfo> stores=new ArrayList<FileSliceServerInfo>();
				FileSliceServerInfo fssi;
				for(int i=0; i<noOfServers; i++){
					//values must be read in order
					fssi=new FileSliceServerInfo(in.readUTF());
					fssi.setStatus(FileSliceServerInfo.Status.valueOf(in.readInt()));
					
					stores.add(fssi);
				}
				
				return stores;
			}catch(IOException ex){
				//LOG.debug("Got IO exception");
				
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
			
			//LOG.debug("retry waiting response");
		}while(true);
	}

	@Override
	public void removeSliceServer(String namespace, String svrId, User usr) throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="removeSliceServer_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("REMOVE");
					out.flush();
					
					out.writeUTF(svrId);
					out.flush();
				}
				
				//LOG.debug("request send");
				break;
			}catch(IOException ex){
				LOG.debug("Got IO exception");
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
			//LOG.debug("retry sending request");
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					case Status.ERR_CONFLICT:
						throw new LockedSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				//LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public FileSliceServerInfo getSliceServer(String svrId, User usr)
			throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getSliceServer_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GET");
					out.flush();
					
					out.writeUTF(svrId);
					out.flush();
				}
				
				//LOG.debug("request send");
				break;
			}catch(IOException ex){
				LOG.debug("Got IO exception");
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
			//LOG.debug("retry sending request");
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				//read response
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					case Status.ERR_NOT_FOUND:
						throw new NotFoundSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				FileSliceServerInfo fssi=new FileSliceServerInfo(svrId);
				
				fssi.setServerHost(in.readUTF());
				fssi.setMode(FileIOMode.valueOf(in.readInt()));
				fssi.setStatus(FileSliceServerInfo.Status.valueOf(in.readInt()));
				fssi.setType(FileSliceServerInfo.Type.valueOf(in.readInt()));
				
				String tmp=in.readUTF().trim();
				fssi.setKeyId((tmp.length()==0?null:tmp));
				tmp=in.readUTF().trim();
				fssi.setKey((tmp.length()==0?null:tmp));
				
				int cnt=in.readInt();
				System.out.println("prop cnt:" + cnt);
				String key, value;
				for(int i=0; i<cnt; i++){
					key=in.readUTF();
					value=in.readUTF();
					System.out.println("prop:" + key + "="+value);
					
					fssi.setProperty(key, value);
				}
				
				return fssi;
			}catch(IOException ex){
				//LOG.debug("Got IO exception");
				
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
			
			//LOG.debug("retry waiting response");
		}while(true);
	}

	@Override
	public void updateSliceServer(FileSliceServerInfo fssi, User usr)
			throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="updateSliceServer_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UPDATE");
					out.flush();
					
					out.writeUTF(fssi.getServerId());
					out.writeUTF(fssi.getServerHost());
					out.writeInt(fssi.getMode().value());
					out.writeInt(fssi.getStatus().value());
					out.writeInt(fssi.getType().value());
					out.writeUTF((fssi.getKeyId()==null? " " : fssi.getKeyId()));
					out.writeUTF((fssi.getKey()==null? " " : fssi.getKey()));
					
					if(fssi.hasProperties()){
						out.writeInt(fssi.getAllProperties().size());
						for(@SuppressWarnings("rawtypes") Map.Entry k: fssi.getAllProperties().entrySet()){
							out.writeUTF(k.getKey().toString());
							out.writeUTF(k.getValue().toString());
						}
					}
					
					out.flush();
				}
				
				//LOG.debug("request send");
				break;
			}catch(IOException ex){
				LOG.debug("Got IO exception");
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
			//LOG.debug("retry sending request");
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}

	@Override
	public String[] getSharedAccessURL(String serverId, String sliceName, User usr)
			throws SVDSException {
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getSharedAccessURL_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_NAMESPACE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("ACCESSURL");
					out.flush();
					
					out.writeUTF(serverId);
					out.writeUTF(sliceName);
					out.flush();
				}	
				
				//LOG.debug("request send");
				break;
			}catch(IOException ex){
				LOG.debug("Got IO exception");
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}
			//LOG.debug("retry sending request");
		}while(true);
		
		do{
			try{
				//wait for reply
				getResponse(reqId);
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					default:
						throw new SVDSException(respMsg);
				}
				
				int cnt=in.readInt();
				String[] urls=new String[cnt];
				for(int i=0; i<cnt; i++)
					urls[i]=in.readUTF();
				
				return urls;
			}catch(IOException ex){
				//LOG.debug("Got IO exception");
				
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
			
			//LOG.debug("retry waiting response");
		}while(true);
	}
}
