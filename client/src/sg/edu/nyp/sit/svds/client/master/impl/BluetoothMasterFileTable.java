package sg.edu.nyp.sit.svds.client.master.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.Status;
import sg.edu.nyp.sit.pvfs.svc.PVFSEvents;
import sg.edu.nyp.sit.pvfs.svc.PVFSSvc;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.exception.*;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class BluetoothMasterFileTable implements IMasterFileTable {
	public static final long serialVersionUID = 5L;
	
	private static final Log LOG = LogFactory.getLog(BluetoothMasterFileTable.class);
	
	private PVFSSvc svc;
	private DataOutputStream out;
	private DataInputStream in;
	
	private boolean rejectRequest=false;
	
	public BluetoothMasterFileTable(Runnable svc){
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
		
		LOG.debug("attempt reconnect");
		if(!svc.reconnect()) {
			rejectRequest=true;
			
			LOG.debug("reconnect return fail.");
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
		
		LOG.debug("refresh input output streams");
		
		return true;
	}
	
	private void sendMetadata(String data) throws SVDSException{
		//TODO: when reconnect fail because it was shutdown, call sendMetadata method from PVFSSvc.
		//may need to throw a specific exception indicating that the metadata is sync but no further
		//request should be made
		try{
			rejectRequest=true;
			
			svc.sendMetadata(data);
			
			throw new LastRequestSVDSException("Future request will be rejected");
		}catch(Exception e){
			//only if the reconnect and send message fail, then throw the error
			throw new SVDSException(e);
		}
	}
	
	private void getResponse(String reqId){
		svc.waitForResults(reqId).acquireUninterruptibly();
	}
	
	public void addFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="addFileInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("ADD");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
					out.writeInt(rec.getType().value());
					
					if(rec.getType()==FileInfo.Type.FILE){
						out.writeLong(rec.getFileSize());
						out.writeInt(rec.getIdaVersion());
	
						if(rec.verifyChecksum()){
							out.writeBoolean(true);
							out.writeInt(rec.getBlkSize());
							out.writeUTF(rec.getKeyHash());
						}else
							out.writeBoolean(false);
	
						writeFileSlices(rec.getSlices());
					}
					out.flush();
				}
				
				//if the write is successful, must break out of the loop!
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					case Status.ERR_CONFLICT:
						throw new DuplicatedSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				rec.setCreationDate(new Date(in.readLong()));
				
				//if the write is successful, must break out of the loop!
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
	
	public void updateFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="updateFileInfo_"+System.nanoTime();
		
		//testing
		/*
		System.out.println("DISCONNET NOW");
		try { Thread.sleep(10*1000); } catch (InterruptedException e) { }
		System.out.println("CONTINUE");
		*/
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UPDATE");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
					out.writeUTF(svc.getConnectingDevice());
					out.writeLong(rec.getFileSize());
					
					if(rec.verifyChecksum()){
						out.writeBoolean(true);
						out.writeInt(rec.getBlkSize());
						out.writeUTF(rec.getKeyHash());
					}else
						out.writeBoolean(false);
					
					writeFileSlices(rec.getSlices());
					
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					StringBuilder sb_data=new StringBuilder();
					sb_data.append("UPDATE");
					sb_data.append("\t"+rec.getFullPath());
					sb_data.append("\t"+svc.getConnectingDevice());
					sb_data.append("\t"+rec.getFileSize());
					if(rec.verifyChecksum()){
						sb_data.append("\t1");
						sb_data.append("\t"+rec.getBlkSize());
						sb_data.append("\t"+rec.getKeyHash());
					}else sb_data.append("\t0");
					
					sb_data.append("\t"+rec.getSlices().size());
					for(FileSliceInfo fsi: rec.getSlices()){
						sb_data.append("\t"+fsi.getSliceName());
						sb_data.append("\t"+fsi.getSliceSeq());
						sb_data.append("\t"+fsi.getLength());
						sb_data.append("\t"+(fsi.getSliceChecksum()==null?"":fsi.getSliceChecksum()));
						sb_data.append("\t"+fsi.getServerId());
						
						//Segment not implemented in PVFS
						/*
						sb_data.append("\t"+fsi.getSegmentCount());
						if(fsi.getSegments()!=null){
							for(FileSliceInfo fsgi: fsi.getSegments()){
								sb_data.append("\t"+fsgi.getSliceName());
								sb_data.append("\t"+fsgi.getServerId());
								sb_data.append("\t"+fsgi.getOffset());
								sb_data.append("\t"+fsgi.getLength());
							}
						}
						*/
					}
					
					LOG.debug("Send msg length:" + sb_data.length());
					//TODO: check if the data is more than 1024 bytes
					sendMetadata(sb_data.toString());
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					case Status.ERR_FILE_LOCKED:
						throw new LockedSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				rec.setLastModifiedDate(new Date(in.readLong()));
				
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
	
	private void writeFileSlices(List<FileSliceInfo> slices) throws IOException{
		synchronized(out){
			out.writeInt(slices.size());
			for(FileSliceInfo fsi: slices){
				out.writeUTF(fsi.getSliceName());
				out.writeInt(fsi.getSliceSeq());
				out.writeLong(fsi.getLength());
				out.writeUTF((fsi.getSliceChecksum()==null?"":fsi.getSliceChecksum()));
				out.writeUTF(fsi.getServerId());
				
				/*
				out.writeInt(fsi.getSegmentCount());
				if(fsi.getSegments()!=null){
					for(FileSliceInfo fsgi: fsi.getSegments()){
						out.writeUTF(fsgi.getSliceName());
						out.writeUTF(fsgi.getServerId());
						out.writeLong(fsgi.getOffset());
						out.writeLong(fsgi.getLength());
					}
				}
				*/
			}
			out.flush();
		}
	}
	
	public void moveFileInfo(FileInfo rec, String new_namespace, String new_path, 
			User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="moveFileInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("MOVE");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
					out.writeUTF(new_path);
					
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					case Status.ERR_FILE_LOCKED:
						throw new LockedSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				rec.setLastAccessedDate(new Date(in.readLong()));
				
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
	
	public void deleteFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="deleteFileInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("DELETE");
					out.flush();

					out.writeUTF(rec.getFullPath());
					out.writeInt(rec.getType().value());
					out.flush();
				}

				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					rejectRequest=true;
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
					case Status.ERR_FILE_LOCKED:
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
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}
	
	public void accessFile(FileInfo rec, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="accessFile_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("ACCESS");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
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
	
	public FileInfo getFileInfo(String namespace, String filename, 
			User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="getFileInfo_"+System.nanoTime();
		
		LOG.debug(reqId);
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GET");
					out.flush();
					
					out.writeUTF(filename);
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
				//LOG.debug("wait for response");
				getResponse(reqId);

				//LOG.debug("got response");

				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
				case Status.INFO_OK:
					break;
				case Status.ERR_BAD_REQUEST:
					LOG.debug(respMsg+": " + filename);
					throw new NotFoundSVDSException(respMsg);
				default:
					LOG.debug(respMsg+": " + filename);
					throw new SVDSException(respMsg);
				}

				FileInfo fi=new FileInfo(filename, namespace, null);

				int idaVersion=in.readInt();
				int shares=in.readInt();
				int quorum=in.readInt();
				String matrix=in.readUTF();

				fi.setIdaVersion(idaVersion);
				fi.setIda(new IdaInfo(shares, quorum, matrix));

				if(in.readInt()==Status.INFO_NO_MORE_CONTENT){
					fi.isEmpty=true;
					return fi;
				}else fi.isEmpty=false;

				fi.setType(FileInfo.Type.valueOf(in.readInt()));
				fi.setCreationDate(new Date(in.readLong()));
				fi.setLastModifiedDate(new Date(in.readLong()));
				fi.setLastAccessedDate(new Date(in.readLong()));

				if(fi.getType()==FileInfo.Type.DIRECTORY){
					return fi;
				}

				fi.setFileSize(in.readLong());

				if(in.readBoolean()){
					fi.setBlkSize(in.readInt());
					fi.setKeyHash(in.readUTF());
				}

				//fi.setChgMode(FileIOMode.valueOf(in.readInt()));
				fi.setChgMode(FileIOMode.NONE);

				int sliceCnt=in.readInt();
				List<FileSliceInfo> slices=new ArrayList<FileSliceInfo>();
				FileSliceInfo fsi;
				String slicename, sliceChecksum, sliceSvr;
				int sliceSeq;
				long sliceLen;
				for(int i=0; i<sliceCnt; i++){
					slicename=in.readUTF();
					sliceSeq=in.readInt();
					sliceLen=in.readLong();
					sliceChecksum=in.readUTF();
					if(sliceChecksum.isEmpty()) sliceChecksum=null;
					sliceSvr=in.readUTF();

					fsi=new FileSliceInfo(slicename, sliceSvr, sliceLen, 
							sliceChecksum, sliceSeq);
					//fsi.setSliceRecovery(in.readBoolean());
					fsi.setSliceRecovery(false);

					slices.add(fsi);
				}

				fi.setSlices(slices);

				getFileSliceServers();

				return fi;
			}catch(IOException ex){
				if(!handleIOError()) {
					//only if the reconnect fail, then throw the error
					LOG.debug("Error getting file info : " + ex.getMessage());
					throw new SVDSException(ex);
				}
			}finally{
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}
	
	private void getFileSliceServers() throws IOException{
		int fsCnt=in.readInt();
		String svrId, host, svrKeyId, svrKey;
		FileSliceServerInfo.Type svrType;
		FileIOMode svrMode;
		int propCnt;
		Properties svrProp;
		for(int i=0; i<fsCnt; i++){
			svrId=in.readUTF();
			host=in.readUTF();
			svrType=FileSliceServerInfo.Type.valueOf(in.readInt());
			svrMode=FileIOMode.valueOf(in.readInt());
			
			svrKeyId=in.readUTF().trim();
			svrKey=in.readUTF().trim();
			
			propCnt=in.readInt();
			svrProp=new Properties();
			for(int j=0; j<propCnt; j++){
				svrProp.put(in.readUTF(), in.readUTF());
			}
			//svrProp.list(System.out);
			
			IFileSliceStore.updateServerMapping(svrId, host, svrType, svrMode, 
					(svrKeyId.length()==0 ? null : svrKeyId), 
					(svrKey.length()==0 ? null : svrKey), svrProp);
		}
	}
	
	public void lockFileInfo(FileInfo rec, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="lockFileInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("LOCK");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
					//don't use the user object cos in PVFS, file is locked by the device
					out.writeUTF(svc.getConnectingDevice());
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					case Status.ERR_FILE_LOCKED:
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
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}
	
	public void unlockFileInfo(FileInfo rec, User usr) throws SVDSException{
		//should send message to unlock file because the previous request may result in error
		//and need to unlock the file
		
		String reqId="unlockFileInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("UNLOCK");
					out.flush();
					
					out.writeUTF(rec.getFullPath());
					//don't use the user object cos in PVFS, file is locked by the device
					out.writeUTF(svc.getConnectingDevice());
					out.flush();
				}
				
				break;
			}catch(IOException ex){
				if(!handleIOError()) {
					sendMetadata("UNLOCK\t"+rec.getFullPath()+"\t"+svc.getConnectingDevice());
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					case Status.ERR_FILE_LOCKED:
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
				LOG.debug("release input stream lock");
				svc.releaseInputStream();
			}
		}while(true);
	}
	
	public void changeFileMode(FileInfo rec, FileIOMode mode, User usr) 
		throws SVDSException{
		throw new NotSupportedSVDSException();
	}
	
	public void refreshChangeFileMode(FileInfo rec, User usr) throws SVDSException{
		throw new NotSupportedSVDSException();
	}
	
	public List<String> listFiles(String namespace, String directoryPath, User usr) 
		throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="listFiles_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("LIST");
					out.flush();
					
					out.writeUTF(directoryPath);
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
				
				int cnt=in.readInt();
				List<String> files=new ArrayList<String>();
				for(int i=0; i<cnt; i++){
					files.add(in.readUTF());
				}
				
				return files;
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
	
	public List<FileInfo> refreshDirectoryFiles(String namespace, String path, 
			Date lastChkDate, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		//LOG.debug("refresh directory files invoked. timespan: " + lastChkDate.getTime());
		
		String reqId="refreshDirectoryFiles_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("REFRESH");
					out.flush();
					
					//for testing the reconnection
					//System.out.println("Disable bluetooth now!\nMethod will continue in 60 seconds");
					//try { Thread.sleep(1000*60); } catch (InterruptedException e) {e.printStackTrace();}
					//System.out.println("Continue now");
	
					//start sending the data
					out.writeUTF(path);
					out.writeLong(lastChkDate.getTime());
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
					case Status.ERR_BAD_REQUEST:
						throw new NotFoundSVDSException(respMsg);
					default:
						throw new SVDSException(respMsg);
				}
				
				lastChkDate.setTime(in.readLong());
				
				int noOfFiles=in.readInt();
				if(noOfFiles==-1){
					LOG.debug("No files found");
					return null;
				}
				
				List<FileInfo> files=new ArrayList<FileInfo>();
				FileInfo fi;
				FileInfo.Type t;
				long tmpTime;
				for(int i=0; i<noOfFiles; i++){
					//values must be read in order
					path=in.readUTF();
					t=FileInfo.Type.valueOf(in.readInt());
					
					//System.out.println("File: " + path + ", " + t.toString());
					fi=new FileInfo(path, namespace, t);
					
					fi.setFileSize(in.readLong());
					fi.setCreationDate(new Date(in.readLong()));
					tmpTime=in.readLong();
					fi.setLastModifiedDate(tmpTime==0? null : new Date(tmpTime));
					tmpTime=in.readLong();
					fi.setLastAccessedDate(tmpTime==0 ? null : new Date(tmpTime));
					
					files.add(fi);
				}
				
				return files;
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
	
	public List<FileSliceInfo> generateFileSliceInfo(String namespace, int numReq, 
			FileIOMode pref, User usr) throws SVDSException{
		if(rejectRequest) throw new RejectedSVDSException(RejectedSVDSException.BLUETOOTH, "Request is rejected");
		
		String reqId="generateFileSliceInfo_"+System.nanoTime();
		
		do{
			try{
				synchronized(out){
					out.writeUTF(PVFSEvents.OP_FILE_REQ.toString());
					out.writeUTF(reqId);
					out.writeUTF("GENERATE");
					out.flush();
					
					out.writeInt(numReq);
					out.writeInt(pref.value());
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
				
				List<FileSliceInfo> slices=new ArrayList<FileSliceInfo>();
				
				int resp=in.readInt();
				String respMsg=in.readUTF();
				switch(resp){
					case Status.INFO_OK:
						break;
					case Status.ERR_BAD_REQUEST:
						return slices;
					default:
						throw new SVDSException(respMsg);
				}
				
				int cnt=in.readInt();
				String slicename, sliceSvr;
				for(int i=0; i<cnt; i++){
					slicename=in.readUTF();
					sliceSvr=in.readUTF();
					
					slices.add(new FileSliceInfo(slicename, sliceSvr));
				}
				
				getFileSliceServers();
				
				return slices;
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
}
