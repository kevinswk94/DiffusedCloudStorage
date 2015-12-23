package sg.edu.nyp.sit.svds.client.master.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.client.filestore.IFileSliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterAuthentication;
import sg.edu.nyp.sit.svds.client.master.IMasterFileTable;
import sg.edu.nyp.sit.svds.client.master.MasterAuthenticationFactory;
import sg.edu.nyp.sit.svds.exception.*;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;
import sg.edu.nyp.sit.svds.metadata.RestletMasterQueryPropName;
import sg.edu.nyp.sit.svds.metadata.User;

public class RestletMasterFileTable implements IMasterFileTable {
	public static final long serialVersionUID = 2L;
	
	private static final Log LOG = LogFactory.getLog(RestletMasterFileTable.class);
	
	private IMasterAuthentication auth=null;
	private final String host;
	private final String connector;
	
	//no of times to retry if the request to external servers failed
	private int retryTimes=10;
	private int retryPause=5000;	//5 seconds
	
	public RestletMasterFileTable(String host, String connector) {
		this.host=host;
		this.connector=connector;
		auth=MasterAuthenticationFactory.getInstance();
	}
	
	@Override
	public void addFileInfo(FileInfo rec, User usr) throws SVDSException {
		Long seqNo=new Long((new Date()).getTime());
		
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			success=addFileInfo(rec, seqNo.toString(), usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error adding file metadata.");
	}
	
	private boolean addFileInfo(FileInfo rec, String seq, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/add?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.SEQ.value()+"=" + seq
				+ "&"+RestletMasterQueryPropName.File.OWNER.value()+"=" + URLEncoder.encode(rec.getOwner().getId(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.TYPE.value()+"=" + rec.getType().value()
				+"&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+(rec.getType()==FileInfo.Type.DIRECTORY? "":"&"+RestletMasterQueryPropName.File.SIZE.value()+"=" + rec.getFileSize()
						+ "&"+RestletMasterQueryPropName.File.IDA_VERSION.value()+"=" + rec.getIdaVersion()
						+ (!rec.verifyChecksum() ? "": "&"+RestletMasterQueryPropName.File.FILE_BLKSIZE.value()+"=" + rec.getBlkSize()
								+ (rec.getKeyHash()==null? "":"&"+RestletMasterQueryPropName.File.FILE_KEYHASH.value()+"=" 
										+ URLEncoder.encode(rec.getKeyHash(), "UTF-8"))))
				+(auth==null || usr==null?"":auth.generateAuthentication(usr, null));
		
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			fsConn.setDoInput(true);
			
			if(rec.getType()==FileInfo.Type.FILE && rec.getFileSize()>0){
				fsConn.setDoOutput(true);
				
				OutputStream out=fsConn.getOutputStream();
				
				writeFileSlices(out, rec);
				
				out.close();
			}
			
			boolean success=true;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_CONFLICT:
					throw new DuplicatedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					success= false;
			}
			
			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String strDt=in.readLine();
			in.close();
			
			rec.setCreationDate(new Date(Long.parseLong(strDt)));
			rec.setLastModifiedDate(new Date(Long.parseLong(strDt)));
			rec.setLastAccessedDate(new Date(Long.parseLong(strDt)));
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public void deleteFileInfo(FileInfo rec, User usr) throws SVDSException {
		Long seqNo=new Long((new Date()).getTime());
		
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			success=deleteFileInfo(rec, seqNo.toString(), usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error removing file metadata.");
	}
	
	private boolean deleteFileInfo(FileInfo rec, String seq, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/delete?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.TYPE.value()+"=" + rec.getType().value()
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+(auth==null || usr==null? "":auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			boolean success = true;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case FileInfo.FILE_STATUS_LOCKED:
					throw new LockedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					success= false;
			}
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public FileInfo getFileInfo(String namespace, String filename, User usr) throws SVDSException {
		int retryCnt=0;
		boolean toRetry, success=false;
		FileInfo rec;
		do{
			toRetry=false;
			rec=new FileInfo(filename, namespace, null);
			success=getFileInfo(namespace, filename, rec, usr);
			
			if(!success	&& retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				rec=null;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting file metadata.");
		
		return rec;
	}
	
	private boolean getFileInfo(String namespace, String filename, FileInfo fi, User usr)
		throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/get?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(filename, "UTF-8")
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));

			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
			
			if(data.size()<4)
				throw new SVDSException("File information is not complete.");

			fi.setIdaVersion(Integer.parseInt(data.get(FileInfo.PropName.IDA_VERSION.value()).toString()));
			fi.setIda(new IdaInfo(Integer.parseInt(data.get(IdaInfo.PropName.SHARES.value()).toString()),
					Integer.parseInt(data.get(IdaInfo.PropName.QUORUM.value()).toString()),
					data.get(IdaInfo.PropName.MATRIX.value()).toString()));
			if(data.size()==4){
				fi.isEmpty=true;
				return true;
			}
			
			fi.isEmpty=false;
			fi.setType(FileInfo.Type.valueOf(Integer.parseInt(data.get(FileInfo.PropName.TYPE.value()).toString())));
			fi.setOwner(new User(data.get(FileInfo.PropName.OWNER.value()).toString()));
			fi.setCreationDate(new Date(Long.parseLong(data.get(FileInfo.PropName.CREATION.value()).toString())));
			fi.setLastModifiedDate(new Date(Long.parseLong(data.get(FileInfo.PropName.LASTMOD.value()).toString())));
			fi.setLastAccessedDate(new Date(Long.parseLong(data.get(FileInfo.PropName.LASTACC.value()).toString())));
			fi.setNamespace(namespace);
			fi.setFullPath(filename);
			
			if(fi.getType()==FileInfo.Type.FILE){
				fi.setFileSize(Long.parseLong(data.get(FileInfo.PropName.SIZE.value()).toString()));
				fi.setChgMode(FileIOMode.valueOf(Integer.parseInt(data.get(FileInfo.PropName.CHGMODE.value()).toString())));
				
				if(data.containsKey(FileInfo.PropName.SLICE_BLKSIZE.value())){
					fi.setKeyHash(data.get(FileInfo.PropName.SLICE_KEYHASH.value()).toString());
					fi.setBlkSize(Integer.parseInt(data.get(FileInfo.PropName.SLICE_BLKSIZE.value()).toString()));
				}
				
				int sliceCnt=Integer.parseInt(data.get(FileSliceInfo.PropName.COUNT.value()).toString());
				FileSliceInfo fsi;
				List<FileSliceInfo> slices=new ArrayList<FileSliceInfo>();
				for(int cnt=0; cnt<sliceCnt; cnt++){
					if(!data.containsKey(FileSliceInfo.PropName.NAME.value()+cnt) || 
						!data.containsKey(FileSliceInfo.PropName.SVR.value()+cnt) ||
						!data.containsKey(FileSliceInfo.PropName.SEQ.value()+cnt) ||
						!data.containsKey(FileSliceInfo.PropName.LEN.value()+cnt) ||
						!data.containsKey(FileSliceInfo.PropName.SEG_RECOVERY.value()+cnt))
						return false;
					
					fsi=new FileSliceInfo(data.get(FileSliceInfo.PropName.NAME.value()+cnt).toString(),
							data.get(FileSliceInfo.PropName.SVR.value()+cnt).toString(),
							Long.parseLong(data.get(FileSliceInfo.PropName.LEN.value()+cnt).toString()),
							(!data.containsKey(FileSliceInfo.PropName.CHECKSUM.value()+cnt) ? null : data.get(FileSliceInfo.PropName.CHECKSUM.value()+cnt).toString()),
							Integer.parseInt(data.get(FileSliceInfo.PropName.SEQ.value()+cnt).toString()));
					
					fsi.setSliceRecovery(data.get(FileSliceInfo.PropName.SEG_RECOVERY.value()+cnt).toString().equals("0") ? false : true);
					
					slices.add(fsi);
				}
				fi.setSlices(slices);
				slices=null;
				
				//update the global mapping table
				Hashtable<Integer, Properties> fsOpts=getFileSliceServersOptions(data);
				
				int mappingCnt=Integer.parseInt(data.get(FileSliceServerInfo.PropName.COUNT.value()).toString());
				String keyId, key;
				for(int cnt=0; cnt<mappingCnt; cnt++){
					keyId=data.containsKey(FileSliceServerInfo.PropName.KEYID.value()+cnt) ? 
							data.get(FileSliceServerInfo.PropName.KEYID.value()+cnt).toString() : "";
					key=data.containsKey(FileSliceServerInfo.PropName.KEY.value()+cnt) ? 
							data.get(FileSliceServerInfo.PropName.KEY.value()+cnt).toString() : "";
							
					IFileSliceStore.updateServerMapping(data.get(FileSliceServerInfo.PropName.ID.value()+cnt).toString(),
							data.get(FileSliceServerInfo.PropName.HOST.value()+cnt).toString(),
							FileSliceServerInfo.Type.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.TYPE.value()+cnt).toString())),
							FileIOMode.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.MODE.value()+cnt).toString())),
							(keyId.length()==0? null: keyId), (key.length()==0? null: key),
							fsOpts.get(cnt));
				}
			}

			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			//ex.printStackTrace();
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	private Hashtable<Integer, Properties> getFileSliceServersOptions(Properties data){
		if(data==null)
			return null;
		
		Hashtable<Integer, Properties> tbl=new Hashtable<Integer, Properties>();
		
		String keyName;
		Properties prop;
		for(Object k: data.keySet()){
			keyName=k.toString();
			
			if(keyName.startsWith(FileSliceServerInfo.PropName.OPT.value())){
				int i=Integer.parseInt(keyName.substring(FileSliceServerInfo.PropName.OPT.value().length(), 
						keyName.indexOf(".")));
				
				if(tbl.containsKey(i))
					prop=tbl.get(i);
				else{
					prop=new Properties();
					tbl.put(i, prop);
				}
					
				prop.put(keyName.substring(keyName.indexOf(".")+1), 
						data.get(k));
			}
		}
		
		return tbl;
	}
	
	@Override
	public List<String> listFiles(String namespace, String directoryPath, User usr)
		throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		List<String> files=new ArrayList<String>();
		do{
			toRetry=false;
			
			files.clear();
			success=listFiles(namespace, directoryPath, files, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error getting directory listing.");
		
		return files;
	}
	
	private boolean listFiles(String namespace, String directoryPath, List<String> files,
		User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/list?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(directoryPath, "UTF-8")
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			
			String tmp;
			while((tmp=reader.readLine())!=null){
				if(tmp.length()==0)
					continue;
				files.add(tmp);
			}
			reader.close();
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void updateFileInfo(FileInfo rec, User usr) throws SVDSException{
		Long seqNo=new Long((new Date()).getTime());
		
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=updateFileInfo(rec, seqNo.toString(), usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error updating file metadata.");
	}
	
	private boolean updateFileInfo(FileInfo rec, String seq, User usr)
		throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/update?" 
				+  RestletMasterQueryPropName.File.NAMESPACE.value()+"="	+ URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.SIZE.value()+"=" + rec.getFileSize()
				+ "&"+RestletMasterQueryPropName.File.USER.value()+"=" + rec.getLockBy().getId()
				+ "&"+RestletMasterQueryPropName.File.SEQ.value()+"=" + seq
				+ "&"+RestletMasterQueryPropName.File.FILE_BLKSIZE.value()+"="+rec.getBlkSize()
				+ (rec.getKeyHash()==null?""
						:"&"+RestletMasterQueryPropName.File.FILE_KEYHASH.value()+"="+URLEncoder.encode(rec.getKeyHash(), "UTF-8"))
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoOutput(true);
			fsConn.setDoInput(true);
			
			OutputStream out=fsConn.getOutputStream();
			
			writeFileSlices(out, rec);
			
			out.close();
			
			boolean success=true;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case FileInfo.FILE_STATUS_LOCKED:
					throw new LockedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					success= false;
			}
			
			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String strDt=in.readLine();
			in.close();
			
			rec.setLastModifiedDate(new Date(Long.parseLong(strDt)));
			rec.setLastAccessedDate(new Date(Long.parseLong(strDt)));
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	private void writeFileSlices(OutputStream out, FileInfo rec) throws Exception{
		out.write((FileSliceInfo.PropName.COUNT.value()+"="+rec.getSlices().size()+"\n").getBytes());
		int cnt=0;
		String segPrefix=null;
		FileSliceInfo seg=null;
		for(FileSliceInfo i: rec.getSlices()){
			out.write((FileSliceInfo.PropName.NAME.value()+cnt+"="+Resources.encodeKeyValue(i.getSliceName())+"\n").getBytes());
			out.write((FileSliceInfo.PropName.SEQ.value()+cnt+"="+i.getSliceSeq()+"\n").getBytes());
			out.write((FileSliceInfo.PropName.SVR.value()+cnt+"="+Resources.encodeKeyValue(i.getServerId())+"\n").getBytes());
			out.write((FileSliceInfo.PropName.LEN.value()+cnt+"="+i.getLength()+"\n").getBytes());
			if(rec.verifyChecksum()) out.write((FileSliceInfo.PropName.CHECKSUM.value()+cnt+"="+Resources.encodeKeyValue(i.getSliceChecksum())+"\n").getBytes());
			out.write((FileSliceInfo.PropName.SEG_CNT.value()+cnt+"="+(i.hasSegments()? i.getSegments().size() : 0)+"\n").getBytes());
			
			//segments are written in the order they are in the list
			if(i.hasSegments()){
				segPrefix=FileSliceInfo.PropName.SEG.value()+cnt+"_";
				for(int s=0; s<i.getSegments().size(); s++){
					seg=i.getSegments().get(s);
					
					out.write((segPrefix+FileSliceInfo.PropName.NAME.value()+s+"="+Resources.encodeKeyValue(seg.getSliceName())+"\n").getBytes());
					out.write((segPrefix+FileSliceInfo.PropName.SVR.value()+s+"="+Resources.encodeKeyValue(seg.getServerId())+"\n").getBytes());
					out.write((segPrefix+FileSliceInfo.PropName.SEG_OFFSET.value()+s+"="+seg.getOffset()+"\n").getBytes());
					out.write((segPrefix+FileSliceInfo.PropName.SEG_LEN.value()+s+"="+seg.getLength()+"\n").getBytes());
				}
			}
			
			out.flush();
			cnt++;
		}
	}
	
	@Override
	public void lockFileInfo(FileInfo rec, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=lockFile(rec, true, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to lock file.");
	}
	
	private boolean lockFile(FileInfo rec, boolean lock, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/"+(lock? "lock":"unlock")+"?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"="+ URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.USER.value()+"=" + rec.getLockBy().getId()
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			boolean success = true;;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
					//throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_CONFLICT:
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case FileInfo.FILE_STATUS_LOCKED:
					throw new LockedSVDSException(fsConn.getResponseMessage());
					//throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void unlockFileInfo(FileInfo rec, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=lockFile(rec, false, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to unlock file.");
	}
	
	//does not support changing namespace because of some slice servers
	//may not support the new namespace
	@Override
	public void moveFileInfo(FileInfo rec, String new_namespace, String new_path, 
			User usr)throws SVDSException{
		Long seqNo=new Long((new Date()).getTime());
		
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=moveFileInfo(rec, new_namespace, new_path, seqNo.toString(), usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error moving file metadata.");
	}
	
	private boolean moveFileInfo(FileInfo rec, String new_namespace, String new_path, 
			String seq, User usr)throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/move?" 
				+ RestletMasterQueryPropName.File.OLD_NAMESPACE.value()+"="+ URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.OLD_PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.NAMESPACE.value()+"="	+ URLEncoder.encode(new_namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(new_path, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.SEQ.value()+"=" + seq
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			boolean success=true;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_CONFLICT:
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case FileInfo.FILE_STATUS_LOCKED:
					throw new LockedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					success= false;
			}
			
			BufferedReader in = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String strDt=in.readLine();
			in.close();
			
			rec.setLastAccessedDate(new Date(Long.parseLong(strDt)));
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}

	@Override
	public List<FileSliceInfo> generateFileSliceInfo(String namespace, int numReq, 
			FileIOMode pref, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		List<FileSliceInfo> slices=null;
		do{
			toRetry=false;
			if(slices!=null){
				slices.clear();
				slices=null;
			}
			
			slices=new ArrayList<FileSliceInfo>();
			success=generateFileSliceInfo(namespace, numReq, pref, slices, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				slices=null;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Error generating file metadata.");
		
		return slices;
	}
	
	private boolean generateFileSliceInfo (String namespace, int numReq, FileIOMode pref, 
			List<FileSliceInfo> slices, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/generate?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.SIZE.value()+"=" + numReq
				+ "&"+RestletMasterQueryPropName.File.MODE.value()+"="+pref.value()
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					//throw new NotFoundSVDSException(fsConn.getResponseMessage());
					//means no slice server of the namespace is found so no slice is generated, return empty list
					return true;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
			
			FileSliceInfo fsi;
			for(int cnt=0; cnt<numReq; cnt++){
				if(!data.containsKey(FileSliceInfo.PropName.NAME.value()+cnt) || 
					!data.containsKey(FileSliceInfo.PropName.SVR.value()+cnt))
					throw new SVDSException("File slice information is corrupted.");
				
				fsi=new FileSliceInfo(data.get(FileSliceInfo.PropName.NAME.value()+cnt).toString(),
						data.get(FileSliceInfo.PropName.SVR.value()+cnt).toString());
				
				//set a default sequence no for the slices
				fsi.setSliceSeq(cnt);
				
				slices.add(fsi);
				fsi=null;
			}
			
			//update the global mapping table
			Hashtable<Integer, Properties> fsOpts=getFileSliceServersOptions(data);
			
			int mappingCnt=Integer.parseInt(data.get(FileSliceServerInfo.PropName.COUNT.value()).toString());
			LOG.debug("gen slices mapping cnt: " + mappingCnt);
			String keyId, key;
			for(int cnt=0; cnt<mappingCnt; cnt++){
				keyId=data.containsKey(FileSliceServerInfo.PropName.KEYID.value()+cnt) ? 
						data.get(FileSliceServerInfo.PropName.KEYID.value()+cnt).toString() : "";
				key=data.containsKey(FileSliceServerInfo.PropName.KEY.value()+cnt) ? 
						data.get(FileSliceServerInfo.PropName.KEY.value()+cnt).toString() : "";
							
				IFileSliceStore.updateServerMapping(data.get(FileSliceServerInfo.PropName.ID.value()+cnt).toString(),
						data.get(FileSliceServerInfo.PropName.HOST.value()+cnt).toString(),
						FileSliceServerInfo.Type.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.TYPE.value()+cnt).toString())),
						FileIOMode.valueOf(Integer.parseInt(data.get(FileSliceServerInfo.PropName.MODE.value()+cnt).toString())),
						(keyId.length()==0? null: keyId), (key.length()==0? null: key),
						fsOpts.get(cnt));
			}
		
			return true;
		}catch(SVDSException ex){
			LOG.error(ex);
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public List<FileInfo> refreshDirectoryFiles(String namespace, String path, 
			Date lastChkDate, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry;
		int success;
		List<FileInfo> files=new ArrayList<FileInfo>();
		do{
			toRetry=false;
			files.clear();
			success=refreshDirectoryFiles(namespace, path, lastChkDate, files, usr);
			
			if(success<0 && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}else if(success==1)
				files=null;
		}while(toRetry);

		if(success<0)
			throw new SVDSException("Error getting directory files information.");
		
		return files;
	}
	
	private int refreshDirectoryFiles(String namespace, String path, Date lastChkDate, 
			List<FileInfo> files, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/refresh?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(path, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.LAST_CHECK.value()+"=" + lastChkDate.getTime()
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));

			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return -1;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			Properties data=new Properties();
			data.load(reader);
			reader.close();
			
			if(!data.containsKey(FileInfo.PropName.COUNT.value()) ||
					!data.containsKey(FileInfo.PropName.DIR_LASTCHECK.value()))
				throw new CorruptedSVDSException("Missing return information.");
			
			lastChkDate.setTime(Long.parseLong(data.get(FileInfo.PropName.DIR_LASTCHECK.value()).toString()));
			
			FileInfo fi=null;
			int noOfFiles=Integer.parseInt(data.get(FileInfo.PropName.COUNT.value()).toString());
			LOG.debug(noOfFiles + " file changes!");
			
			if(noOfFiles==-1){
				return 1;
			}
			
			for(int i=0; i<noOfFiles; i++){
				fi=new FileInfo(data.get(FileInfo.PropName.PATH.value()+i).toString(), namespace,
						FileInfo.Type.valueOf(Integer.parseInt(data.get(FileInfo.PropName.TYPE.value()+i).toString())));

				fi.setCreationDate(new Date(Long.parseLong(data.get(FileInfo.PropName.CREATION.value()+i).toString())));
				fi.setLastAccessedDate(new Date(Long.parseLong(data.get(FileInfo.PropName.LASTACC.value()+i).toString())));
				fi.setLastModifiedDate(new Date(Long.parseLong(data.get(FileInfo.PropName.LASTMOD.value()+i).toString())));
				fi.setFileSize(Long.parseLong(data.get(FileInfo.PropName.SIZE.value()+i).toString()));

				files.add(fi);
			}

			return 0;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			ex.printStackTrace();
			LOG.error(ex);
			return -1;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void accessFile(FileInfo rec, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=accessFile(rec.getNamespace(), rec.getFullPath(), usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to access file.");
	}
	
	private boolean accessFile(String namespace, String filename, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/access?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"="	+ URLEncoder.encode(namespace, "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(filename, "UTF-8")
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
		
			boolean success = true;;
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_CONFLICT:
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			return success;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void changeFileMode(FileInfo rec, FileIOMode mode, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=chgFileMode(rec, mode, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to change file mode.");
	}
	
	private boolean chgFileMode(FileInfo rec, FileIOMode mode, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/chgmode?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.MODE.value()+"="+mode.value()
				+ "&"+RestletMasterQueryPropName.File.USER.value()+"=" + URLEncoder.encode(rec.getLockBy().getId(), "UTF-8")
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case FileInfo.FILE_STATUS_LOCKED:
					throw new LockedSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}

			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
	
	@Override
	public void refreshChangeFileMode(FileInfo rec, User usr) throws SVDSException{
		int retryCnt=0;
		boolean toRetry, success=false;
		do{
			toRetry=false;
			
			success=refreshChgFileMode(rec, usr);
			
			if(!success && retryCnt<retryTimes){
				toRetry=true;
				retryCnt++;
				try {
					//if the request is not completed, retry again after waiting a while
					Thread.sleep(retryPause);
				} catch (InterruptedException e) {
				}
			}
		}while(toRetry);

		if(!success)
			throw new SVDSException("Unable to refresh change file mode.");
	}
	
	private boolean refreshChgFileMode(FileInfo rec, User usr) throws SVDSException{
		HttpURLConnection fsConn=null;
		
		try{
			String strUrl = connector+"://" + host + "/file/chkstatus?" 
				+ RestletMasterQueryPropName.File.NAMESPACE.value()+"=" + URLEncoder.encode(rec.getNamespace(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.PATH.value()+"=" + URLEncoder.encode(rec.getFullPath(), "UTF-8")
				+ "&"+RestletMasterQueryPropName.File.STATUS.value()+"=MODE"
				+(auth==null || usr==null ? "" :auth.generateAuthentication(usr, null));
			
			URL fsUrl = new URL(strUrl);
			fsConn=(HttpURLConnection)fsUrl.openConnection();
			
			fsConn.setDoInput(true);
			
			switch(fsConn.getResponseCode()){
				case HttpURLConnection.HTTP_OK:
					break;
				case HttpURLConnection.HTTP_NOT_FOUND:
					throw new NotFoundSVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_NOT_ACCEPTABLE:
				case HttpURLConnection.HTTP_BAD_REQUEST:
					throw new SVDSException(fsConn.getResponseMessage());
				case HttpURLConnection.HTTP_UNAUTHORIZED:
					throw new UnauthorizedSVDSException(fsConn.getResponseMessage());
				default:
					return false;
			}
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(fsConn.getInputStream()));
			String mode=reader.readLine();
			reader.close();
			
			if(mode==null || mode.length()==0)
				return false;
			
			rec.setChgMode(FileIOMode.valueOf(Integer.parseInt(mode)));
			
			return true;
		}catch(SVDSException ex){
			throw ex;
		}catch(Exception ex){
			LOG.error(ex);
			return false;
		}finally{
			if(fsConn!=null)
				fsConn.disconnect();
		}
	}
}
