package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eldos.cbfs.ICbFsFileInfo;
import sg.edu.nyp.sit.svds.client.File;
import sg.edu.nyp.sit.svds.client.SVDSInputStream;
import sg.edu.nyp.sit.svds.client.SVDSOutputStream;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileInfo;
import sg.edu.nyp.sit.svds.metadata.User;

public class VirtualFile {
	public static final long serialVersionUID = 2L;

	private static final Log LOG = LogFactory.getLog(VirtualFile.class);

	private boolean isStreaming = false;

	private File f = null;
	private String virtualFullPath = null;

	private SVDSInputStream in = null;
	private long lastReadOffset = 0;

	private SVDSOutputStream out = null;
	private long lastWriteOffset = 0;

	private Date lastChk = null;
	private FileAttributes attrs;

	private List<VirtualSubFile> subFiles = null;

	

	private boolean isWriting = false;

	public VirtualFile(File f) {
		this.f = f;
		this.virtualFullPath = f.getFullPath().replace(FileInfo.PATH_SEPARATOR,
				VirtualFS.PATH_SEPARATOR);

		if (f.isDirectory()) {
			lastChk = new Date(0);
			attrs = FileAttributes.DIRECTORY;
			subFiles = new ArrayList<VirtualSubFile>();
		} else {
			attrs = FileAttributes.NORMAL;
		}
		// createa local temp for debugging

		
	}

	public void setEndOfFile(long position) throws Exception {
		if (position == 0) {
			LOG.debug("resize to 0 byte file");
			f.deleteFile();
			f.createNewFile();
			out = new SVDSOutputStream(f, isStreaming);
			out.close();
			out = null;
			isWriting = false;
		} else if (position < f.getFileSize()) {
			LOG.debug("originial file size=" + f.getFileSize());
			LOG.debug("truncating file");
			// copy previous buffer to new buffer
			if (out == null)
				out = new SVDSOutputStream(f, isStreaming);
			out.setEOF(position);
			out.close();
			out = null;
			isWriting = false;
		} /*
		 * else if (position > f.getFileSize()) { if (f.getFileSize() == 0) {
		 * LOG.debug("originial file size=" + f.getFileSize()); return; }
		 * LOG.debug("originial file size=" + f.getFileSize()); if (out == null)
		 * out = new SVDSOutputStream(f, isStreaming); long numBytesToFill =
		 * position - f.getFileSize(); LOG.debug("numBytesToFill=" +
		 * numBytesToFill); out.seek(f.getFileSize()); for (int i = 0; i <
		 * numBytesToFill; i++) { out.write(0); } out.close(); out = null;
		 * 
		 * }
		 */
	}

	public int write(byte[] data, long position, int bytesToWrite)
			throws Exception {

		
		try {
			// if updated file is shoter then the original,
			// truncate by deleting and creating new slices
			// this works only if the method is called once for entire file
			// write
			boolean forceClose = false;
			if (position + bytesToWrite < f.getFileSize()) {
				LOG.debug("written bytes is less than the original file size="
						+ f.getFileSize());
				if (isWriting) {
					if (out != null) {
						LOG.debug("write is still in progress, might be a random i/o");
						out.seek(position);
						// lastWriteOffset = position + bytesToWrite;
						// out.close();
						// out=null;
					}
				} else {
					f.deleteFile();
					f.createNewFile();
					forceClose = true;
				}
			}

			if (out == null) {
				out = new SVDSOutputStream(f, isStreaming);
				isWriting = true;
			}

			if (position != lastWriteOffset) {
				LOG.debug("position IS NOT equal to lastWriteOffset...do a seek");
				out.seek(position);

			}
			
			out.write(data, 0, bytesToWrite);

			lastWriteOffset = position + bytesToWrite;

			// when the updated file is shorter than the original,
			// eldos will perform a write, read, write operation as opposed to
			// write, read operation. Thus in this situation, the data from the
			// read
			// is the old data as the output stream is not closed thus the
			// updated
			// content is not updated in the file slices in the slice stores.
			// Therefore, to work around this, close the output stream to force
			// the data to be updated in the file slices in the slice stores.
			if (forceClose) {
				out.close();
				out = null;
				isWriting = false;
			}

			return bytesToWrite;
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				throw ex;
			} else
				return 0;
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			return 0;
		}

	}

	public int read(byte[] data, long position, int bytesToRead)
			throws Exception {
		// return 0;
		// it is observed that durind compress to zip (in windows
		// a read is done wihtout first closing after writing,
		// this will fail as only close will split the file and
		// send to slice stores.
		// since read is directly from slice store,
		// it will not read anything
		if (isWriting) {
			out.close();
			out = null;
			isWriting = false;
		}
		LOG.debug("checking if out is closing");
		/*
		 * while (out.isStreamClosing()) {
		 * LOG.debug("closing in progress.... yield"); Thread.yield(); }
		 */

		try {
			LOG.debug("check inputstream to see if it is already created");
			if (in == null)
				in = new SVDSInputStream(f, isStreaming);

			if (position != lastReadOffset) {
				LOG.debug("seeking position: " + position);
				in.seek(position);
			}
			LOG.debug("f.getFileSize="+f.getFileSize());
			int sizeToRead = (int) Math.min(bytesToRead, f.getFileSize()
					- position);
			LOG.debug("sizeToRead:" + sizeToRead);
			if (sizeToRead <= 0)
				return 0;
			LOG.debug("reading from inputstream");
			int bytesRead = in.read(data, 0, sizeToRead);
			LOG.debug("bytesRead: " + bytesRead);

			lastReadOffset += sizeToRead;

			LOG.debug("actual read size:" + sizeToRead);

			return sizeToRead;
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				throw ex;
			} else
				return 0;
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			return 0;
		}
	}

	public void move(String newPath) throws Exception {
		try {
			f.moveTo(newPath);
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				throw ex;
			}
		} catch (SVDSException ex) {
			LOG.error(ex);
			ex.printStackTrace();
		}
	}

	public void refreshDirectoryFiles(User usr) throws Exception {
		if (f.isFile())
			return;

		try {
			List<FileInfo> files = (MasterTableFactory.getFileInstance())
					.refreshDirectoryFiles(f.getNamespace(), f.getFullPath(),
							lastChk, usr);
			// List<FileInfo>
			// files=(MasterTableFactory.getFileInstance()).refreshDirectoryFiles(
			// f.getNamespace(), f.getFullPath(), new Date(0), usr);

			// no changes
			if (files == null)
				return;

			subFiles.clear();
			for (FileInfo fi : files) {
				LOG.debug(fi.getFullPath());
				subFiles.add(new VirtualSubFile(fi));
			}
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				throw ex;
			} else
				ex.printStackTrace();
		} catch (SVDSException ex) {
			ex.printStackTrace();
		}
	}

	public void close() throws Exception {
		
		try {
			if (out != null) {
				out.close();
				out = null;
				isWriting = false;
			}

			if (in != null) {
				in.close();
				in = null;
			}
		} catch (IOException ex) {
			if (ex.getMessage().equals(RejectedSVDSException.PROXY + "")) {
				throw new RejectedSVDSException(RejectedSVDSException.PROXY);
			} else {
				LOG.error(ex);
				ex.printStackTrace();
			}
		}
	}

	public boolean delete() throws Exception {
		try {
			boolean completed;

			if (f.isDirectory())
				completed = f.deleteDirectory();
			else
				completed = f.deleteFile();

			if (completed) {
				if (subFiles != null)
					subFiles.clear();
				subFiles = null;
				lastChk = null;
				f = null;
			}

			return completed;
		} catch (RejectedSVDSException ex) {
			if (ex.getOrigin() == RejectedSVDSException.PROXY) {
				throw ex;
			} else
				return false;
		} catch (SVDSException ex) {
			ex.printStackTrace();
			return false;
		}
	}

	public void changeDirectory(String oriDirPath, String newDirPath) {
		f.getFileInfo().setFullPath(
				newDirPath
						+ f.getFileInfo().getFullPath()
								.substring(oriDirPath.length()));
	}

	public VirtualSubFile getSubFile(int index) {
		return (index >= subFiles.size() ? null : subFiles.get(index));
	}

	public VirtualSubFile getSubFile(String name) {
		for (VirtualSubFile sf : subFiles) {
			if (sf.getName().equals(name))
				return sf;
		}

		return null;
	}

	public boolean hasSubFiles() {
		return subFiles.size() > 0;
	}

	public FileAttributes getAttrs() {
		return attrs;
	}

	public Date getCreationTime() {
		return f.getCreationDate();
	}

	public Date getLastAccessedTime() {
		return f.getLastAccessedDate();
	}

	public Date getLastModifiedTime() {
		return f.getLastModifiedDate();
	}

	public long getFileSize() {
		return f.getFileSize();
	}

	public String getFullPath() {
		return virtualFullPath;
	}

	public String getName() {
		return f.getFilename();
	}

	public void SetFileAttributes(ICbFsFileInfo fileInfo, Date creationTime,
			Date lastAccessTime, Date lastWriteTime, long fileAttributes) {
		LOG.debug("fileAttributes: volume=" + fileInfo.getVolume()
				+ ", attributes=" + fileAttributes);
		if (out != null)
			try {
				LOG.debug("closing the file:" + fileInfo.getFileName());
				out.close();
				out = null;
				isWriting = false;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		
	}
}
