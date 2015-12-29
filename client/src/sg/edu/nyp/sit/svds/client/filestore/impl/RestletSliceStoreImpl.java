package sg.edu.nyp.sit.svds.client.filestore.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.exception.CorruptedSVDSException;
import sg.edu.nyp.sit.svds.exception.NotFoundSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.exception.UnauthorizedSVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class RestletSliceStoreImpl
{
	public static void delete(FileSliceServerInfo fs, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/delete?" + query;

			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrueve data from " + fs.getServerHost());
			default:
				throw new Exception(fsConn.getResponseMessage());
			}
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}

	/**
	 * Stores the input file into the slicestore
	 * 
	 * @param fs Metadata for each slice store server
	 * @param in Byte array that contains contents of input slice
	 * @param length Size of the file slice
	 * @param query Contains slicename, checksum, key and blksize
	 * @throws Exception
	 */
	public static void store(FileSliceServerInfo fs, byte[] in, int length, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/put?" + query;

			System.out.println("Storing using url: " + strUrl);
			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			fsConn.setDoOutput(true);

			OutputStream out = fsConn.getOutputStream();
			out.write(in, 0, length);
			out.flush();
			out.close();

			System.out.println("Response: " + fsConn.getResponseCode());
			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case FileSliceInfo.SLICE_STATUS_EXPECTATION_FAILED:
				throw new CorruptedSVDSException(fsConn.getResponseMessage());
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrieve data from " + fs.getServerHost());
			default:
				throw new Exception(fsConn.getResponseMessage());
			}
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}

	public static int retrieve(FileSliceServerInfo fs, byte[] data, int dataOffset, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/get?" + query;

			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			fsConn.setDoInput(true);

			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case HttpURLConnection.HTTP_NOT_FOUND:
				throw new NotFoundSVDSException("File slice not found.");
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrieve data from " + fs.getServerHost());
			default:
				throw new SVDSException(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}

			InputStream in = fsConn.getInputStream();
			byte[] tmp = new byte[4096];
			int index = dataOffset;
			int len;
			while ((len = in.read(tmp)) != -1)
			{
				System.arraycopy(tmp, 0, data, index, len);
				index += len;
			}
			in.close();
			tmp = null;

			return index - dataOffset;
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}

	public static byte[] retrieve(FileSliceServerInfo fs, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/get?" + query;

			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			fsConn.setDoInput(true);

			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case HttpURLConnection.HTTP_NOT_FOUND:
				throw new NotFoundSVDSException("File slice not found.");
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrieve data from " + fs.getServerHost());
			default:
				throw new SVDSException(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}

			InputStream in = fsConn.getInputStream();
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			byte[] tmp = new byte[4096];
			int len;
			while ((len = in.read(tmp)) != -1)
			{
				out.write(tmp, 0, len);
			}
			in.close();
			tmp = null;

			return (out.size() > 0 ? out.toByteArray() : null);
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}

	public static void storeHashes(FileSliceServerInfo fs, List<byte[]> in, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/hput?" + query;

			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			fsConn.setDoOutput(true);

			OutputStream out = fsConn.getOutputStream();
			for (byte[] b : in)
			{
				out.write(b);
				out.flush();
			}
			out.close();

			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrueve data from " + fs.getServerHost());
			default:
				throw new Exception(fsConn.getResponseMessage());
			}
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}

	public static List<byte[]> retrieveHashes(FileSliceServerInfo fs, String query) throws Exception
	{
		HttpURLConnection fsConn = null;

		try
		{
			String strUrl = "http://" + fs.getServerHost() + "/svds/hget?" + query;

			URL fsUrl = new URL(strUrl);
			fsConn = (HttpURLConnection) fsUrl.openConnection();

			fsConn.setDoInput(true);

			switch (fsConn.getResponseCode())
			{
			case HttpURLConnection.HTTP_OK:
				break;
			case HttpURLConnection.HTTP_NOT_FOUND:
				return null;
			case HttpURLConnection.HTTP_UNAUTHORIZED:
				throw new UnauthorizedSVDSException("Unable to retrueve data from " + fs.getServerHost());
			default:
				throw new Exception(fsConn.getResponseCode() + ": " + fsConn.getResponseMessage());
			}

			byte[] d = new byte[Resources.HASH_BIN_LEN];
			List<byte[]> hashes = new ArrayList<byte[]>();

			// long timeStart=System.nanoTime();
			InputStream in = new BufferedInputStream(fsConn.getInputStream());
			while ((in.read(d)) != -1)
			{
				// must copy to a new array as list will only keep the reference
				hashes.add(Arrays.copyOf(d, d.length));
				// hashes.add(d);
				// d=new byte[Resources.HASH_LEN];
			}
			d = null;
			in.close();
			// long timeEnd=System.nanoTime();

			// System.out.println("Time taken: " + (timeEnd-timeStart));

			return hashes;
		} finally
		{
			if (fsConn != null)
				fsConn.disconnect();
		}
	}
}
