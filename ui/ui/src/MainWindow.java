package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import org.apache.http.client.AuthCache;

import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.RabinImpl2;
import sg.edu.nyp.sit.svds.client.ida.Util;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

import com.amazonaws.services.identitymanagement.model.summaryKeyType;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;

public class MainWindow extends JFrame
{
	private JPanel panel_contentPane;
	private JTextField tb_filename;

	private File _inputFile; // The input file
	private String _currentPath; // The path of the directory the user is currently in

	private int _p = generateLargePrime(); // large prime
	private int _alpha = generateSecureRandomInteger(_p); // random integer within large prime p
	private int _key = generatePRFKeyK();
	private List<Long> _listOfAuthenticators = new ArrayList<Long>();
	private List<Long> _listOfAuthenticators2 = new ArrayList<Long>();

	/**
	 * Launch the application.
	 */
	public static void main(String[] args)
	{
		EventQueue.invokeLater(new Runnable()
		{
			public void run()
			{
				try
				{
					MainWindow frame = new MainWindow();
					frame.setVisible(true);
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public MainWindow()
	{
		setResizable(false);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		panel_contentPane = new JPanel();
		panel_contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		panel_contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(panel_contentPane);

		JPanel panel_chooseFile = new JPanel();
		panel_contentPane.add(panel_chooseFile, BorderLayout.NORTH);
		panel_chooseFile.setLayout(new FormLayout(new ColumnSpec[] { ColumnSpec.decode("286px:grow"), ColumnSpec.decode("40px"), ColumnSpec.decode("93px"), }, new RowSpec[] { RowSpec.decode("23px"), }));

		tb_filename = new JTextField();
		tb_filename.setEditable(false);
		panel_chooseFile.add(tb_filename, "1, 1, fill, fill");
		tb_filename.setColumns(1);

		JButton btn_chooseFile = new JButton("Choose File");
		btn_chooseFile.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				JFileChooser fc = new JFileChooser("C:\\Sample Files");
				fc.setFileSelectionMode(JFileChooser.FILES_ONLY);

				int validFile = fc.showOpenDialog(panel_contentPane);
				if (validFile == JFileChooser.APPROVE_OPTION)
				{
					_inputFile = fc.getSelectedFile();
					tb_filename.setText(_inputFile.getName());
					_currentPath = fc.getCurrentDirectory().toString() + "\\";
				}
			}
		});
		panel_chooseFile.add(btn_chooseFile, "3, 1, fill, fill");

		JPanel panel_manipulationControls = new JPanel();
		panel_contentPane.add(panel_manipulationControls, BorderLayout.CENTER);
		panel_manipulationControls.setLayout(null);

		JButton btnSplit = new JButton("Split");
		btnSplit.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				try
				{
					// splitAndCombineFile();

					// Erasure encodes the input file and returns a list of the InputStreams generated
					// InputStreams will become empty after using it in a method
					List<InputStream> encodedSlices = getErasureEncodedFileSlices(_inputFile);
					
					// Convert the InputStreams into Byte Arrays
					List<byte[]> encodedSliceBytes = convertInputStreamToByteArray(encodedSlices);
					
					//printByteArray(encodedSliceBytes);

					// Generate list of authenticators
					/*for (InputStream inputStream : encodedSlices)
						_listOfAuthenticators.add(calculateAuthenticationValue(inputStream));*/
					
					// Generate list of authenticators using byte arrays
					for (byte[] ba : encodedSliceBytes)
						_listOfAuthenticators.add(calculateAuthenticationValue(ba));

					// Displays the values of _listOfAuthenticators to console
					System.out.print("Authenticators: ");
					for (long i : _listOfAuthenticators)
						System.out.print(i + ", ");
					System.out.println();

					// TODO: Create challenge set of L indices and L random coefficients
					// int qSize = 5;
					List<Integer> Q = new ArrayList<Integer>();

					for (int i = 1; i <= _listOfAuthenticators.size(); i++)
					{
						Q.add(i);
						Q.add(generateSecureRandomInteger(_p));
					}

					System.out.print("Q: ");
					for (int i : Q)
						System.out.print(i + ", ");

					System.out.println();

					// Sends Q to the prover
					// pseudo code: prover(List<InputStream> Q, List<InputStream> encodedSlices)
					//prover(Q, encodedSlices);
					proverr(Q, encodedSliceBytes);

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}
		});
		btnSplit.setBounds(175, 89, 84, 33);
		panel_manipulationControls.add(btnSplit);
	}

	private void splitAndCombineFile()
	{
		int rowSize = 15;
		int colSize = 12;

		// Generate a two-dimensional array to store the Cauchy mattix
		int[][] mat = Util.generateIndependenceMatrix(rowSize, colSize);

		// Generate the matrix as a string for use in IdaInfo
		StringBuffer sb = new StringBuffer();
		for (int row = 0; row < rowSize; row++)
		{
			for (int col = 0; col < colSize; col++)
			{
				sb.append(mat[row][col]);
				if (col < colSize - 1)
					sb.append(",");
			}
			if (row < rowSize - 1)
				sb.append("|");
		}

		String matrix = sb.toString();
		System.out.println("Matrix: " + matrix);

		// Declare an instance of the IDA algorithm
		IInfoDispersal iid = new RabinImpl2();

		// Declare an instance of the IdaInfo class
		IdaInfo info = new IdaInfo(rowSize, colSize, matrix);
		info.setDataSize(_inputFile.length());

		FileInputStream fileInputStream;

		try
		{
			fileInputStream = new FileInputStream(_inputFile);

			// Calculate time taken to split the file
			long startTime = System.currentTimeMillis();
			List<InputStream> lsSegmented = iid.split(fileInputStream, info);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time taken = " + (endTime - startTime));

			System.out.println("Output Segments :" + lsSegmented.size());

			List<InputStream> lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0;

			// Loop through List of InputStreams and add sequence number to each
			// element, starting from 0
			for (InputStream inputStream : lsSegmented)
			{
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));
			}

			// Rearrange InputStreams
			List<InputStream> lsShuffle = new ArrayList<InputStream>();

			// lsShuffle.add(lsSegmentedWithId.get(0)); // random
			// lsShuffle.add(lsSegmentedWithId.get(1));
			// lsShuffle.add(lsSegmentedWithId.get(2));
			// lsShuffle.add(lsSegmentedWithId.get(13)); // random
			// lsShuffle.add(lsSegmentedWithId.get(4));
			// lsShuffle.add(lsSegmentedWithId.get(5));
			// lsShuffle.add(lsSegmentedWithId.get(6));
			// lsShuffle.add(lsSegmentedWithId.get(12));
			// lsShuffle.add(lsSegmentedWithId.get(8));
			// lsShuffle.add(lsSegmentedWithId.get(9));
			// lsShuffle.add(lsSegmentedWithId.get(10));
			// lsShuffle.add(lsSegmentedWithId.get(11));

			// Pick random elements of colSize (e.g. 12) from lsSegmentedWithId
			// and add them to lsShuffle
			lsShuffle = PickRandomItems(lsSegmentedWithId, colSize);

			// TODO Create a method to output all file slices to disk for
			// visualization purposes
			/*
			 * int e = 0, dd; for (InputStream is : lsShuffle) {
			 * FileOutputStream os = new FileOutputStream(currentPath +
			 * "\\Slices\\slice" + e++ + "_" + tb_filename.getText()); while
			 * ((dd = is.read()) != -1) os.write(dd); os.close(); }
			 */

			// Combine elements of lsShuffle into a InputStream using the
			// IdaInfo variable, info
			// Requires each segment in List to have id, otherwise a OutOfBounds
			// exception will occur
			InputStream isCombined = iid.combine(lsShuffle, info);

			System.out.println("Current path: " + _currentPath + tb_filename.getText());
			FileOutputStream outputFile = new FileOutputStream(_currentPath + "combined_" + tb_filename.getText());

			int d;
			while ((d = isCombined.read()) != -1)
				outputFile.write(d);

			outputFile.close();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (IDAException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private List<InputStream> getErasureEncodedFileSlices(File inFile)
	// private void erasureEncodeFile(File inFile)
	{
		// int rowSize = 15; // Total number of blocks
		// int colSize = 12; // Number of blocks needed to reconstruct the data
		// (Quorum)

		int rowSize = 5;
		int colSize = 3;

		// Generate a two-dimensional array to store the Cauchy matrix
		int[][] mat = Util.generateIndependenceMatrix(rowSize, colSize);

		// Convert the matrix to a string for use in the IdaInfo class
		StringBuffer sb = new StringBuffer();
		for (int row = 0; row < rowSize; row++)
		{
			for (int col = 0; col < colSize; col++)
			{
				sb.append(mat[row][col]);
				if (col < colSize - 1)
					sb.append(",");
			}
			if (row < rowSize - 1)
				sb.append("|");
		}

		String matrix = sb.toString();
		System.out.println("Matrix: " + matrix);

		// Declare an instance of the IDA algorithm
		IInfoDispersal iid = new RabinImpl2();

		// Declare an instance of the IdaInfo class and set its size
		IdaInfo info = new IdaInfo(rowSize, colSize, matrix);
		info.setDataSize(inFile.length()); // Size of the file, in bytes

		FileInputStream fileInputStream;
		List<InputStream> lsSegmented = null;
		List<InputStream> lsSegmentedWithId = null;

		try
		{
			fileInputStream = new FileInputStream(inFile);

			// Split the input file into slices
			lsSegmented = iid.split(fileInputStream, info);

			// Display number of slices generated
			System.out.println("Output Segments : " + lsSegmented.size());

			lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0; // byte can contain value from -128 to 127

			// Loop through List of InputStreams and add sequence number to each
			// element, starting from 0
			for (InputStream inputStream : lsSegmented)
			{
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));
			}

		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (IDAException e)
		{
			e.printStackTrace();
		}

		return lsSegmentedWithId;
		// return lsSegmented;
	}
	
	private List<byte[]> convertInputStreamToByteArray(List<InputStream> listOfInputStreams)
	{
		List<byte[]> listOfByteArrays = new ArrayList<byte[]>();
		
		for (InputStream is : listOfInputStreams)
		{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			
			try
			{
				int oneByte;
				while ((oneByte = is.read()) != -1)
				{
					buffer.write(oneByte);
				}
			} catch (Exception ex)
			{
				ex.printStackTrace();
			}
			listOfByteArrays.add(buffer.toByteArray());
		}
		
		return listOfByteArrays;
	}

	/**
	 * Gets randomly chosen n number of items from a list
	 * 
	 * @param lst List of InputStreams
	 * @param n number of elements to be randomly chosen
	 * @return Returns a list of randomly picked InputStreams
	 **/
	private List<InputStream> PickRandomItems(List<InputStream> lst, int n)
	{
		List<InputStream> copy = new LinkedList<InputStream>(lst);
		Collections.shuffle(copy);
		return copy.subList(0, n);
	}

	/**
	 * Generates a large prime using a couple of SecureRandom instances
	 * 
	 * @return Returns a large prime p
	 */
	private int generateLargePrime()
	{
		boolean isPrime;
		int max, largestPrime = 3;

		try
		{
			SecureRandom randSeed = SecureRandom.getInstance("SHA1PRNG", "SUN");
			randSeed.nextBytes(new byte[128]);
			byte[] seed = randSeed.generateSeed(10);

			SecureRandom rand1 = SecureRandom.getInstance("SHA1PRNG", "SUN");
			rand1.setSeed(seed);

			max = rand1.nextInt((999999 - 100000 + 1) + 100000);

			for (int y = 3; y < max; y += 2)
			{
				isPrime = true;

				for (int z = 2; z <= Math.sqrt(y); z++)
				{
					if (y % z == 0)
						isPrime = false;
				}
				if (isPrime)
				{
					if (y > largestPrime)
						largestPrime = y;
				}
			}
			// System.out.println("Max: " + max);
			// System.out.println("Largest Prime: " + largestPrime);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		return largestPrime;
	}

	/**
	 * Generates a random value between 0 and bound
	 * 
	 * @paramm bound The upper limit
	 * @return Returns a securely generated random integer
	 */
	private int generateSecureRandomInteger(int bound)
	{
		int result = 0;

		try
		{
			SecureRandom randSeed = SecureRandom.getInstance("SHA1PRNG", "SUN");
			randSeed.nextBytes(new byte[128]);
			byte[] seed = randSeed.generateSeed(10);

			SecureRandom rand1 = SecureRandom.getInstance("SHA1PRNG", "SUN");
			rand1.setSeed(seed);

			result = rand1.nextInt(bound);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		return result;
	}

	/**
	 * Generates a secure random integer to be used as key k
	 * @return
	 */
	private int generatePRFKeyK()
	{
		Random rand = new Random();
		return (int) (rand.nextDouble() * 1000000);
	}

	/**
	 * Calculates the authentication value for a given block of the input file
	 * 
	 * @param inputStream A block of the erasure encoded file
	 * @return Returns the generated authentication value
	 */
	private long calculateAuthenticationValue(InputStream inputStream)
	{
		/* The issue is I do not know whether there are */
		/* multiple keys and alphas, or just one of each. */

		// int key = generatePRFKeyK();

		long authenticator;
		long blockXAlpha = 0;

		try
		{
			int oneByte;
			while ((oneByte = inputStream.read()) != -1)
			{
				blockXAlpha += _alpha * oneByte;
			}
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		authenticator = _key + blockXAlpha;
		return authenticator;
	}
	
	private long calculateAuthenticationValue(byte[] ba)
	{
		long authenticator;
		long blockXAlpha = 0;
		
		try
		{
			for (byte b : ba)
				blockXAlpha += _alpha * b;
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		authenticator = _key + blockXAlpha;
		return authenticator;
	}

	private void printInputStreamContents(List<InputStream> lis)
	{
		try
		{
			// Prints the contents of each InputStream to console
			int count = 1;
			for (InputStream is : lis)
			{
				System.out.println("Stream " + count++ + ": ");
				int oneByte;
				while ((oneByte = is.read()) != -1)
					System.out.write(oneByte);
				System.out.println();
				System.out.flush();
			}
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	private void printInputStreamContents(InputStream is)
	{
		try
		{
			// Prints the contents of each InputStream to console
			int oneByte;
			while ((oneByte = is.read()) != -1)
				System.out.write(oneByte);
			System.out.println();
			System.out.flush();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void printByteArrayInputStreamContents(List<ByteArrayInputStream> lbais)
	{
		try
		{
			// Prints the contents of each InputStream to console
			int count = 1;
			for (ByteArrayInputStream bais : lbais)
			{
				System.out.println("Stream " + count++ + ": ");
				int oneByte;
				while ((oneByte = bais.read()) != -1)
				{
					System.out.write(oneByte);
				}
				System.out.println();
				System.out.flush();
			}
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void printByteArrayInputStreamContents(ByteArrayInputStream bais)
	{
		try
		{
			// Prints the contents of each InputStream to console
			int oneByte;
			while ((oneByte = bais.read()) != -1)
			{
				System.out.write(oneByte);
			}
			System.out.println();
			System.out.flush();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void printByteArray(List<byte[]> loba)
	{
		int count = 1;
		for (byte[] ba : loba)
		{
			System.out.println("Stream " + count++ + ": ");
			System.out.println("Array length: " + ba.length);
			for (byte b : ba)
				System.out.write(b);
			System.out.println();
			System.out.flush();
		}
	}
	
	private void printByteArray(byte[] ba)
	{
		System.out.println("Stated byte count: " + ba.length);
		int counter = 0;
		for (byte b : ba)
		{
			if (b != -1)
			{
				System.out.write(b);
				counter++;
			}
			else
				break;
		}
			
		System.out.flush();
		System.out.println("Actual byte count: " + counter);
		System.out.println();
	}

	
	private void prover(List<Integer> Q, List<InputStream> encodedSlices)
	{

		System.out.println("EncodedSlices size: " + encodedSlices.size());

		try
		{
			// TODO: Calculate sigma as part of the response to be sent back to
			// the verifier
			long sigma = 0;

			List<Integer> coefficients = new ArrayList<Integer>();
			for (int i = 1; i < Q.size() + 1; i += 2)
			{
				coefficients.add(Q.get(i));
			}

			for (int i = 0; i < _listOfAuthenticators.size(); i++)
			{
				sigma += coefficients.get(i) * _listOfAuthenticators.get(i);
			}

			// Print the coefficients to console
			System.out.print("Coefficients: ");
			for (int i : coefficients)
				System.out.print(i + ", ");

			System.out.println();

			// Print the calculated sigma to console
			System.out.println("Sigma: " + sigma);

			// TODO: Calculate mu as part of the response to be sent back to the
			// verifier
			long mu = 0;

			for (int i = 0; i < coefficients.size(); i++)
			{
				InputStream inputStream = encodedSlices.get(i);
				int oneByte;
				while ((oneByte = inputStream.read()) != -1)
				{
					mu += coefficients.get(i) * oneByte;
				}
			}

			// Print the calculated mu to console
			System.out.println("Mu: " + mu);
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	 * Calculates sigma and mu to be returned to the verifier
	 * @param Q
	 */
	
	private void proverr(List<Integer> Q, List<byte[]> encodedSliceBytes)
	{
		System.out.println("EncodedSlices size: " + encodedSliceBytes.size());

		try
		{
			// Calculate sigma as part of the response to be sent back to the verifier
			long sigma = 0;

			List<Integer> coefficients = new ArrayList<Integer>();
			for (int i = 1; i < Q.size() + 1; i += 2)
			{
				coefficients.add(Q.get(i));
			}

			for (int i = 0; i < _listOfAuthenticators.size(); i++)
			{
				sigma += coefficients.get(i) * _listOfAuthenticators.get(i);
			}

			// Print the coefficients to console
			System.out.print("Coefficients: ");
			for (int i : coefficients)
				System.out.print(i + ", ");

			System.out.println();

			// Print the calculated sigma to console
			System.out.println("Sigma: " + sigma);

			/* ///////////////////////////////////////////////////////////////////////////// */			
			
			// Calculate mu as part of the response to be sent back to the verifier
			long mu = 0;
			
			for (int i = 0; i < coefficients.size(); i++)
			{
				byte[] ba = encodedSliceBytes.get(i);
				int coefficient = coefficients.get(i);
				for (byte b : ba)
					mu += coefficient * b;
			}

			System.out.println("Mu: " + mu);
			
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
}
