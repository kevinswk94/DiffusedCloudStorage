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
import java.math.BigInteger;
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
import javax.swing.JLabel;
import javax.swing.SpringLayout;

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
	private List<Integer> _coefficients = new ArrayList<Integer>();
	private JTextField tb_alpha;
	private JTextField tb_prfkey;
	private JLabel lbl_Alpha;
	private JLabel lbl_PrfKey;
	private JLabel lbl_sigma;
	private JTextField tb_sigma;
	private JLabel lbl_mu;
	private JTextField tb_mu;
	private JLabel lbl_computedSigma;
	private JTextField tb_calculatedSigma;
	private JLabel lbl_prime;
	private JTextField tb_prime;
	private JLabel lbl_result;
	private JTextField tb_result;

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
		setTitle("Standalone Prover / Verifier");
		setResizable(false);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 365);
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

		JButton btn_chooseFile = new JButton("File");
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

		JButton btn_verify = new JButton("Verify");
		btn_verify.setBounds(195, 250, 83, 23);
		btn_verify.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				try
				{
					tb_prime.setText(String.valueOf(_p));
					tb_alpha.setText(String.valueOf(_alpha));
					tb_prfkey.setText(String.valueOf(_key));
					
					// splitAndCombineFile();

					// Erasure encodes the input file and returns a list of the InputStreams generated
					// InputStreams will become empty after using it in a method
					List<InputStream> encodedSlices = getErasureEncodedFileSlices(_inputFile);
					
					// Convert the InputStreams into Byte Arrays
					List<byte[]> encodedSliceBytes = convertInputStreamToByteArray(encodedSlices);
					
					// Generate list of authenticators using byte arrays
					for (byte[] ba : encodedSliceBytes)
						_listOfAuthenticators.add(calculateAuthenticationValue(ba));

					System.out.println("Authenticators: ");
					for (long l : _listOfAuthenticators)
						System.out.print(l + ", ");
					System.out.println();
					
					// Create challenge set of L indices and L random coefficients
					// int qSize = 5;
					List<Integer> Q = new ArrayList<Integer>();

					for (int i = 1; i <= _listOfAuthenticators.size(); i++)
					{
						Q.add(i);
						
						//Q.add(generateSecureRandomInteger(_p));
						Q.add(generateSecureRandomInteger(255));
					}

					// Sends Q and file blocks to the prover. Get the response back
					List<Long> response = prover(Q, encodedSliceBytes);
					verifier(response);

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}
		});
		panel_manipulationControls.setLayout(null);
		panel_manipulationControls.add(btn_verify);
		
		tb_alpha = new JTextField();
		tb_alpha.setEditable(false);
		tb_alpha.setBounds(203, 82, 145, 20);
		panel_manipulationControls.add(tb_alpha);
		tb_alpha.setColumns(10);
		
		tb_prfkey = new JTextField();
		tb_prfkey.setEditable(false);
		tb_prfkey.setBounds(203, 49, 145, 20);
		panel_manipulationControls.add(tb_prfkey);
		tb_prfkey.setColumns(10);
		
		lbl_Alpha = new JLabel("Alpha:");
		lbl_Alpha.setBounds(49, 49, 83, 20);
		panel_manipulationControls.add(lbl_Alpha);
		
		lbl_PrfKey = new JLabel("PRF Key:");
		lbl_PrfKey.setBounds(49, 82, 83, 20);
		panel_manipulationControls.add(lbl_PrfKey);
		
		lbl_sigma = new JLabel("Sigma:");
		lbl_sigma.setBounds(49, 113, 54, 20);
		panel_manipulationControls.add(lbl_sigma);
		
		tb_sigma = new JTextField();
		tb_sigma.setEditable(false);
		tb_sigma.setColumns(10);
		tb_sigma.setBounds(203, 113, 145, 20);
		panel_manipulationControls.add(tb_sigma);
		
		lbl_mu = new JLabel("Mu:");
		lbl_mu.setBounds(49, 144, 54, 20);
		panel_manipulationControls.add(lbl_mu);
		
		tb_mu = new JTextField();
		tb_mu.setEditable(false);
		tb_mu.setColumns(10);
		tb_mu.setBounds(203, 144, 145, 20);
		panel_manipulationControls.add(tb_mu);
		
		lbl_computedSigma = new JLabel("Calculated Sigma:");
		lbl_computedSigma.setBounds(49, 175, 124, 20);
		panel_manipulationControls.add(lbl_computedSigma);
		
		tb_calculatedSigma = new JTextField();
		tb_calculatedSigma.setEditable(false);
		tb_calculatedSigma.setColumns(10);
		tb_calculatedSigma.setBounds(203, 175, 145, 20);
		panel_manipulationControls.add(tb_calculatedSigma);
		
		lbl_prime = new JLabel("Prime:");
		lbl_prime.setBounds(49, 18, 83, 20);
		panel_manipulationControls.add(lbl_prime);
		
		tb_prime = new JTextField();
		tb_prime.setEditable(false);
		tb_prime.setColumns(10);
		tb_prime.setBounds(203, 18, 145, 20);
		panel_manipulationControls.add(tb_prime);
		
		lbl_result = new JLabel("Result:");
		lbl_result.setBounds(49, 206, 54, 20);
		panel_manipulationControls.add(lbl_result);
		
		tb_result = new JTextField();
		tb_result.setEditable(false);
		tb_result.setColumns(10);
		tb_result.setBounds(203, 206, 145, 20);
		panel_manipulationControls.add(tb_result);
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
	
	/**
	 * Calculates sigma and mu to be returned to the verifier
	 * @param Q The randomly generated index-coefficient challenge
	 * @param encodedSliceBytes The list of byte arrays that contain the file slices
	 */
	private List<Long> prover(List<Integer> Q, List<byte[]> encodedSliceBytes)
	{
		System.out.println("EncodedSlices size: " + encodedSliceBytes.size());
		List<Long> response = new ArrayList<Long>();
		try
		{
			// Calculate sigma as part of the response to be sent back to the verifier
			long sigma = 0;

			
			for (int i = 1; i < Q.size() + 1; i += 2)
			{
				_coefficients.add(Q.get(i));
			}

			for (int i = 0; i < _listOfAuthenticators.size(); i++)
			{
				sigma += _coefficients.get(i) * _listOfAuthenticators.get(i);
			}

			// Print the coefficients to console
			System.out.print("Coefficients: ");
			for (int i : _coefficients)
				System.out.print(i + ", ");

			System.out.println();

			// Print the calculated sigma to console
			System.out.println("Sigma: " + sigma);

			/* ///////////////////////////////////////////////////////////////////////////// */			
			
			// Calculate mu as part of the response to be sent back to the verifier
			long mu = 0;
			
			for (int i = 0; i < _coefficients.size(); i++)
			{
				byte[] ba = encodedSliceBytes.get(i);
				int coefficient = _coefficients.get(i);
				for (byte b : ba)
					mu += coefficient * b;
			}

			System.out.println("Mu: " + mu);
			
			response.add(sigma);
			response.add(mu);
			tb_mu.setText(String.valueOf(mu));
			
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		return response;
	}
	
	/**
	 * 
	 * @param response
	 */
	private void verifier(List<Long> response)
	{
		long sigma = response.get(0);
		long mu = response.get(1);
		long keyXCoefficient = 0;
	
		for (int i : _coefficients)
			keyXCoefficient += _key * i;
		
		long verifySigma = _alpha * mu + keyXCoefficient;
		
		System.out.println("Sigma: " + sigma);
		System.out.println("Verify Sigma: " + verifySigma);
		
		tb_sigma.setText(String.valueOf(sigma));
		tb_calculatedSigma.setText(String.valueOf(verifySigma));
		
		if (sigma == verifySigma)
			tb_result.setText("Verified!");
		else
			tb_result.setText("Not Verified!");
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
}
