package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.JFileChooser;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.JButton;

import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.RabinImpl2;
import sg.edu.nyp.sit.svds.client.ida.Util;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import java.awt.Color;
import java.awt.Font;

public class Split extends JFrame
{
	private File _inputFile; // The input file to be split
	private File _challengeFile; // The input challenge file
	private File _responseFile; // The input response file
	private String _currentPath; // The path of the directory the user is currently in

	private int _p; // large prime
	private int _alpha; // random integer within large prime p
	private List<Integer> _alphas = new ArrayList<Integer>();
	private int _key; // random integer generated using a PRF

	private JPanel panel_contentPane;
	private JTextField tb_filename;
	private JTextField tb_primeP;
	private JTextField tb_alpha;
	private JTextField tb_prfKey;
	private JTextField tb_chalFilename;
	private JTextField tb_sigma;
	private JTextField tb_mu;
	private JTextField tb_respFilename;
	private JTextField tb_fileSigma;
	private JTextField tb_calcSigma;
	private JTextField tb_status;

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
					Split frame = new Split();
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
	public Split()
	{
		setTitle("Proof of Retrievability");
		setResizable(false);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 300);
		panel_contentPane = new JPanel();
		panel_contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(panel_contentPane);
		panel_contentPane.setLayout(null);

		JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		tabbedPane.setBounds(0, 0, 444, 271);
		panel_contentPane.add(tabbedPane);

		JPanel panel_splitterPane = new JPanel();
		tabbedPane.addTab("Splitter", null, panel_splitterPane, null);
		panel_splitterPane.setLayout(null);

		tb_filename = new JTextField();
		tb_filename.setEditable(false);
		tb_filename.setBounds(10, 11, 320, 20);
		panel_splitterPane.add(tb_filename);
		tb_filename.setColumns(10);

		JButton btn_chooseFile = new JButton("Choose");
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
		btn_chooseFile.setBounds(340, 10, 89, 23);
		panel_splitterPane.add(btn_chooseFile);

		JLabel lbl_primeP = new JLabel("Prime:");
		lbl_primeP.setBounds(32, 80, 89, 14);
		panel_splitterPane.add(lbl_primeP);

		JLabel lbl_alpha = new JLabel("Alpha:");
		lbl_alpha.setBounds(32, 105, 89, 14);
		panel_splitterPane.add(lbl_alpha);

		JLabel lbl_prfKey = new JLabel("PRF Key:");
		lbl_prfKey.setBounds(32, 130, 89, 14);
		panel_splitterPane.add(lbl_prfKey);

		tb_primeP = new JTextField();
		tb_primeP.setEditable(false);
		tb_primeP.setBounds(127, 77, 131, 20);
		panel_splitterPane.add(tb_primeP);
		tb_primeP.setColumns(10);

		tb_alpha = new JTextField();
		tb_alpha.setEditable(false);
		tb_alpha.setColumns(10);
		tb_alpha.setBounds(127, 102, 131, 20);
		panel_splitterPane.add(tb_alpha);

		tb_prfKey = new JTextField();
		tb_prfKey.setEditable(false);
		tb_prfKey.setColumns(10);
		tb_prfKey.setBounds(127, 127, 131, 20);
		panel_splitterPane.add(tb_prfKey);

		JButton btn_split = new JButton("Split");
		btn_split.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Create lists to store the alphas
				List<Integer> listOfAlphas = new ArrayList<Integer>();
				
				// Generates large prime _p, random int _alpha and PRF key _k
				_p = generateLargePrime();
				_key = generatePRFKeyK();
				// _alpha = generateSecureRandomInteger(_p);

				// Displays them on the GUI
				tb_primeP.setText(String.valueOf(_p));
				tb_prfKey.setText(String.valueOf(_key));
				// tb_alpha.setText(String.valueOf(_alpha));

				// Erasure encodes the input file and returns a list of the InputStreams generated
				// InputStreams will become empty after using it in a method
				List<InputStream> encodedSlices = getErasureEncodedFileSlices(_inputFile);

				// Convert the InputStreams into Byte Arrays
				List<byte[]> encodedSliceBytes = convertInputStreamToByteArray(encodedSlices);
				
				List<byte[]> blockSectors = splitSliceIntoSectors(encodedSliceBytes.get(0));
				
				for (int i = 1; i <= blockSectors.size(); i++)
					_alphas.add(generateSecureRandomInteger(255));
				
				List<Long> listOfAuthenticators = new ArrayList<Long>();
				
				// Generate list of authenticators using byte arrays
				for (byte[] baa : encodedSliceBytes)
					listOfAuthenticators.add(calculateAuthenticationValue(baa));

				// Save file slices to disk
				saveFileSlicesToDisk(encodedSliceBytes);
				
				/*List<Long> listOfAuthenticators = new ArrayList<Long>();
				for (byte[] ba : encodedSliceBytes)
					listOfAuthenticators.add(calculateAuthenticationValue(ba));*/

				// Save authenticators to disk
				saveAuthenticatorsToDisk(listOfAuthenticators);
			}
		});
		btn_split.setBounds(302, 90, 89, 44);
		panel_splitterPane.add(btn_split);

		JButton btnGenerateChallenge = new JButton("Generate Challenge");
		btnGenerateChallenge.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				// Retrieve authenticators from disk
				List<Long> listOfAuthenticators = new ArrayList<Long>();
				listOfAuthenticators = retrieveAuthenticatorsFromDisk(_inputFile.getName());

				// Create challenge set of random coefficients
				List<Integer> Q = new ArrayList<Integer>();
				for (int i = 1; i <= listOfAuthenticators.size(); i++)
					Q.add(generateSecureRandomInteger(255));

				// Save index coefficient pairs to disk
				saveChallengeToDisk(Q);
			}
		});
		btnGenerateChallenge.setBounds(138, 179, 153, 29);
		panel_splitterPane.add(btnGenerateChallenge);

		JPanel panel_proverPane = new JPanel();
		tabbedPane.addTab("Prover", null, panel_proverPane, null);
		panel_proverPane.setLayout(null);

		tb_chalFilename = new JTextField();
		tb_chalFilename.setEditable(false);
		tb_chalFilename.setBounds(10, 11, 320, 20);
		panel_proverPane.add(tb_chalFilename);
		tb_chalFilename.setColumns(10);

		JButton btn_chooseChalFile = new JButton("Choose");
		btn_chooseChalFile.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				JFileChooser fc = new JFileChooser("C:\\Sample Files");
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Challenge File", "chal");
				fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fc.setFileFilter(filter);

				int validFile = fc.showOpenDialog(panel_contentPane);
				if (validFile == JFileChooser.APPROVE_OPTION)
				{
					_currentPath = fc.getCurrentDirectory().toString() + "\\";
					_challengeFile = fc.getSelectedFile();
					tb_chalFilename.setText(_challengeFile.getName());
				}
			}
		});
		btn_chooseChalFile.setBounds(340, 10, 89, 23);
		panel_proverPane.add(btn_chooseChalFile);

		JLabel lblNewLabel = new JLabel("Sigma:");
		lblNewLabel.setBounds(32, 87, 89, 14);
		panel_proverPane.add(lblNewLabel);

		JLabel lblMu = new JLabel("Mu:");
		lblMu.setBounds(32, 112, 89, 14);
		panel_proverPane.add(lblMu);

		tb_sigma = new JTextField();
		tb_sigma.setEditable(false);
		tb_sigma.setBounds(131, 84, 131, 20);
		panel_proverPane.add(tb_sigma);
		tb_sigma.setColumns(10);

		tb_mu = new JTextField();
		tb_mu.setEditable(false);
		tb_mu.setColumns(10);
		tb_mu.setBounds(131, 109, 131, 20);
		panel_proverPane.add(tb_mu);

		JButton btn_prove = new JButton("Prove!");
		btn_prove.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Get list of authenticators (1 of 3)
				List<Long> authenticators = retrieveAuthenticatorsFromDisk(_inputFile.getName());
				
				// Retrieve all the file slices corresponding to the authentication file
				List<File> listOfSlices = retrieveFileSlices(FilenameUtils.removeExtension(_challengeFile.getName()));
				
				// Convert file slices to byte arrays (2 of 3)
				List<byte[]> sliceBytes = new ArrayList<byte[]>();

				for (File f : listOfSlices)
				{
					FileInputStream fis = null;
					byte[] bytes = new byte[(int) f.length()];

					try
					{
						fis = new FileInputStream(f);
						fis.read(bytes);
						fis.close();
					} catch (Exception ex)
					{
						ex.printStackTrace();
					}
					sliceBytes.add(bytes);
				}
				
				// Retrieve the challenge coefficients from disk (3 of 3)
				List<Integer> Q = retrieveChallengeFromFile(_inputFile.getName());

				// Perform the calculation of sigma and mu
				List<Long> response = new ArrayList<Long>();
				try
				{
					// Calculating sigma
					long sigma = 0;

					for (int i = 0; i < authenticators.size(); i++)
						sigma += Q.get(i) * authenticators.get(i);

					// Add sigma to the response
					response.add(sigma);
					
					// Calculating mus
					
					for (int i = 0; i < sliceBytes.size(); i++)
					{
						long mu = 0;
						List<byte[]> sectors = splitSliceIntoSectors(sliceBytes.get(i));
						int coefficient = Q.get(i);
						for (byte[] ba : sectors)
						{
							for (byte b : ba)
								mu += coefficient * b;
						}
						
						// Add mus to the response
						response.add(mu);
					}
					
					/*for (int i = 0; i < Q.size(); i++)
					{
						byte[] ba = sliceBytes.get(i);
						int coefficient = Q.get(i);
						for (byte b : ba)
							mu += coefficient * b;
					}*/
					
					// Display sigma and mu on the program
					tb_sigma.setText(String.valueOf(sigma));
					//tb_mu.setText(String.valueOf(mu));

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}

				// Save the response to disk
				saveResponseToDisk(response);
			}
		});
		btn_prove.setBounds(302, 87, 89, 39);
		panel_proverPane.add(btn_prove);

		JPanel panel_verifierPane = new JPanel();
		tabbedPane.addTab("Verifier", null, panel_verifierPane, null);
		panel_verifierPane.setLayout(null);
		
		tb_respFilename = new JTextField();
		tb_respFilename.setEditable(false);
		tb_respFilename.setBounds(10, 11, 320, 20);
		panel_verifierPane.add(tb_respFilename);
		tb_respFilename.setColumns(10);
		
		JButton btn_chooseRespFile = new JButton("Choose");
		btn_chooseRespFile.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				JFileChooser fc = new JFileChooser("C:\\Sample Files");
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Response File", "resp");
				fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fc.setFileFilter(filter);

				int validFile = fc.showOpenDialog(panel_contentPane);
				if (validFile == JFileChooser.APPROVE_OPTION)
				{
					_currentPath = fc.getCurrentDirectory().toString() + "\\";
					_responseFile = fc.getSelectedFile();
					tb_respFilename.setText(_responseFile.getName());
				}
			}
		});
		btn_chooseRespFile.setBounds(340, 10, 89, 23);
		panel_verifierPane.add(btn_chooseRespFile);
		
		JButton btnVerify = new JButton("Verify");
		btnVerify.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Retrieve the response and Q from disk
				List<Long> response = retrieveResponseFromDisk(FilenameUtils.removeExtension(_responseFile.getName()));
				List<Integer> Q = retrieveChallengeFromFile(_inputFile.getName());
				
				// Calculate sigma to compare against the response
				long sigma = response.get(0);
				long mu = response.get(1);
				long keyXCoefficient = 0;

				for (int i : Q)
					keyXCoefficient += _key * i;

				long verifySigma = _alpha * mu + keyXCoefficient;

				// Display sigma, mu and the calcuated sigma on the program
				tb_fileSigma.setText(String.valueOf(sigma));
				tb_calcSigma.setText(String.valueOf(verifySigma));
				
				// Compare the two sigmas and display the respective message
				if (sigma == verifySigma)
					tb_status.setText("Verified!");
				else
					tb_status.setText("Not Verified!");
			}
		});
		btnVerify.setBounds(328, 97, 89, 45);
		panel_verifierPane.add(btnVerify);
		
		tb_fileSigma = new JTextField();
		tb_fileSigma.setEditable(false);
		tb_fileSigma.setBounds(166, 72, 127, 20);
		panel_verifierPane.add(tb_fileSigma);
		tb_fileSigma.setColumns(10);
		
		JLabel lbl_fileSigma = new JLabel("Sigma:");
		lbl_fileSigma.setBounds(32, 72, 82, 14);
		panel_verifierPane.add(lbl_fileSigma);
		
		JLabel lbl_calcSigma = new JLabel("Calculated Sigma:");
		lbl_calcSigma.setBounds(32, 108, 124, 14);
		panel_verifierPane.add(lbl_calcSigma);
		
		tb_calcSigma = new JTextField();
		tb_calcSigma.setEditable(false);
		tb_calcSigma.setColumns(10);
		tb_calcSigma.setBounds(166, 108, 127, 20);
		panel_verifierPane.add(tb_calcSigma);
		
		JLabel lbl_status = new JLabel("Status:");
		lbl_status.setBounds(32, 142, 82, 14);
		panel_verifierPane.add(lbl_status);
		
		tb_status = new JTextField();
		tb_status.setFont(new Font("Tahoma", Font.BOLD, 11));
		tb_status.setForeground(Color.RED);
		tb_status.setEditable(false);
		tb_status.setColumns(10);
		tb_status.setBounds(166, 142, 127, 20);
		panel_verifierPane.add(tb_status);
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////// SPLITTER
	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private List<InputStream> getErasureEncodedFileSlices(File inFile)
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
		// System.out.println("Matrix: " + matrix);

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
			// System.out.println("Output Segments : " + lsSegmented.size());

			lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0; // byte can contain value from -128 to 127

			// Loop through List of InputStreams and add sequence number to each
			// element, starting from 0
			for (InputStream inputStream : lsSegmented)
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));

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

	/**
	 * Takes a list of InputStreams and convert them into byte arrays, as the Input Streams clear after they are read
	 * @param listOfInputStreams
	 * @return List of byte arrays
	 */
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
					buffer.write(oneByte);

			} catch (Exception ex)
			{
				ex.printStackTrace();
			}
			listOfByteArrays.add(buffer.toByteArray());
		}

		return listOfByteArrays;
	}
	
	/**
	 * Splits a slice into sectors of a fixed size (1KB)
	 * @param ba The input byte array to be split
	 * @return Returns a list of sectors
	 */
	private List<byte[]> splitSliceIntoSectors(byte[] ba)
	{
		List<byte[]> listOfSectors = new ArrayList<byte[]>();
		
		int start = 0;
		int sizeOfSector = 1024; // 1KB
        
        // Divide slice into sectors
        while (true)
        {
        	byte[] buffer = new byte[sizeOfSector];
        	if (start + sizeOfSector > ba.length) // If the buffer is not full (last sector)
        	{
        		System.arraycopy(ba, start, buffer, 0, ba.length - start);
        		listOfSectors.add(buffer);
        		break;
        	}
        	else // If the buffer is full
        	{
        		System.arraycopy(ba, start, buffer, 0, sizeOfSector);
        		listOfSectors.add(buffer);
        		start += sizeOfSector;
        	}
        }
        
        /*int ii = 0;
        for (byte[] baa : listOfSectors)
        	System.out.println("Sector " + ii++ + " size: " + baa.length);*/
        
        return listOfSectors;
	}
	
	/**
	 * Generates a large prime using a couple of SecureRandom instances
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

			max = rand1.nextInt((9999 - 1000 + 1) + 1000);

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
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		return largestPrime;
	}

	/**
	 * Generates a secure random value between 0 and bound
	 * @param bound The upper limit
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
	 * Generates a random integer to be used as PRF key k
	 * @return Returns generated integer, PRF key k
	 */
	private int generatePRFKeyK()
	{
		Random rand = new Random();
		return (int) (rand.nextDouble() * 10000);
	}
	
	/**
	 * Saves the individual file slices to disk
	 * @param loba The list of byte arrays to be saved
	 */
	private void saveFileSlicesToDisk(List<byte[]> loba)
	{
		try
		{
			int counter = 0;
			for (byte[] ba : loba)
			{
				ByteArrayInputStream bais = new ByteArrayInputStream(ba);
				FileOutputStream outputFile = new FileOutputStream(_currentPath + tb_filename.getText() + ".s" + counter++);

				int d;
				while ((d = bais.read()) != -1)
					outputFile.write(d);

				outputFile.close();
			}
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	 * Calculates the authentication value for a given slice of the file
	 * @param ba A byte array slice of a file
	 * @return Returns the generated authentication value
	 */
	private long calculateAuthenticationValue(byte[] ba)
	{
		List<byte[]> sectors = splitSliceIntoSectors(ba);
		
		long authenticator;
		long blockXAlpha = 0;
		
		try
		{
			for (int i = 0; i < sectors.size(); i++)
			{
				byte[] baa = sectors.get(i);
				int alpha = _alphas.get(i);
				
				for (byte b : baa)
					blockXAlpha += alpha * b;
			}
			
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		authenticator = _key + blockXAlpha;
		return authenticator;
	}	

	/**
	 * Save the authenticators to a comma delimited file
	 * @param listOfAuthenticators
	 */
	private void saveAuthenticatorsToDisk(List<Long> listOfAuthenticators)
	{
		try
		{
			PrintWriter writer = new PrintWriter(_currentPath + tb_filename.getText() + ".auth", "UTF-8");
			for (int i = 0; i < listOfAuthenticators.size(); i++)
			{
				if (listOfAuthenticators.get(i) != listOfAuthenticators.get(listOfAuthenticators.size() - 1))
					writer.print(listOfAuthenticators.get(i) + ",");
				else if (listOfAuthenticators.get(i) == listOfAuthenticators.get(listOfAuthenticators.size() - 1))
					writer.print(listOfAuthenticators.get(i));
			}
			writer.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	/**
	 * Save the coefficients to a comma delimited file
	 * @param Q The list of coefficients
	 */
	private void saveChallengeToDisk(List<Integer> Q)
	{
		try
		{
			PrintWriter writer = new PrintWriter(_currentPath + tb_filename.getText() + ".chal", "UTF-8");
			for (int i = 0; i < Q.size(); i++)
			{
				if (Q.get(i) != Q.get(Q.size() - 1))
					writer.print(Q.get(i) + ",");
				else if (Q.get(i) == Q.get(Q.size() - 1))
					writer.print(Q.get(i));
			}
			writer.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////// PROVER
	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Retrieves the file slices corresponding to the authenticator selected
	 * @param filename The name of the file to be retrieved
	 * @return Returns a list of the file slices corresponding to the authenticator selected
	 */
	private List<File> retrieveFileSlices(String filename)
	{
		List<File> listOfSlices = new ArrayList<File>();
		int counter = 0;
		while (true)
		{
			File f = new File(_currentPath + filename + ".s" + counter);
			if (f.exists() && !f.isDirectory())
			{
				listOfSlices.add(f);
				counter++;
			} else
				break;
		}

		return listOfSlices;
	}

	/**
	 * Retrieves the authentication values from a file on disk
	 * @param filename The name of the file to be retrieved
	 * @return Returns a list of authenticators
	 */
	private List<Long> retrieveAuthenticatorsFromDisk(String filename)
	{
		List<Long> listOfAuthenticators = new ArrayList<Long>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".auth"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					listOfAuthenticators.add(Long.parseLong(str, 10));
			}
			br.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return listOfAuthenticators;
	}

	/**
	 * Retrieves the verifier's challenge from disk
	 * @param filename The name of the challenge file
	 * @return Returns a list containing the verifier's challenge (list of coefficients)
	 */
	private List<Integer> retrieveChallengeFromFile(String filename)
	{
		List<Integer> listOfCoeffients = new ArrayList<Integer>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".chal"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					listOfCoeffients.add(Integer.parseInt(str, 10));
			}
			br.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return listOfCoeffients;
	}
	
	/**
	 * Saves the prover's response to disk
	 * @param response The generated prover's response (sigma & mu)
	 */
	private void saveResponseToDisk(List<Long> response)
	{
		try
		{
			PrintWriter writer = new PrintWriter(_currentPath + tb_filename.getText() + ".resp", "UTF-8");
			for (int i = 0; i < response.size(); i++)
			{
				if (response.get(i) != response.get(response.size() - 1))
					writer.print(response.get(i) + ",");
				else if (response.get(i) == response.get(response.size() - 1))
					writer.print(response.get(i));
			}
			writer.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////// VERIFIER
	// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Retrieves the prover's response from disk
	 * @param filename The name of the response file
	 * @return A list containing the prover's response (sigma & mu)
	 */
	private List<Long> retrieveResponseFromDisk(String filename)
	{
		List<Long> response = new ArrayList<Long>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".resp"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					response.add(Long.parseLong(str, 10));
			}
			br.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return response;
	}
}
