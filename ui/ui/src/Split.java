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
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

import java.awt.Color;
import java.awt.Font;
import javax.swing.SwingConstants;
import javax.swing.JTextArea;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

public class Split extends JFrame
{
	private File _inputFile; // The input file to be split
	private File _challengeFile; // The input challenge file
	private File _responseFile; // The input response file
	private String _currentPath; // The path of the directory the user is currently in

	private int _p; // large prime
	// list of random integers within large prime p
	private int _key; // random integer generated using a PRF
	
	private long _inputFileSize;
	private int _noOfSectors;
	private int _sectorSize = 1024; // 1KB

	private JPanel panel_contentPane;
	private JTextField tb_filename;
	private JTextField tb_primeP;
	private JTextField tb_prfKey;
	private JTextField tb_chalFilename;
	private JTextField tb_sigma;
	private JTextField tb_respFilename;
	private JTextField tb_fileSigma;
	private JTextField tb_calcSigma;
	private JTextField tb_status;
	private JTextField tb_noOfSlices;
	private JTextField tb_sectorsPerSlice;
	private JTextArea ta_alphas;
	private JTextArea ta_mu;

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
					
					// Record the size of the input file
					_inputFileSize = _inputFile.length();
					System.out.println("Input file size: " + _inputFileSize);
					
					// Generates large prime _p
					_p = generateLargePrime();
					
					// Displays P and Key on the GUI
					tb_primeP.setText(String.valueOf(_p));
				}
			}
		});
		btn_chooseFile.setBounds(340, 10, 89, 23);
		panel_splitterPane.add(btn_chooseFile);

		JLabel lbl_primeP = new JLabel("Prime:");
		lbl_primeP.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_primeP.setBounds(10, 55, 82, 14);
		panel_splitterPane.add(lbl_primeP);

		JLabel lbl_alpha = new JLabel("Alphas:");
		lbl_alpha.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_alpha.setBounds(22, 148, 70, 14);
		panel_splitterPane.add(lbl_alpha);

		JLabel lbl_prfKey = new JLabel("PRF Key:");
		lbl_prfKey.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_prfKey.setBounds(213, 55, 70, 14);
		panel_splitterPane.add(lbl_prfKey);

		tb_primeP = new JTextField();
		tb_primeP.setEditable(false);
		tb_primeP.setBounds(102, 55, 83, 20);
		panel_splitterPane.add(tb_primeP);
		tb_primeP.setColumns(10);

		tb_prfKey = new JTextField();
		tb_prfKey.setEditable(false);
		tb_prfKey.setColumns(10);
		tb_prfKey.setBounds(293, 52, 72, 20);
		panel_splitterPane.add(tb_prfKey);

		JButton btn_split = new JButton("Split");
		btn_split.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Generate PRF key
				_key = generatePRFKeyK();
				
				// Display PRF key on GUI
				tb_prfKey.setText(String.valueOf(_key));
				
				// Get erasure encoded byte arrays from input file
				List<byte[]> listOfEncodedSliceBytes = getErasureEncodedFileSlices(_inputFile);
				
				_noOfSectors = splitSliceIntoSectors(listOfEncodedSliceBytes.get(0)).size();
				
				// Displays no. of slices & sectors per slice on GUI
				tb_noOfSlices.setText(String.valueOf(listOfEncodedSliceBytes.size()));
				tb_sectorsPerSlice.setText(String.valueOf(_noOfSectors));
				
				// Clear the list of alphas before adding
				List<Integer> _alphas = new ArrayList<Integer>();
				//_alphas.clear();
				for (int i = 0; i < _noOfSectors; i++)
					_alphas.add(generateSecureRandomInteger(_p));
				
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < _alphas.size(); i++)
				{
					if (_alphas.get(i) != _alphas.get(_alphas.size() - 1))
						sb.append(_alphas.get(i) + ", ");
					else if (_alphas.get(i) == _alphas.get(_alphas.size() - 1))
						sb.append(_alphas.get(i));
				}
				
				ta_alphas.setText(sb.toString());
				
				List<BigInteger> listOfAuthenticators = calculateAuthenticationValues(listOfEncodedSliceBytes, _alphas);

				// Create t and add PRF key and alphas to it
				List<Integer> t = new ArrayList<Integer>();
				t.add(_key);
				for (int i : _alphas)
					t.add(i);
					
				// Save file slices to disk
				saveFileSlicesToDisk(listOfEncodedSliceBytes);

				// Save authenticators to disk
				saveAuthenticatorsToDisk(listOfAuthenticators);
				
				// Save t to disk
				saveTToDisk(t);
			}
		});
		btn_split.setBounds(96, 203, 89, 29);
		panel_splitterPane.add(btn_split);

		JButton btnGenerateChallenge = new JButton("Generate Challenge");
		btnGenerateChallenge.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent arg0)
			{
				// Retrieve authenticators from disk
				List<BigInteger> listOfAuthenticators = new ArrayList<BigInteger>();
				listOfAuthenticators = retrieveAuthenticatorsFromDisk(_inputFile.getName());

				// Create challenge set of random coefficients
				List<Integer> Q = new ArrayList<Integer>();
				for (int i = 0; i < listOfAuthenticators.size(); i++)
					Q.add(generateSecureRandomInteger(255));

				// Save index coefficient pairs to disk
				saveChallengeToDisk(Q);
			}
		});
		btnGenerateChallenge.setBounds(217, 203, 153, 29);
		panel_splitterPane.add(btnGenerateChallenge);
		
		JLabel lbl_noOfSlices = new JLabel("File Slices:");
		lbl_noOfSlices.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_noOfSlices.setBounds(10, 90, 82, 14);
		panel_splitterPane.add(lbl_noOfSlices);
		
		tb_noOfSlices = new JTextField();
		tb_noOfSlices.setEditable(false);
		tb_noOfSlices.setColumns(10);
		tb_noOfSlices.setBounds(102, 87, 62, 20);
		panel_splitterPane.add(tb_noOfSlices);
		
		JLabel lbl_sectorsPerSlice = new JLabel("Sectors per slice:");
		lbl_sectorsPerSlice.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_sectorsPerSlice.setBounds(174, 90, 111, 14);
		panel_splitterPane.add(lbl_sectorsPerSlice);
		
		tb_sectorsPerSlice = new JTextField();
		tb_sectorsPerSlice.setEditable(false);
		tb_sectorsPerSlice.setColumns(10);
		tb_sectorsPerSlice.setBounds(293, 86, 54, 20);
		panel_splitterPane.add(tb_sectorsPerSlice);
		
		ta_alphas = new JTextArea();
		ta_alphas.setEditable(false);
		ta_alphas.setLineWrap(true);
		//ta_alphas.setBounds(103, 118, 82, 74);
		//panel_splitterPane.add(ta_alphas);
		
		JScrollPane sp_alphas = new JScrollPane();
		sp_alphas.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		sp_alphas.setBounds(102, 118, 245, 74);
		panel_splitterPane.add(sp_alphas);
		sp_alphas.setViewportView(ta_alphas);

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

		JLabel lbl_sigma = new JLabel("Sigma:");
		lbl_sigma.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_sigma.setBounds(29, 60, 61, 14);
		panel_proverPane.add(lbl_sigma);

		JLabel lbl_mu = new JLabel("Mu:");
		lbl_mu.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_mu.setBounds(29, 125, 61, 14);
		panel_proverPane.add(lbl_mu);

		tb_sigma = new JTextField();
		tb_sigma.setEditable(false);
		tb_sigma.setBounds(111, 60, 131, 20);
		panel_proverPane.add(tb_sigma);
		tb_sigma.setColumns(10);

		JButton btn_prove = new JButton("Prove!");
		btn_prove.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Get list of authenticators (1 of 3)
				List<BigInteger> authenticators = retrieveAuthenticatorsFromDisk(FilenameUtils.removeExtension(_challengeFile.getName()));
				
				List<Integer> t = retrieveTFromFile(FilenameUtils.removeExtension(_challengeFile.getName()));
				
				// Retrieve all the file slices corresponding to the authentication file
				List<File> listOfSlices = retrieveFileSlices(FilenameUtils.removeExtension(_challengeFile.getName()));
				
				// Convert file slices to byte arrays (2 of 3)
				List<byte[]> listOfSliceBytes = new ArrayList<byte[]>();

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
					listOfSliceBytes.add(bytes);
				}
				
				// Retrieve the challenge coefficients from disk (3 of 3)
				List<Integer> Q = retrieveChallengeFromFile(_inputFile.getName());

				// Perform the calculation of sigma and mu
				List<BigInteger> response = new ArrayList<BigInteger>();
				try
				{
					// Calculating sigma
					BigInteger sigma = BigInteger.valueOf(0);

					for (int i = 0; i < authenticators.size(); i++)
					{
						BigInteger coefficient = BigInteger.valueOf(Q.get(i));
						coefficient = coefficient.multiply(authenticators.get(i));
						sigma = sigma.add(coefficient);
					}

					// Add sigma to the response
					response.add(sigma);
					
					// Calculating mus
					for (int i = 0; i < t.size() - 1; i++)
					{
						
						BigInteger mu = BigInteger.valueOf(0);
						for (int j = 0; j < Q.size(); j++)
						{
							int coefficient = Q.get(j);
							byte[] ba = splitSliceIntoSectors(listOfSliceBytes.get(j)).get(i);
							for (byte b : ba)
							{
								BigInteger co = BigInteger.valueOf(coefficient);
								co = co.multiply(BigInteger.valueOf(b));
								mu = mu.add(co);
							}
						}
						// Add mu to the response
						response.add(mu);
					}
					
					StringBuilder sb = new StringBuilder();
					for (int i = 1; i < response.size(); i++)
					{
						if (response.get(i) != response.get(response.size() - 1))
							sb.append(response.get(i) + ", ");
						else if (response.get(i) == response.get(response.size() - 1))
							sb.append(response.get(i));
					}
					
					// Display sigma and mu on the program
					tb_sigma.setText(String.valueOf(sigma));
					ta_mu.setText(sb.toString());

				} catch (Exception ex)
				{
					ex.printStackTrace();
				}

				// Save the response to disk
				saveResponseToDisk(response);
			}
		});
		btn_prove.setBounds(312, 100, 89, 39);
		panel_proverPane.add(btn_prove);
		
		ta_mu = new JTextArea();
		ta_mu.setLineWrap(true);
		ta_mu.setEditable(false);
		//ta_mu.setBounds(111, 94, 168, 67);
		//panel_proverPane.add(ta_mu);
		
		JScrollPane sp_mu = new JScrollPane();
		sp_mu.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		sp_mu.setBounds(111, 106, 162, 66);
		panel_proverPane.add(sp_mu);
		sp_mu.setViewportView(ta_mu);

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
				List<BigInteger> response = retrieveResponseFromDisk(FilenameUtils.removeExtension(_responseFile.getName()));
				List<Integer> Q = retrieveChallengeFromFile(FilenameUtils.removeExtension(_responseFile.getName()));
				List<Integer> T = retrieveTFromFile(FilenameUtils.removeExtension(_responseFile.getName()));
				List<Integer> _alphas = new ArrayList<Integer>();
				
				for (int i = 1; i < T.size(); i++)
					_alphas.add(T.get(i));
				
				// Retrieve sigma from file
				BigInteger sigma = response.get(0);
				
				// Retrieve mus from file
				List<BigInteger> listOfMus = new ArrayList<BigInteger>();
				for (int i = 1; i < response.size(); i++)
					listOfMus.add(response.get(i));
				
				// Calculate summation of coefficients x key
				BigInteger keyXCoefficient = BigInteger.valueOf(0);
				BigInteger key = BigInteger.valueOf(T.get(0));
				for (int i : Q)
				{
					key = key.multiply(BigInteger.valueOf(i));
					keyXCoefficient.add(key);
				}
				
				// Calculate summation of alphas x mus
				BigInteger alphasXMus = BigInteger.valueOf(0);
				for (int i = 0; i < _alphas.size(); i++)
				{
					BigInteger alpha = BigInteger.valueOf(_alphas.get(i));
					alpha = alpha.multiply(listOfMus.get(i));
					alphasXMus = alphasXMus.add(alpha);
				}
				
				BigInteger verifySigma = keyXCoefficient.add(alphasXMus);

				// Display sigma, mu and the calcuated sigma on the program
				tb_fileSigma.setText(String.valueOf(sigma));
				tb_calcSigma.setText(String.valueOf(verifySigma));
				
				// Compare the two sigmas and display the respective message
				if (sigma.equals(verifySigma))
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
		lbl_fileSigma.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_fileSigma.setBounds(47, 72, 82, 14);
		panel_verifierPane.add(lbl_fileSigma);
		
		JLabel lbl_calcSigma = new JLabel("Calculated Sigma:");
		lbl_calcSigma.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_calcSigma.setBounds(10, 108, 119, 14);
		panel_verifierPane.add(lbl_calcSigma);
		
		tb_calcSigma = new JTextField();
		tb_calcSigma.setEditable(false);
		tb_calcSigma.setColumns(10);
		tb_calcSigma.setBounds(166, 108, 127, 20);
		panel_verifierPane.add(tb_calcSigma);
		
		JLabel lbl_status = new JLabel("Status:");
		lbl_status.setHorizontalAlignment(SwingConstants.RIGHT);
		lbl_status.setBounds(47, 142, 82, 14);
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

	/**
	 * Erasure encode the input file and obtain a list of erasure encoded slices
	 * @param inFile The input file
	 * @return Returns a list of erasure encoded byte arrays
	 */
	private List<byte[]> getErasureEncodedFileSlices(File inFile)
	{
		// int rowSize = 15; 
		// int colSize = 12; 
		// (Quorum)

		int rowSize = 5; // Total number of blocks
		int colSize = 3; // Number of blocks needed to reconstruct the data

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

		// Declare an instance of the IDA algorithm
		IInfoDispersal iid = new RabinImpl2();

		// Declare an instance of the IdaInfo class and set its size
		IdaInfo info = new IdaInfo(rowSize, colSize, matrix);
		info.setDataSize(inFile.length()); // Size of the file, in bytes

		FileInputStream fileInputStream;
		List<InputStream> lsSegmented = null;
		List<InputStream> lsSegmentedWithId = null;
		List<byte[]> listOfByteArrays = new ArrayList<byte[]>();

		try
		{
			fileInputStream = new FileInputStream(inFile);

			// Split the input file into slices
			lsSegmented = iid.split(fileInputStream, info);

			// Loop through List of InputStreams and add sequence number to each element, starting from 0
			lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0;			
			for (InputStream inputStream : lsSegmented)
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));

			// Convert the InputStreams to byte arrays to prevent the data from disappearing upon first use
			for (InputStream is : lsSegmentedWithId)
			{
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();

				try
				{
					int oneByte;
					while ((oneByte = is.read()) != -1)
						buffer.write(oneByte);
				}
				catch (Exception ex)
				{
					ex.printStackTrace();
				}
				listOfByteArrays.add(buffer.toByteArray());
			}

		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (IDAException e)
		{
			e.printStackTrace();
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
        
        // Divide slice into sectors
        while (true)
        {
        	byte[] buffer = new byte[_sectorSize];
        	if (start + _sectorSize > ba.length) // If the buffer is not full (last sector)
        	{
        		System.arraycopy(ba, start, buffer, 0, ba.length - start);
        		listOfSectors.add(buffer);
        		break;
        	}
        	else // If the buffer is full
        	{
        		System.arraycopy(ba, start, buffer, 0, _sectorSize);
        		listOfSectors.add(buffer);
        		start += _sectorSize;
        	}
        }
        
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
	private List<BigInteger> calculateAuthenticationValues(List<byte[]> listOfEncodedSliceBytes, List<Integer> _alphas)
	{
		List<BigInteger> listOfAuthenticators = new ArrayList<BigInteger>();
		
		for (int i = 0; i < listOfEncodedSliceBytes.size(); i++)
		{
			List<byte[]> sectors = splitSliceIntoSectors(listOfEncodedSliceBytes.get(i));
			BigInteger authenticator = BigInteger.valueOf(0);
			
			try
			{
				for (int j = 0; j < sectors.size(); j++)
				{
					BigInteger sectorXAlpha = BigInteger.valueOf(0);
					byte[] baa = sectors.get(j);
					int alpha = _alphas.get(j);
					
					for (byte b : baa)
					{
						//sectorXAlpha += alpha * b;
						BigInteger alphaa = BigInteger.valueOf(alpha);
						alphaa = alphaa.multiply(BigInteger.valueOf(b));
						sectorXAlpha = sectorXAlpha.add(alphaa);
					}
					
					authenticator = authenticator.add(sectorXAlpha);
				}
				
			} catch (Exception ex)
			{
				ex.printStackTrace();
			}

			//long authenticator = _key + summation;
			authenticator.add(BigInteger.valueOf(_key));
			listOfAuthenticators.add(authenticator);
		}
		
		return listOfAuthenticators;
	}	

	/**
	 * Save the authenticators to a comma delimited file
	 * @param listOfAuthenticators
	 */
	private void saveAuthenticatorsToDisk(List<BigInteger> listOfAuthenticators)
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
	
	/**
	 * Save the PRF key and alphas to disk
	 * @param T The list of integers that contain the PRF key and alphas
	 */
	private void saveTToDisk(List<Integer> T)
	{
		try
		{
			PrintWriter writer = new PrintWriter(_currentPath + tb_filename.getText() + ".t", "UTF-8");
			for (int i = 0; i < T.size(); i++)
			{
				if (T.get(i) != T.get(T.size() - 1))
					writer.print(T.get(i) + ",");
				else if (T.get(i) == T.get(T.size() - 1))
					writer.print(T.get(i));
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
	 * Retrieves the file slices corresponding to the authentication file selected
	 * @param filename The name of the authentication file
	 * @return Returns a list of the file slices corresponding to the authentication file selected
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
	 * Retrieves the authenticators from a file on disk
	 * @param filename The name of the file to be retrieved
	 * @return Returns a list of authenticators
	 */
	private List<BigInteger> retrieveAuthenticatorsFromDisk(String filename)
	{
		List<BigInteger> listOfAuthenticators = new ArrayList<BigInteger>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".auth"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					listOfAuthenticators.add(new BigInteger(str));
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
	 * Retrieves the list containing the PRF key and alphas from disk
	 * @param filename The name of the T file
	 * @return Returns a list containing the PRF key and alphas
	 */
	private List<Integer> retrieveTFromFile(String filename)
	{
		List<Integer> listOfT = new ArrayList<Integer>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".t"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					listOfT.add(Integer.parseInt(str, 10));
			}
			br.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return listOfT;
	}
	
	/**
	 * Saves the prover's response to disk
	 * @param response The generated prover's response (sigma & mus)
	 */
	private void saveResponseToDisk(List<BigInteger> response)
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
	 * @return A BigInteger list containing the prover's response (sigma & mus)
	 */
	private List<BigInteger> retrieveResponseFromDisk(String filename)
	{
		List<BigInteger> response = new ArrayList<BigInteger>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(_currentPath + filename + ".resp"));
			String line = null;

			while ((line = br.readLine()) != null)
			{
				String[] values = line.split(",");
				for (String str : values)
					response.add(new BigInteger(str));
			}
			br.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
		return response;
	}
}
