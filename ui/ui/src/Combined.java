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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

public class Combined extends JFrame
{
	private File _inputFile; // The input file
	private File _authenticatorFile; // The input file
	private String _currentPath; // The path of the directory the user is currently inv
	
	private int _p; // large prime
	private int _alpha; // random integer within large prime p
	private int _key; // random integer generated using a PRF
	
	private JPanel panel_contentPane;
	private JTextField tb_filename;
	private JTextField tb_primeP;
	private JTextField tb_alpha;
	private JTextField tb_prfKey;
	private JTextField tb_authFilename;
	private JTextField textField;
	private JTextField textField_1;

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
					Combined frame = new Combined();
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
	public Combined()
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
				// Generates large prime _p, random int _alpha and PRF key _k
				_p = generateLargePrime();
				_alpha = generateSecureRandomInteger(_p);
				_key = generatePRFKeyK();

				// Displays them on the GUI
				tb_primeP.setText(String.valueOf(_p));
				tb_alpha.setText(String.valueOf(_alpha));
				tb_prfKey.setText(String.valueOf(_key));

				// Erasure encodes the input file and returns a list of the InputStreams generated
				// InputStreams will become empty after using it in a method
				List<InputStream> encodedSlices = getErasureEncodedFileSlices(_inputFile);

				// Convert the InputStreams into Byte Arrays
				List<byte[]> encodedSliceBytes = convertInputStreamToByteArray(encodedSlices);
				
				// Save file slices to disk
				saveFileSlicesToDisk(encodedSliceBytes);
				
				// Generate list of authenticators using byte arrays
				List<Long> listOfAuthenticators = new ArrayList<Long>();
				for (byte[] ba : encodedSliceBytes)
					listOfAuthenticators.add(calculateAuthenticationValue(ba));
				
				// Save authenticators to disk
				saveAuthenticatorsToDisk(listOfAuthenticators);
				
				// TODO: Generate index coefficient pairs
				
				//TODO: Save index coefficient pairs to disk
			}
		});
		btn_split.setBounds(302, 90, 89, 44);
		panel_splitterPane.add(btn_split);

		JPanel panel_proverPane = new JPanel();
		tabbedPane.addTab("Prover", null, panel_proverPane, null);
		panel_proverPane.setLayout(null);
		
		tb_authFilename = new JTextField();
		tb_authFilename.setEditable(false);
		tb_authFilename.setBounds(10, 11, 320, 20);
		panel_proverPane.add(tb_authFilename);
		tb_authFilename.setColumns(10);
		
		JButton btn_chooseAuthFile = new JButton("Choose");
		btn_chooseAuthFile.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				JFileChooser fc = new JFileChooser("C:\\Sample Files");
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Authentication File", "auth");
				fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
				fc.setFileFilter(filter);

				int validFile = fc.showOpenDialog(panel_contentPane);
				if (validFile == JFileChooser.APPROVE_OPTION)
				{
					_currentPath = fc.getCurrentDirectory().toString() + "\\";
					_authenticatorFile = fc.getSelectedFile();
					tb_authFilename.setText(_authenticatorFile.getName());
				}
			}
		});
		btn_chooseAuthFile.setBounds(340, 10, 89, 23);
		panel_proverPane.add(btn_chooseAuthFile);

		JLabel lblNewLabel = new JLabel("Sigma:");
		lblNewLabel.setBounds(32, 87, 89, 14);
		panel_proverPane.add(lblNewLabel);

		JLabel lblMu = new JLabel("Mu:");
		lblMu.setBounds(32, 112, 89, 14);
		panel_proverPane.add(lblMu);

		textField = new JTextField();
		textField.setEditable(false);
		textField.setBounds(131, 84, 131, 20);
		panel_proverPane.add(textField);
		textField.setColumns(10);

		textField_1 = new JTextField();
		textField_1.setEditable(false);
		textField_1.setColumns(10);
		textField_1.setBounds(131, 109, 131, 20);
		panel_proverPane.add(textField_1);

		JButton btnNewButton = new JButton("Prove!");
		btnNewButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Retrieve all the file slices corresponding to the authentication file
				List<File> listOfSlices = retrieveFileSlices(FilenameUtils.removeExtension(_authenticatorFile.getName()));
				
				// Convert file slices to byte arrays
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
				
				// TODO: Perform the calculation of sigma and mu
				
				// TODO: Save sigma and mu to disk for the verifier to use
			}
		});
		btnNewButton.setBounds(302, 87, 89, 39);
		panel_proverPane.add(btnNewButton);

		JPanel panel_verifierPane = new JPanel();
		tabbedPane.addTab("Verifier", null, panel_verifierPane, null);
		panel_verifierPane.setLayout(null);
	}
	
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
	 * Calculates the authentication value for a given block of the input file
	 * @param ba A byte array block of the erasure encoded file
	 * @return Returns the generated authentication value
	 */
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
				if (listOfAuthenticators.get(i) != listOfAuthenticators.get(listOfAuthenticators.size() -1))
					writer.print(listOfAuthenticators.get(i) + ",");
				else if (listOfAuthenticators.get(i) == listOfAuthenticators.get(listOfAuthenticators.size() -1))
					writer.print(listOfAuthenticators.get(i));
			}
			writer.close();
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
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
			if(f.exists() && !f.isDirectory())
			{
				listOfSlices.add(f);
				counter++;
			}
			else
				break;
		}
		
		return listOfSlices;
	}
}
