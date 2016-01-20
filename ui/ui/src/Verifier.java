package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.JButton;
import javax.swing.JTextField;

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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Verifier extends JFrame
{
	private File _inputFile; // The input file
	private String _currentPath; // The path of the directory the user is currently in
	
	private int _p; // large prime
	private int _alpha; // random integer within large prime p
	private int _key; // random integer generated using a PRF
	
	private JPanel panel_contentPane;
	private JTextField tb_filename;

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
					Verifier frame = new Verifier();
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
	public Verifier()
	{
		setResizable(false);
		setTitle("Verifier");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 423, 300);
		panel_contentPane = new JPanel();
		panel_contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(panel_contentPane);
		panel_contentPane.setLayout(null);

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
		btn_chooseFile.setBounds(332, 11, 75, 23);
		panel_contentPane.add(btn_chooseFile);

		tb_filename = new JTextField();
		tb_filename.setEditable(false);
		tb_filename.setBounds(10, 12, 300, 20);
		panel_contentPane.add(tb_filename);
		tb_filename.setColumns(10);

		JButton btn_store = new JButton("Store");
		btn_store.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				// Generates large prime _p, random int _alpha and PRF key _k
				_p = generateLargePrime();
				_alpha = generateSecureRandomInteger(_p);
				_key = generatePRFKeyK();
				
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
				
				saveAuthenticatorsToDisk(listOfAuthenticators);
			}
		});
		btn_store.setBounds(170, 116, 89, 49);
		panel_contentPane.add(btn_store);
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
}
