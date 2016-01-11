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

	private int _p; // large prime
	private int _alpha; // random integer within large prime p
	private int _key; // random integer generated using a PRF
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
					// Generates large prime _p, random int _alpha and PRF key _k
					_p = generateLargePrime();
					_alpha = generateSecureRandomInteger(_p);
					_key = generatePRFKeyK();

					// Displays them on the GUI
					tb_prime.setText(String.valueOf(_p));
					tb_alpha.setText(String.valueOf(_alpha));
					tb_prfkey.setText(String.valueOf(_key));

					// Erasure encodes the input file and returns a list of the InputStreams generated
					// InputStreams will become empty after using it in a method
					List<InputStream> encodedSlices = getErasureEncodedFileSlices(_inputFile);

					// Convert the InputStreams into Byte Arrays
					List<byte[]> encodedSliceBytes = convertInputStreamToByteArray(encodedSlices);

					// Generate list of authenticators using byte arrays
					List<Long> listOfAuthenticators = new ArrayList<Long>();
					for (byte[] ba : encodedSliceBytes)
						listOfAuthenticators.add(calculateAuthenticationValue(ba));

					// Create challenge set of random coefficients
					List<Integer> Q = new ArrayList<Integer>();
					for (int i = 1; i <= listOfAuthenticators.size(); i++)
						Q.add(generateSecureRandomInteger(255));

					// Sends Q and file blocks to the prover. Get the response back.
					List<Long> response = prover(Q, encodedSliceBytes, listOfAuthenticators);

					// Determines if the response is verified and displays result on GUI
					boolean verified = verifier(response, Q);
					if (verified)
						tb_result.setText("Verified!");
					else
						tb_result.setText("Not Verified!");

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
	 * 
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
	 * 
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
	 * 
	 * @return Returns generated integer, PRF key k
	 */
	private int generatePRFKeyK()
	{
		Random rand = new Random();
		return (int) (rand.nextDouble() * 10000);
	}

	/**
	 * Calculates the authentication value for a given block of the input file
	 * 
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
	 * Accepts Q, encodedSliceBytes and list of authenticators and calculates sigma and mu to be returned to the verifier
	 * 
	 * @param Q The randomly generated index-coefficient challenge
	 * @param encodedSliceBytes The list of byte arrays that contain the file slices
	 * @param listOfAuthenticators A list of authenticators used in calculating sigma
	 * @return A list that contains sigma and mu to be accepted by the verifier
	 */
	private List<Long> prover(List<Integer> Q, List<byte[]> encodedSliceBytes, List<Long> listOfAuthenticators)
	{
		// System.out.println("EncodedSlices size: " + encodedSliceBytes.size());
		List<Long> response = new ArrayList<Long>();
		try
		{
			// Calculate sigma as part of the response to be sent back to the verifier
			long sigma = 0;

			for (int i = 0; i < listOfAuthenticators.size(); i++)
				sigma += Q.get(i) * listOfAuthenticators.get(i);

			/* ///////////////////////////////////////////////////////////////////////////// */

			// Calculate mu as part of the response to be sent back to the verifier
			long mu = 0;

			for (int i = 0; i < Q.size(); i++)
			{
				byte[] ba = encodedSliceBytes.get(i);
				int coefficient = Q.get(i);
				for (byte b : ba)
					mu += coefficient * b;
			}

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
	 * Accepts the response from the Prover (sigma & mu), calculate verifySigma and compare them
	 * 
	 * @param response List that contains sigma and mu
	 * @param Q The list of coefficients used to calculate verifySigma
	 * @return Returns a bool to determine whether the sigmas match
	 */
	private boolean verifier(List<Long> response, List<Integer> Q)
	{
		long sigma = response.get(0);
		long mu = response.get(1);
		long keyXCoefficient = 0;

		for (int i : Q)
			keyXCoefficient += _key * i;

		long verifySigma = _alpha * mu + keyXCoefficient;

		tb_sigma.setText(String.valueOf(sigma));
		tb_calculatedSigma.setText(String.valueOf(verifySigma));

		if (sigma == verifySigma)
			return true;
		else
			return false;
	}
}
