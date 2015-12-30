package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.RabinImpl2;
import sg.edu.nyp.sit.svds.client.ida.Util;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.RowSpec;

public class MainWindow extends JFrame
{
	private JPanel panel_contentPane;
	private JTextField tb_filename;
	private File inputFile; // The input file
	private String currentPath; // The path of the directory the user is currently in
	private int p = generateLargePrime(); // large prime

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
					inputFile = fc.getSelectedFile();
					tb_filename.setText(inputFile.getName());
					currentPath = fc.getCurrentDirectory().toString() + "\\";
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
					//splitAndCombineFile();
					
					// Erasure encodes the input file and returns a list of the InputStreams generated
					List<InputStream> encodedSlices = getErasureEncodedFileSlices(inputFile);
					
					// Prints the contents of each InputStream to console
					int count = 1;
					for (InputStream is : encodedSlices)
					{
						System.out.println("Stream " + count++ + ": ");
						int oneByte;
						while ((oneByte = is.read()) != -1) {
							System.out.write(oneByte);
						}
						System.out.println();
						System.out.flush();
					}
					
					// TODO: Generate a random integer, alpha, for use during creation of authentication values
					int alpha = generateAlpha(p);
					
					// TODO: Generate a pseudo random function key, k
					
					
					
				} catch (IOException ex)
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
		info.setDataSize(inputFile.length());
		
		FileInputStream fileInputStream;

		try
		{
			fileInputStream = new FileInputStream(inputFile);

			// Calculate time taken to split the file
			long startTime = System.currentTimeMillis();
			List<InputStream> lsSegmented = iid.split(fileInputStream, info);
			long endTime = System.currentTimeMillis();
			System.out.println("Total time taken = " + (endTime - startTime));
			
			System.out.println("Output Segments :" + lsSegmented.size());

			List<InputStream> lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0;

			// Loop through List of InputStreams and add sequence number to each element, starting from 0
			for (InputStream inputStream : lsSegmented)
			{
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));
			}

			// Rearrange InputStreams
			List<InputStream> lsShuffle = new ArrayList<InputStream>();
			
//			lsShuffle.add(lsSegmentedWithId.get(0)); // random
//			lsShuffle.add(lsSegmentedWithId.get(1));
//			lsShuffle.add(lsSegmentedWithId.get(2));
//			lsShuffle.add(lsSegmentedWithId.get(13)); // random
//			lsShuffle.add(lsSegmentedWithId.get(4));
//			lsShuffle.add(lsSegmentedWithId.get(5));
//			lsShuffle.add(lsSegmentedWithId.get(6));
//			lsShuffle.add(lsSegmentedWithId.get(12));
//			lsShuffle.add(lsSegmentedWithId.get(8));
//			lsShuffle.add(lsSegmentedWithId.get(9));
//			lsShuffle.add(lsSegmentedWithId.get(10));
//			lsShuffle.add(lsSegmentedWithId.get(11));
			
			// Pick random elements of colSize (e.g. 12) from lsSegmentedWithId and add them to lsShuffle
			lsShuffle = PickRandomItems(lsSegmentedWithId, colSize);
			
			//TODO Create a method to output all file slices to disk for visualization purposes
			/*int e = 0, dd;
			for (InputStream is : lsShuffle)
			{
				FileOutputStream os = new FileOutputStream(currentPath + "\\Slices\\slice" + e++ + "_" + tb_filename.getText());
				while ((dd = is.read()) != -1)
					os.write(dd);
				os.close();
			}*/
			
			// Combine elements of lsShuffle into a InputStream using the IdaInfo variable, info
			// Requires each segment in List to have id, otherwise a OutOfBounds exception will occur
			InputStream isCombined = iid.combine(lsShuffle, info);

			System.out.println("Current path: " + currentPath + tb_filename.getText());
			FileOutputStream outputFile = new FileOutputStream(currentPath + "combined_" + tb_filename.getText());

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
	//private void erasureEncodeFile(File inFile)
	{
//		int rowSize = 15; // Total number of blocks
//		int colSize = 12; // Number of blocks needed to reconstruct the data (Quorum)
		
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

			// Loop through List of InputStreams and add sequence number to each element, starting from 0
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
		//return lsSegmented;
	}
	
	/**
	 * Gets randomly chosen n number of items from a list
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
			
			max = rand1.nextInt((999999-100000+1)+100000);
			
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
//			System.out.println("Max: " + max);
//			System.out.println("Largest Prime: " + largestPrime);
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		return largestPrime;
	}
	
	/**
	 * Generates a value between 0 and max
	 * 
	 * @param max The upper limit of the range of values to choose from
	 * @return Returns a random value as alpha
	 */
	private int generateAlpha(int max)
	{
		int result = 0;
		
		try
		{
			SecureRandom randSeed = SecureRandom.getInstance("SHA1PRNG", "SUN");
			randSeed.nextBytes(new byte[128]);
			byte[] seed = randSeed.generateSeed(10);
			
			SecureRandom rand1 = SecureRandom.getInstance("SHA1PRNG", "SUN");
			rand1.setSeed(seed);
			
			result = rand1.nextInt(max);
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
		
		return result;
	}
}
