package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import java.awt.GridLayout;
import java.awt.GridBagLayout;

import javax.swing.JTextField;

import java.awt.GridBagConstraints;

import javax.swing.JButton;

import sg.edu.nyp.sit.svds.client.ida.IInfoDispersal;
import sg.edu.nyp.sit.svds.client.ida.RabinImpl2;
import sg.edu.nyp.sit.svds.client.ida.Util;
import sg.edu.nyp.sit.svds.exception.IDAException;
import sg.edu.nyp.sit.svds.metadata.IdaInfo;

import java.awt.Insets;

import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.RowSpec;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;

public class MainWindow extends JFrame
{
	private JPanel panel_contentPane;
	private JTextField tb_filename;
	private File inputFile;
	private String currentPath;

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
				splitFile();
			}
		});
		btnSplit.setBounds(175, 89, 84, 33);
		panel_manipulationControls.add(btnSplit);
	}

	public void splitFile()
	{
		int rowSize = 15;
		int colSize = 12;

		int[][] mat = Util.generateIndependenceMatrix(rowSize, colSize);
		StringBuffer sb = new StringBuffer();

		// convert the matrix into a string
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

		IInfoDispersal iid = new RabinImpl2();
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
			System.out.println("Output Segments (" + lsSegmented.size() + "):");

			List<InputStream> lsSegmentedWithId = new ArrayList<InputStream>();
			byte index = 0;

			// Needs to add sequence number to each sequence
			for (InputStream inputStream : lsSegmented)
			{
				lsSegmentedWithId.add(new SequenceInputStream(new ByteArrayInputStream(new byte[] { index++ }), inputStream));
			}

			// Rearrange InputStreams
			List<InputStream> lsShuffle = new ArrayList<InputStream>();
			lsShuffle.add(lsSegmentedWithId.get(0)); // random
			lsShuffle.add(lsSegmentedWithId.get(1));
			lsShuffle.add(lsSegmentedWithId.get(2));
			lsShuffle.add(lsSegmentedWithId.get(3)); // random
			lsShuffle.add(lsSegmentedWithId.get(4));
			lsShuffle.add(lsSegmentedWithId.get(5));
			lsShuffle.add(lsSegmentedWithId.get(6));
			lsShuffle.add(lsSegmentedWithId.get(7));
			lsShuffle.add(lsSegmentedWithId.get(8));
			lsShuffle.add(lsSegmentedWithId.get(9));
			lsShuffle.add(lsSegmentedWithId.get(10));
			lsShuffle.add(lsSegmentedWithId.get(11));

			InputStream isCombined = iid.combine(lsShuffle, info);

			System.out.println("Current path: " + currentPath + tb_filename.getText());
			FileOutputStream outputFile = new FileOutputStream(currentPath + "combined_" + tb_filename.getText());

			int d;
			while ((d = isCombined.read()) != -1)
			{
				// System.out.println(d);
				outputFile.write(d);
			}
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
}
