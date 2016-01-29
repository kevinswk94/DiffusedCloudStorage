package ui.src;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

public class Prover extends JFrame
{
	private File _inputFile; // The input file
	private String _currentPath; // The path of the directory the user is currently in
	
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
					Prover frame = new Prover();
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
	public Prover()
	{
		setResizable(false);
		setTitle("Prover");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 423, 300);
		panel_contentPane = new JPanel();
		panel_contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		panel_contentPane.setLayout(new BorderLayout(0, 0));
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
	}

}
