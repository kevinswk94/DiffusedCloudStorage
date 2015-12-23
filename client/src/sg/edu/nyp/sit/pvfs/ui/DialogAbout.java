package sg.edu.nyp.sit.pvfs.ui;

import java.io.IOException;

import javax.swing.ImageIcon;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.svds.Resources;

public class DialogAbout extends javax.swing.JDialog {
	public static final long serialVersionUID = 1L;

	public DialogAbout(java.awt.Frame parent) {
        super(parent);
        initComponents();
    }
	
	private void initComponents() {
		javax.swing.JLabel lblTitle = new javax.swing.JLabel();
		javax.swing.JLabel lblVersion = new javax.swing.JLabel();
		javax.swing.JLabel lblVersionValue = new javax.swing.JLabel();
		javax.swing.JLabel lblAuthor = new javax.swing.JLabel();
		javax.swing.JLabel lblAuthorValue = new javax.swing.JLabel();
		javax.swing.JLabel lblDesc = new javax.swing.JLabel();
		javax.swing.JLabel img = new javax.swing.JLabel();
		
		setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("About " + Main.sysName); 
        setIconImage(Main.sysIcon);
        setModal(true);
        setName("aboutBox"); 
        setResizable(false);
        setLocationRelativeTo(null); 
        
        lblTitle.setFont(lblTitle.getFont().deriveFont(lblTitle.getFont().getStyle() | java.awt.Font.BOLD, lblTitle.getFont().getSize()+4));
        lblTitle.setText(Main.sysName); 
        lblTitle.setName("lblTitle"); 
        
        lblVersion.setFont(lblVersion.getFont().deriveFont(lblVersion.getFont().getStyle() | java.awt.Font.BOLD));
        lblVersion.setText("Version:"); 
        lblVersion.setName("lblVersion"); 

        lblVersionValue.setText("0.1"); 
        lblVersionValue.setName("lblVersionValue"); 

        lblAuthor.setFont(lblAuthor.getFont().deriveFont(lblAuthor.getFont().getStyle() | java.awt.Font.BOLD));
        lblAuthor.setText("Author:"); 
        lblAuthor.setName("lblAuthor"); 

        lblAuthorValue.setText("<html>School of Information Technology, NYP"); 
        lblAuthorValue.setName("lblAuthorValue"); 

        lblDesc.setText("<html>A simple application that allows you to access your personal files stored in the cloud.");
        lblDesc.setName("lblDesc");

        try{
	        img.setIcon(new ImageIcon(Resources.findFile("harddrive_big.png"), Main.sysName)); 
	        img.setName("img");
        }catch(IOException ex){
        	ex.printStackTrace();
        }

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(img)
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(lblTitle, javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(lblAuthor)
                        .addGap(18, 18, 18)
                        .addComponent(lblAuthorValue, javax.swing.GroupLayout.DEFAULT_SIZE, 225, Short.MAX_VALUE))
                    .addGroup(javax.swing.GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                        .addComponent(lblVersion)
                        .addGap(18, 18, 18)
                        .addComponent(lblVersionValue))
                    .addComponent(lblDesc, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 285, Short.MAX_VALUE))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(lblTitle)
                .addGap(18, 18, 18)
                .addComponent(lblDesc, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblVersion)
                    .addComponent(lblVersionValue))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblAuthor)
                    .addComponent(lblAuthorValue, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)))
            .addComponent(img, javax.swing.GroupLayout.PREFERRED_SIZE, 175, javax.swing.GroupLayout.PREFERRED_SIZE)
        );

        pack();
	}
}
