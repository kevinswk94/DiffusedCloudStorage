package sg.edu.nyp.sit.pvfs.ui;

import java.awt.Font;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;

import javax.swing.JOptionPane;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.SVDSException;

public class FormSettings extends javax.swing.JFrame {
	public static final long serialVersionUID = 2L;

	public FormSettings() {
		mt=MasterTableFactory.getNamespaceInstance();
		
        initComponents();
    }
	
	private javax.swing.JButton btnSave;
    private javax.swing.JButton btnSaveClose;
    private javax.swing.JCheckBox cbReqAuth;
    private javax.swing.JLabel lblDriveName;
    private javax.swing.JLabel lblReconnTimeout;
    private javax.swing.JLabel lblReconnTimeoutSub;
    private javax.swing.JTextField txtDriveName;
    private javax.swing.JTextField txtReconnTimeout;
    private javax.swing.JPasswordField txtPwd;
    private javax.swing.JPasswordField txtRePwd;
    private javax.swing.JLabel lblPwd;
    private javax.swing.JLabel lblRePwd;
    
    private void initComponents() {
        lblDriveName = new javax.swing.JLabel();
        txtDriveName = new javax.swing.JTextField();
        btnSave = new javax.swing.JButton();
        btnSaveClose = new javax.swing.JButton();
        lblReconnTimeout = new javax.swing.JLabel();
        txtReconnTimeout = new javax.swing.JTextField();
        lblReconnTimeoutSub = new javax.swing.JLabel();
        cbReqAuth = new javax.swing.JCheckBox();
        lblPwd = new javax.swing.JLabel();
        lblRePwd = new javax.swing.JLabel();
        txtPwd = new javax.swing.JPasswordField();
        txtRePwd = new javax.swing.JPasswordField();

        setDefaultCloseOperation(javax.swing.WindowConstants.HIDE_ON_CLOSE);
        setName("FormSettings");
        setTitle("Virtual Drive Settings");
        setIconImage(Main.sysIcon);

        setResizable(false);
        setLocationRelativeTo(null);
        
        addComponentListener( new ComponentListener() {
			@Override
			public void componentHidden(ComponentEvent e) {}

			@Override
			public void componentMoved(ComponentEvent e) {}

			@Override
			public void componentResized(ComponentEvent e) {}

			@Override
			public void componentShown(ComponentEvent e) {
				initValues();
			} 
        });

        lblDriveName.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblDriveName.setText("Drive Name:");
        lblDriveName.setName("lblDriveName");

        txtDriveName.setToolTipText("Enter the name for your virtual drive that will show "
        		+"up in the explorer window."); 
        txtDriveName.setName("txtDriveName");

        btnSave.setText("Save");
        btnSave.setName("btnSave");
        btnSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnSaveActionPerformed(evt);
            }
        });

        btnSaveClose.setText("Save & Close");
        btnSaveClose.setName("btnSaveClose");
        btnSaveClose.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnSaveCloseActionPerformed(evt);
            }
        });
        
        lblReconnTimeout.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblReconnTimeout.setText("Reconnection Timeout:");
        lblReconnTimeout.setName("lblReconnTimeout");

        txtReconnTimeout.setName("txtReconnTimeout");

        lblReconnTimeoutSub.setText("minute(s)");
        lblReconnTimeoutSub.setName("lblReconnTimeoutSub");

        cbReqAuth.setFont(new Font("Tahoma", Font.BOLD, 11));
        cbReqAuth.setText("Require Authentication");
        cbReqAuth.setName("cbReqAuth");
        //cbReqAuth.addItemListener(new ItemListener(){
        //	public void itemStateChanged(ItemEvent itemEvent) {
        //		boolean reqAuth=(itemEvent.getStateChange()==ItemEvent.SELECTED?true:false);
        //		enableDisablePwd(reqAuth);
        //	}
        //});
        cbReqAuth.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
               enableDisablePwd(cbReqAuth.isSelected());
            }
        });
        
        lblPwd.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblPwd.setText("Password:"); 
        lblPwd.setName("lblPwd"); 

        txtPwd.setName("txtPwd"); 

        lblRePwd.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblRePwd.setText("Confirm Password:"); 
        lblRePwd.setName("lblRePwd"); 

        txtRePwd.setName("txtRePwd"); 

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(lblDriveName)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 71, Short.MAX_VALUE)
                        .addComponent(txtDriveName, javax.swing.GroupLayout.PREFERRED_SIZE, 158, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(27, 27, 27))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(lblReconnTimeout)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(txtReconnTimeout, javax.swing.GroupLayout.PREFERRED_SIZE, 65, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(lblReconnTimeoutSub)
                        .addContainerGap(78, Short.MAX_VALUE))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(19, 19, 19)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addGroup(layout.createSequentialGroup()
                                .addComponent(lblPwd)
                                .addGap(63, 63, 63))
                            .addGroup(layout.createSequentialGroup()
                                .addComponent(lblRePwd)
                                .addGap(18, 18, 18)))
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                            .addComponent(txtPwd, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 138, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(txtRePwd, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 138, Short.MAX_VALUE))
                        .addContainerGap(45, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(btnSave)
                        .addGap(18, 18, 18)
                        .addComponent(btnSaveClose)
                        .addContainerGap(154, Short.MAX_VALUE))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(cbReqAuth)
                        .addContainerGap(167, Short.MAX_VALUE))))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblDriveName)
                    .addComponent(txtDriveName, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(lblReconnTimeoutSub)
                    .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(lblReconnTimeout)
                        .addComponent(txtReconnTimeout, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addGap(31, 31, 31)
                .addComponent(cbReqAuth)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(txtPwd, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(lblPwd))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(txtRePwd, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(lblRePwd))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 30, Short.MAX_VALUE)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnSave)
                    .addComponent(btnSaveClose))
                .addGap(19, 19, 19))
        );

        pack();
    }
    
    IMasterNamespaceTable mt=null;
    
    private void initValues(){
    	try {
			txtDriveName.setText(mt.getNamespace(Main.getUser()));
			txtReconnTimeout.setText((mt.getReconTimeout(Main.getUser())/(60*1000))+"");
			
			boolean reqAuth= mt.isAuthReq(Main.getUser());
			cbReqAuth.setSelected(reqAuth);
			enableDisablePwd(reqAuth);
		} catch (SVDSException ex) {
			// TODO Auto-generated catch block
			JOptionPane.showMessageDialog(rootPane, "Error retrieving settings.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
		}
    }
    
    private void enableDisablePwd(boolean reqAuth){
    	txtPwd.setEnabled(reqAuth);
		txtRePwd.setEditable(reqAuth);
		
		txtPwd.setText("");
		txtRePwd.setText("");
    }
    
    private boolean updateValues(){
    	try {
			mt.updateNamespace(txtDriveName.getText().trim(), Main.getUser());
			mt.updateReconTimeout(Long.parseLong(txtReconnTimeout.getText().trim()) * 60 * 1000, Main.getUser());
			
			mt.updateAuthReq(cbReqAuth.isSelected(), (txtPwd.getPassword().length==0?null:new String(txtPwd.getPassword())), Main.getUser());

			return true;
		} catch (SVDSException ex) {
			return false;
		}
    }
    
    private boolean validateFields(){
    	//validation
    	if(txtReconnTimeout.getText().trim().length()==0 || txtDriveName.getText().trim().length()==0){
    		JOptionPane.showMessageDialog(rootPane, "Values cannot be empty.",
    				Main.sysName, JOptionPane.ERROR_MESSAGE);
    		return false;
    	}
    	
    	long timeout;
    	try{
    		timeout=Long.parseLong(txtReconnTimeout.getText().trim());
    		
    		if(timeout<=0)
    			throw new NumberFormatException();
    	}catch(NumberFormatException ex){
    		JOptionPane.showMessageDialog(rootPane, "Invalid value for reconnection timeout.",
    				Main.sysName, JOptionPane.ERROR_MESSAGE);
    		return false;
    	}
    	
    	if((txtPwd.getPassword().length>0||txtRePwd.getPassword().length>0) &&
    			(!new String(txtPwd.getPassword()).equals(new String(txtRePwd.getPassword())))){
    		JOptionPane.showMessageDialog(rootPane, "Password is not the same.",
    				Main.sysName, JOptionPane.ERROR_MESSAGE);
    		return false;
    	}
    	
    	return true;
    }

    private void btnSaveActionPerformed(java.awt.event.ActionEvent evt) {  
    	if(!validateFields()) return;
    	
    	if(updateValues()){
    		Main.updateDirectLogonPwd(new String(txtPwd.getPassword()));
	    	JOptionPane.showMessageDialog(rootPane, "Settings updated successfully.",
	    			Main.sysName, JOptionPane.INFORMATION_MESSAGE);
	    	
	    	txtPwd.setText("");
	    	txtRePwd.setText("");
    	}else{
    		JOptionPane.showMessageDialog(rootPane, "Error updating settings.",
    				Main.sysName, JOptionPane.ERROR_MESSAGE);
    	}
    }                                       

    private void btnSaveCloseActionPerformed(java.awt.event.ActionEvent evt) {  
    	if(!validateFields()) return;
    	
    	if(updateValues()){
    		Main.updateDirectLogonPwd(new String(txtPwd.getPassword()));
	    	JOptionPane.showMessageDialog(rootPane, "Settings updated successfully.",
	    			Main.sysName, JOptionPane.INFORMATION_MESSAGE);
	    	this.setVisible(false);
    	}else{
    		JOptionPane.showMessageDialog(rootPane, "Error updating settings.",
    				Main.sysName, JOptionPane.ERROR_MESSAGE);
    	}
    }
}
