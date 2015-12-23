package sg.edu.nyp.sit.pvfs.ui;

import java.awt.Font;

import javax.swing.JOptionPane;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.svds.client.filestore.impl.S3SliceStore;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileIOMode;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class DialogSliceStoresNewEdit extends javax.swing.JDialog  {
	public static final long serialVersionUID = 5L;
	
	private final FormSliceStores parent;
	
	public DialogSliceStoresNewEdit(FormSliceStores parent) {
		this.parent=parent;
		
        initComponents();
        
        mt=MasterTableFactory.getNamespaceInstance();
    }
	
	private javax.swing.JButton btnBack;
    private javax.swing.JButton btnSave;
    private javax.swing.JButton btnSaveClose;
    private javax.swing.JComboBox ddlMode;
    private javax.swing.JLabel lblActive;
    private javax.swing.JLabel lblAzureSecretAccessKey;
    private javax.swing.JLabel lblAzureStorageAcct;
    private javax.swing.JLabel lblHost;
    private javax.swing.JLabel lblId;
    private javax.swing.JLabel lblMode;
    private javax.swing.JLabel lblRestletHost;
    private javax.swing.JLabel lblRestletSSL;
    private javax.swing.JLabel lblS3AccessKey;
    private javax.swing.JLabel lblS3Bucket;
    private javax.swing.JLabel lblS3SecretAccessKey;
    private javax.swing.JPanel panelAzure;
    private javax.swing.JPanel panelRestlet;
    private javax.swing.JPanel panelS3;
    private javax.swing.JRadioButton rbActiveNo;
    private javax.swing.JRadioButton rbActiveYes;
    private javax.swing.JRadioButton rbRestletSSLOff;
    private javax.swing.JRadioButton rbRestletSSLOn;
    private javax.swing.ButtonGroup rbgActive;
    private javax.swing.ButtonGroup rbgRestletSSL;
    private javax.swing.JTabbedPane tabType;
    private javax.swing.JTextField txtAzureSecretAccessKey;
    private javax.swing.JTextField txtAzureStorageAcct;
    private javax.swing.JTextField txtHost;
    private javax.swing.JTextField txtId;
    private javax.swing.JTextField txtRestletHost;
    private javax.swing.JTextField txtS3AccessKey;
    private javax.swing.JTextField txtS3Bucket;
    private javax.swing.JTextField txtS3SecretAccessKey;
	
	private void initComponents() {
        rbgActive = new javax.swing.ButtonGroup();
        rbgRestletSSL = new javax.swing.ButtonGroup();
        lblId = new javax.swing.JLabel();
        txtHost = new javax.swing.JTextField();
        lblHost = new javax.swing.JLabel();
        txtId = new javax.swing.JTextField();
        lblMode = new javax.swing.JLabel();
        ddlMode = new javax.swing.JComboBox();
        lblActive = new javax.swing.JLabel();
        rbActiveYes = new javax.swing.JRadioButton();
        rbActiveNo = new javax.swing.JRadioButton();
        tabType = new javax.swing.JTabbedPane();
        panelS3 = new javax.swing.JPanel();
        lblS3AccessKey = new javax.swing.JLabel();
        txtS3AccessKey = new javax.swing.JTextField();
        lblS3SecretAccessKey = new javax.swing.JLabel();
        txtS3SecretAccessKey = new javax.swing.JTextField();
        lblS3Bucket = new javax.swing.JLabel();
        txtS3Bucket = new javax.swing.JTextField();
        panelRestlet = new javax.swing.JPanel();
        lblRestletHost = new javax.swing.JLabel();
        txtRestletHost = new javax.swing.JTextField();
        lblRestletSSL = new javax.swing.JLabel();
        rbRestletSSLOn = new javax.swing.JRadioButton();
        rbRestletSSLOff = new javax.swing.JRadioButton();
        panelAzure = new javax.swing.JPanel();
        lblAzureStorageAcct = new javax.swing.JLabel();
        txtAzureStorageAcct = new javax.swing.JTextField();
        lblAzureSecretAccessKey = new javax.swing.JLabel();
        txtAzureSecretAccessKey = new javax.swing.JTextField();
        btnSave = new javax.swing.JButton();
        btnSaveClose = new javax.swing.JButton();
        btnBack = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.HIDE_ON_CLOSE);
        setTitle("New/Edit Slice Store");
        setName("frmSliceStoresNewEdit");
        setIconImage(Main.sysIcon);
        
        setModal(true);
        setResizable(false);
        setLocationRelativeTo(null);

        lblId.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblId.setText("ID:");
        lblId.setName("lblId");
        
        txtId.setName("txtId");

        lblHost.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblHost.setText("Host:"); 
        lblHost.setName("lblHost"); 
        
        txtHost.setName("txtHost");

        lblMode.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblMode.setText("Mode:"); 
        lblMode.setName("lblMode"); 

        ddlMode.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "--", "Non-Stream", "Stream"}));
        ddlMode.setName("ddlMode"); 

        lblActive.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblActive.setText("Active:");
        lblActive.setName("lblActive");

        rbgActive.add(rbActiveYes);
        rbActiveYes.setText("Yes");
        rbActiveYes.setName("rbActiveYes");
        rbActiveYes.setSelected(true);

        rbgActive.add(rbActiveNo);
        rbActiveNo.setText("No");
        rbActiveNo.setName("rbActiveNo");

        tabType.setName("tabType"); 

        panelS3.setName("panelS3"); 

        lblS3AccessKey.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblS3AccessKey.setText("Access Key:");
        lblS3AccessKey.setName("lblS3AccessKey"); 

        txtS3AccessKey.setName("txtS3AccessKey"); 

        lblS3SecretAccessKey.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblS3SecretAccessKey.setText("Secret Access Key:"); 
        lblS3SecretAccessKey.setName("lblS3SecretAccessKey"); 

        txtS3SecretAccessKey.setName("txtS3SecretAccessKey"); 

        lblS3Bucket.setFont(new Font("Tahoma", Font.BOLD, 11));
        lblS3Bucket.setText("Bucket Name:"); 
        lblS3Bucket.setName("lblS3Bucket"); 

        txtS3Bucket.setName("txtS3Bucket"); 

        javax.swing.GroupLayout panelS3Layout = new javax.swing.GroupLayout(panelS3);
        panelS3.setLayout(panelS3Layout);
        panelS3Layout.setHorizontalGroup(
            panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelS3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(panelS3Layout.createSequentialGroup()
                        .addComponent(lblS3AccessKey)
                        .addGap(50, 50, 50)
                        .addComponent(txtS3AccessKey, javax.swing.GroupLayout.PREFERRED_SIZE, 182, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(panelS3Layout.createSequentialGroup()
                        .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(lblS3SecretAccessKey)
                            .addComponent(lblS3Bucket))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(txtS3Bucket, javax.swing.GroupLayout.PREFERRED_SIZE, 182, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(txtS3SecretAccessKey, javax.swing.GroupLayout.DEFAULT_SIZE, 354, Short.MAX_VALUE))))
                .addContainerGap())
        );
        panelS3Layout.setVerticalGroup(
            panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelS3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblS3AccessKey)
                    .addComponent(txtS3AccessKey, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblS3SecretAccessKey)
                    .addComponent(txtS3SecretAccessKey, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(panelS3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblS3Bucket)
                    .addComponent(txtS3Bucket, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(14, Short.MAX_VALUE))
        );

        tabType.addTab("Amazon S3", panelS3); 

        panelRestlet.setName("panelRestlet"); 

        lblRestletHost.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblRestletHost.setText("Status Host:");
        lblRestletHost.setName("lblRestletHost"); 

        txtRestletHost.setName("txtRestletHost"); 

        lblRestletSSL.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblRestletSSL.setText("Status SSL:"); 
        lblRestletSSL.setName("lblRestletSSL"); 

        rbgRestletSSL.add(rbRestletSSLOn);
        rbRestletSSLOn.setText("On"); 
        rbRestletSSLOn.setName("rbRestletSSLOn"); 

        rbgRestletSSL.add(rbRestletSSLOff);
        rbRestletSSLOff.setText("Off");
        rbRestletSSLOff.setName("rbRestletSSLOff"); 
        rbRestletSSLOff.setSelected(true);

        javax.swing.GroupLayout panelRestletLayout = new javax.swing.GroupLayout(panelRestlet);
        panelRestlet.setLayout(panelRestletLayout);
        panelRestletLayout.setHorizontalGroup(
            panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelRestletLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(lblRestletHost)
                    .addComponent(lblRestletSSL))
                .addGap(18, 18, 18)
                .addGroup(panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(panelRestletLayout.createSequentialGroup()
                        .addComponent(rbRestletSSLOn)
                        .addGap(18, 18, 18)
                        .addComponent(rbRestletSSLOff))
                    .addComponent(txtRestletHost, javax.swing.GroupLayout.PREFERRED_SIZE, 131, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(267, Short.MAX_VALUE))
        );
        panelRestletLayout.setVerticalGroup(
            panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelRestletLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblRestletHost)
                    .addComponent(txtRestletHost, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(panelRestletLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblRestletSSL)
                    .addComponent(rbRestletSSLOn)
                    .addComponent(rbRestletSSLOff))
                .addContainerGap(44, Short.MAX_VALUE))
        );

        tabType.addTab("SVDS Restlet", panelRestlet); 

        panelAzure.setName("panelAzure"); 

        lblAzureStorageAcct.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblAzureStorageAcct.setText("Storage Account:"); 
        lblAzureStorageAcct.setName("lblAzureStorageAcct"); 

        txtAzureStorageAcct.setName("txtAzureStorageAcct"); 

        lblAzureSecretAccessKey.setFont(new Font("Tahoma", Font.BOLD, 11)); 
        lblAzureSecretAccessKey.setText("Secret Access Key:"); 
        lblAzureSecretAccessKey.setName("lblAzureSecretAccessKey"); 

        txtAzureSecretAccessKey.setName("txtAzureSecretAccessKey"); 

        javax.swing.GroupLayout panelAzureLayout = new javax.swing.GroupLayout(panelAzure);
        panelAzure.setLayout(panelAzureLayout);
        panelAzureLayout.setHorizontalGroup(
            panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelAzureLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(lblAzureSecretAccessKey)
                    .addComponent(lblAzureStorageAcct))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(txtAzureStorageAcct, javax.swing.GroupLayout.PREFERRED_SIZE, 175, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(txtAzureSecretAccessKey, javax.swing.GroupLayout.DEFAULT_SIZE, 354, Short.MAX_VALUE))
                .addContainerGap())
        );
        panelAzureLayout.setVerticalGroup(
            panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelAzureLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblAzureStorageAcct)
                    .addComponent(txtAzureStorageAcct, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(panelAzureLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblAzureSecretAccessKey)
                    .addComponent(txtAzureSecretAccessKey, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(45, Short.MAX_VALUE))
        );

        tabType.addTab("Windows Azure", panelAzure);

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

        btnBack.setText("Back");
        btnBack.setName("btnBack");
        btnBack.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnBackActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(tabType, javax.swing.GroupLayout.DEFAULT_SIZE, 500, Short.MAX_VALUE)
                    .addGroup(layout.createSequentialGroup()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(lblMode)
                            .addComponent(lblHost)
                            .addComponent(lblId))
                        .addGap(18, 18, 18)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(txtHost, javax.swing.GroupLayout.PREFERRED_SIZE, 208, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(ddlMode, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(txtId, javax.swing.GroupLayout.PREFERRED_SIZE, 131, javax.swing.GroupLayout.PREFERRED_SIZE)))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(lblActive)
                        .addGap(18, 18, 18)
                        .addComponent(rbActiveYes)
                        .addGap(18, 18, 18)
                        .addComponent(rbActiveNo))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(btnSave)
                        .addGap(18, 18, 18)
                        .addComponent(btnSaveClose)
                        .addGap(18, 18, 18)
                        .addComponent(btnBack)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblId)
                    .addComponent(txtId, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblHost)
                    .addComponent(txtHost, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblMode)
                    .addComponent(ddlMode, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(lblActive)
                    .addComponent(rbActiveYes)
                    .addComponent(rbActiveNo))
                .addGap(18, 18, 18)
                .addComponent(tabType, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(18, 18, 18)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnSave)
                    .addComponent(btnSaveClose)
                    .addComponent(btnBack))
                .addContainerGap(23, Short.MAX_VALUE))
        );

        pack();
    }
	
	IMasterNamespaceTable mt=null;
	private FileSliceServerInfo fssi=null;
	
	public void displaySliceStore(String id){
		try {
			fssi=mt.getSliceServer(id, Main.getUser());
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				this.setVisible(false);
				this.dispose();
			}else{
				JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
						Main.sysName, JOptionPane.ERROR_MESSAGE);
				this.setVisible(false);
			}
			return;
		}catch(SVDSException ex){
			JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
					Main.sysName, JOptionPane.ERROR_MESSAGE);
			
			this.setVisible(false);
			return;
		}
		
		//clear away all previous data
		clearInput();
		try{
				txtId.setText(id);
				txtId.setEnabled(false);
				
				txtHost.setText(fssi.getServerHost());
				
				switch(fssi.getMode()){
					case NON_STREAM:
						ddlMode.setSelectedIndex(1);
						break;
					case STREAM:
						ddlMode.setSelectedIndex(2);
						break;
					default:
						ddlMode.setSelectedIndex(0);
				}
				
				if(fssi.getStatus()==FileSliceServerInfo.Status.ACTIVE)
					rbActiveYes.setSelected(true);
				else
					rbActiveNo.setSelected(true);
				
				switch(fssi.getType()){
					case AZURE:
						tabType.setSelectedIndex(2);
						
						txtAzureStorageAcct.setText(fssi.getKeyId());
						txtAzureSecretAccessKey.setText(fssi.getKey());
						break;
					case RESTLET:
						tabType.setSelectedIndex(1);
						
						txtRestletHost.setText(fssi.getProperty(FileSliceServerInfo.RestletPropName.STATUS_HOST.value()));
						if(fssi.getProperty(FileSliceServerInfo.RestletPropName.STATUS_SSL.value()).equalsIgnoreCase("on"))
							rbRestletSSLOn.setSelected(true);
						else
							rbRestletSSLOff.setSelected(true);
						break;
					case S3:
						tabType.setSelectedIndex(0);
						
						txtS3AccessKey.setText(fssi.getKeyId());
						txtS3SecretAccessKey.setText(fssi.getKey());
						txtS3Bucket.setText(fssi.getProperty(FileSliceServerInfo.S3PropName.CONTAINER.value()));				
						break;
				}
		}catch(Exception ex){
			JOptionPane.showMessageDialog(rootPane, "Error displaying slice store information.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
			
			this.setVisible(false);
		}
	}
	
	public void newSliceStore(){
		fssi=null;
		clearInput();
	}
	
	private void clearInput(){
		txtId.setText("");
		txtId.setEnabled(true);
		txtHost.setText("");
		ddlMode.setSelectedIndex(0);
		rbActiveYes.setSelected(true);
		tabType.setSelectedIndex(0);
		
		txtS3AccessKey.setText("");
		txtS3SecretAccessKey.setText("");
		txtS3Bucket.setText("");
		
		txtRestletHost.setText("");
		rbRestletSSLOff.setSelected(true);
		
		txtAzureStorageAcct.setText("");
		txtAzureSecretAccessKey.setText("");
	}
	
	private void btnSaveActionPerformed(java.awt.event.ActionEvent evt) {  
		if(saveSliceStore()){
			parent.showSliceStores();
			
			JOptionPane.showMessageDialog(rootPane, "Slice store saved successfully.",
					Main.sysName, JOptionPane.INFORMATION_MESSAGE);
		}else
			JOptionPane.showMessageDialog(rootPane, "Error saving slice store.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
	}
	
	private void btnSaveCloseActionPerformed(java.awt.event.ActionEvent evt) {  
		if(saveSliceStore()){
			JOptionPane.showMessageDialog(rootPane, "Slice store saved successfully.",
					Main.sysName, JOptionPane.INFORMATION_MESSAGE);
			
			this.setVisible(false);
			
			parent.showSliceStores();
		}else
			JOptionPane.showMessageDialog(rootPane, "Error saving slice store.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
	}
	
	private boolean saveSliceStore(){
		if(!validateFields()) return false;
		
		if(fssi==null) fssi=new FileSliceServerInfo(txtId.getText().trim());

		fssi.setServerHost(txtHost.getText().trim());
		fssi.setMode(ddlMode.getSelectedIndex()==1?FileIOMode.NON_STREAM:FileIOMode.STREAM);
		fssi.setStatus(rbActiveYes.isSelected()? FileSliceServerInfo.Status.ACTIVE: FileSliceServerInfo.Status.INACTIVE);
		switch(tabType.getSelectedIndex()){
			case -1:
			case 0:
				fssi.setType(FileSliceServerInfo.Type.S3);
				
				fssi.setKeyId(txtS3AccessKey.getText().trim());
				fssi.setKey(txtS3SecretAccessKey.getText().trim());
				
				fssi.setProperty(FileSliceServerInfo.S3PropName.CONTAINER.value(), txtS3Bucket.getText().trim());
				break;
			case 1:
				fssi.setType(FileSliceServerInfo.Type.RESTLET);
				
				fssi.setProperty(FileSliceServerInfo.RestletPropName.STATUS_HOST.value(), txtRestletHost.getText().trim());
				fssi.setProperty(FileSliceServerInfo.RestletPropName.STATUS_SSL.value(), rbRestletSSLOn.isSelected() ? "on" : "off");
				break;
			case 2:
				fssi.setType(FileSliceServerInfo.Type.AZURE);
				
				fssi.setKeyId(txtAzureStorageAcct.getText().trim());
				fssi.setKey(txtAzureSecretAccessKey.getText().trim());
				
				fssi.setProperty(FileSliceServerInfo.AzurePropName.USE_DEVELOPMENT.value(), "0");
				break;
		}
		
		try {
			mt.updateSliceServer(fssi, Main.getUser());
			
			return true;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
				this.setVisible(false);
				this.dispose();
			}
			
			return false;
		} catch (SVDSException ex) {
			ex.printStackTrace();

			return false;
		}
	}
	
	private boolean validateFields(){
		if((txtId.isEnabled() && txtId.getText().trim().length()==0) || txtHost.getText().trim().length()==0 ||
				ddlMode.getSelectedIndex()<0){
			JOptionPane.showMessageDialog(rootPane, "Values cannot be empty.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
			return false;
		}
		
		boolean isValid=true;
		switch(tabType.getSelectedIndex()){
			case -1:
			case 0:
				if(txtS3AccessKey.getText().trim().length()==0 || txtS3SecretAccessKey.getText().trim().length()==0 ||
						txtS3Bucket.getText().trim().length()==0)
					isValid=false;
	
				String host=txtHost.getText().trim();
				if(!host.equalsIgnoreCase(S3SliceStore.Region.AP_Singapore.toString()) && 
						!host.equalsIgnoreCase(S3SliceStore.Region.AP_Tokyo.toString()) &&
						!host.equalsIgnoreCase(S3SliceStore.Region.EU_Ireland.toString()) &&
						!host.equalsIgnoreCase(S3SliceStore.Region.US_Standard.toString()) &&
						!host.equalsIgnoreCase(S3SliceStore.Region.US_West.toString()))
					isValid=false;
				
				break;
			case 1:
				if(txtRestletHost.getText().trim().length()==0)
					isValid=false;
				
				break;
			case 2:
				if(txtAzureStorageAcct.getText().trim().length()==0 || txtAzureSecretAccessKey.getText().trim().length()==0)
					isValid=false;
				
				break;
			default:
				isValid=false;
		}
		
		if(!isValid){
			JOptionPane.showMessageDialog(rootPane, "Values cannot be empty/invalid.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
			
			return false;
		}

		return true;
	}
	
	private void btnBackActionPerformed(java.awt.event.ActionEvent evt) {  
		this.setVisible(false);
	}
}
