package sg.edu.nyp.sit.pvfs.ui;

import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.svds.client.master.IMasterNamespaceTable;
import sg.edu.nyp.sit.svds.client.master.MasterTableFactory;
import sg.edu.nyp.sit.svds.exception.LockedSVDSException;
import sg.edu.nyp.sit.svds.exception.RejectedSVDSException;
import sg.edu.nyp.sit.svds.exception.SVDSException;
import sg.edu.nyp.sit.svds.metadata.FileSliceServerInfo;

public class FormSliceStores extends javax.swing.JFrame {
	public static final long serialVersionUID = 2L;

	public FormSliceStores() {
        initComponents();
        
        mt=MasterTableFactory.getNamespaceInstance();
        
        //run after the UI is shown
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
            	showSliceStores();
            }
        });
    }
	
	private DialogSliceStoresNewEdit frmSliceStoreNewEdit=null;
	
	private javax.swing.JButton btnDelete;
	private javax.swing.JButton btnEdit;
    private javax.swing.JMenuItem itemNewSliceStore;
    private javax.swing.JMenuItem itemRefreshSliceStores;
    private javax.swing.JLabel lblInstruction;
    private javax.swing.JList lstSliceStores;
    private javax.swing.JMenuBar mainMenu;
    private javax.swing.JMenu menuSliceStores;
    private javax.swing.JScrollPane sclSliceStores;
    private DefaultListModel lstSliceStoresModel;
	
	private void initComponents() {
		lblInstruction = new javax.swing.JLabel();
		sclSliceStores = new javax.swing.JScrollPane();
		lstSliceStoresModel=new DefaultListModel();
        lstSliceStores = new javax.swing.JList(lstSliceStoresModel);
        btnDelete = new javax.swing.JButton();
        btnEdit = new javax.swing.JButton();
        mainMenu = new javax.swing.JMenuBar();
        menuSliceStores = new javax.swing.JMenu();
        itemRefreshSliceStores = new javax.swing.JMenuItem();
        itemNewSliceStore = new javax.swing.JMenuItem();

        setDefaultCloseOperation(javax.swing.WindowConstants.HIDE_ON_CLOSE);
        setTitle("Personal Virtual File System Slice Stores");
        setName("frmSliceStores");
        setIconImage(Main.sysIcon);
        setResizable(false);
        setLocationRelativeTo(null);

        lblInstruction.setText("Select slice stores to edit/delete:");
        lblInstruction.setName("lblInstruction");

        sclSliceStores.setName("sclSliceStores");

        lstSliceStores.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        lstSliceStores.setName("lstSliceStores");
        sclSliceStores.setViewportView(lstSliceStores);

        btnDelete.setText("Delete"); 
        btnDelete.setName("btnDelete");
        btnDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnDeleteActionPerformed(evt);
            }
        });
        
        btnEdit.setText("Edit");
        btnEdit.setName("btnEdit"); 
        btnEdit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                btnEditActionPerformed(evt);
            }
        });

        mainMenu.setName("mainMenu"); 

        menuSliceStores.setText("Slice Stores");
        menuSliceStores.setActionCommand("Slice Stores");
        menuSliceStores.setName("menuSliceStores"); 

        itemRefreshSliceStores.setToolTipText("Refresh slice stores in the list below.");
        itemRefreshSliceStores.setLabel("Refresh List");
        itemRefreshSliceStores.setName("itemRefreshSliceStores");
        itemRefreshSliceStores.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                itemRefreshSliceStoresActionPerformed(evt);
            }
        });
        menuSliceStores.add(itemRefreshSliceStores);

        itemNewSliceStore.setText("New");
        itemNewSliceStore.setName("itemNewSliceStore");
        itemNewSliceStore.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
            	showDialogSliceStoreNewEdit(null);
            }
        });
        
        menuSliceStores.add(itemNewSliceStore);

        mainMenu.add(menuSliceStores);

        setJMenuBar(mainMenu);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addContainerGap()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(sclSliceStores, javax.swing.GroupLayout.DEFAULT_SIZE, 380, Short.MAX_VALUE)
                            .addComponent(lblInstruction)))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(136, 136, 136)
                        .addComponent(btnEdit)
                        .addGap(18, 18, 18)
                        .addComponent(btnDelete)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(lblInstruction)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(sclSliceStores, javax.swing.GroupLayout.PREFERRED_SIZE, 230, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(btnDelete)
                    .addComponent(btnEdit))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }
	
	IMasterNamespaceTable mt=null;
	
	public void showSliceStores(){
		List<FileSliceServerInfo> stores;
		try {
			stores=mt.getAvailableSliceServers(null, Main.getUser());
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}else{
				JOptionPane.showMessageDialog(rootPane, "Error retrieving slice stores.",
						Main.sysName, JOptionPane.ERROR_MESSAGE);
			}
			return;
		} catch (SVDSException ex) {
			JOptionPane.showMessageDialog(rootPane, "Error retrieving slice stores.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
			return;
		}
		
		lstSliceStoresModel.clear();
		
		if(stores==null){
			JOptionPane.showMessageDialog(rootPane, "No slice stores found.",
					Main.sysName, JOptionPane.INFORMATION_MESSAGE);
			return;
		}
		
		for(FileSliceServerInfo fssi: stores){
			lstSliceStoresModel.addElement("[" + fssi.getStatus().toString()+"] "+fssi.getServerId());
		}
	}
	
	private void itemRefreshSliceStoresActionPerformed(java.awt.event.ActionEvent evt) {
		showSliceStores();
	}
	
	private void btnDeleteActionPerformed(java.awt.event.ActionEvent evt) {  
		String id=getSelectedSliceStore();
		if(id==null) return;
		
		boolean success=false;
		try{
			mt.removeSliceServer(null, id, Main.getUser());
			success=true;
		}catch(RejectedSVDSException ex){
			if(ex.getOrigin()==RejectedSVDSException.PROXY){
				Main.showTrayError(Main.REMOTE_ENDED_MSG);
				Main.driveUnmounted();
			}else{
				JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
						Main.sysName, JOptionPane.ERROR_MESSAGE);
			}
		}catch(LockedSVDSException ex){
			JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
					Main.sysName, JOptionPane.WARNING_MESSAGE);
		}catch(SVDSException ex){
			JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
					Main.sysName, JOptionPane.ERROR_MESSAGE);
		}
		
		if(success){
			int pos=lstSliceStores.getSelectedIndex();
			lstSliceStores.clearSelection();
			lstSliceStoresModel.removeElementAt(pos);
		}
	}

	private void btnEditActionPerformed(java.awt.event.ActionEvent evt) {  
		String id=getSelectedSliceStore();
		if(id==null) return;
		
		showDialogSliceStoreNewEdit(id);
	}
	
	private void showDialogSliceStoreNewEdit(String id){
		 if(frmSliceStoreNewEdit==null){
         	frmSliceStoreNewEdit=new DialogSliceStoresNewEdit(this);
         }
         
		 if(id==null)
			 frmSliceStoreNewEdit.newSliceStore();
		 else
			 frmSliceStoreNewEdit.displaySliceStore(id);

         frmSliceStoreNewEdit.setVisible(true);
	}
	
	private String getSelectedSliceStore(){
		if(lstSliceStores.getSelectedIndex()==-1){
			JOptionPane.showMessageDialog(rootPane, "Must select 1 slice store.",
					Main.sysName, JOptionPane.WARNING_MESSAGE);
			return null;
		}
		
		String id=lstSliceStores.getSelectedValue().toString();
		return id.substring(id.indexOf("] ")+2);
	}
	
	public void disposeDialogSliceStoresNewEdit(){
		if(frmSliceStoreNewEdit!=null){
			frmSliceStoreNewEdit.dispose();
			frmSliceStoreNewEdit=null;
		}
	}
}
