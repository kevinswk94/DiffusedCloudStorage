package sg.edu.nyp.sit.pvfs.ui;

import java.awt.Cursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.bluetooth.DiscoveryAgent;
import javax.bluetooth.LocalDevice;
import javax.bluetooth.RemoteDevice;
import javax.microedition.io.Connector;
import javax.microedition.io.StreamConnection;
import javax.swing.DefaultListModel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import sg.edu.nyp.sit.pvfs.Main;
import sg.edu.nyp.sit.pvfs.svc.BTAgent;
import sg.edu.nyp.sit.pvfs.svc.DeviceConnected;
import sg.edu.nyp.sit.pvfs.svc.IBTAgent;

public class FormDevice extends javax.swing.JFrame implements IBTAgent {
	public static final long serialVersionUID = 1L;
	
	public FormDevice() throws Exception{
        initComponents();
        
        //run after the UI is shown
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
            	showCachedDevices();
            }
        });
    }
	
	private javax.swing.JMenuItem itemRefreshDevice;
	private javax.swing.JLabel lblInstruction;
	private javax.swing.JList lstDevices;
	private javax.swing.JMenuBar mainMenu;
	private javax.swing.JMenu menuDevices;
	private javax.swing.JScrollPane sclDevices;
	private DefaultListModel lstDevicesModel;

	private void initComponents() throws Exception{
		lblInstruction = new javax.swing.JLabel();
        sclDevices = new javax.swing.JScrollPane();
        lstDevicesModel=new DefaultListModel();
        lstDevices = new javax.swing.JList(lstDevicesModel);
        mainMenu = new javax.swing.JMenuBar();
        menuDevices = new javax.swing.JMenu();
        itemRefreshDevice = new javax.swing.JMenuItem();
        
        setDefaultCloseOperation(javax.swing.WindowConstants.HIDE_ON_CLOSE);
        setName("FormDevice");
        setTitle("Mount Virtual Drive");
        setIconImage(Main.sysIcon);
        
        setResizable(false);
        setLocationRelativeTo(null); 
        
        lblInstruction.setText("Select device to connect:");
        lblInstruction.setName("lblInstruction");
        
        sclDevices.setName("sclDevices"); 

        lstDevices.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        lstDevices.setName("lstDevices"); 
        lstDevices.setLayoutOrientation(JList.VERTICAL);
        lstDevices.addListSelectionListener(new javax.swing.event.ListSelectionListener() {
            public void valueChanged(javax.swing.event.ListSelectionEvent evt) {
                lstDevicesValueChanged(evt);
            }
        });
        sclDevices.setViewportView(lstDevices);
        
        mainMenu.setName("mainMenu");
        
        menuDevices.setText("Devices");
        menuDevices.setName("menuDevices");
        
        itemRefreshDevice.setToolTipText("Refresh devices in the list below.");
        itemRefreshDevice.setText("Refresh List"); 
        itemRefreshDevice.setName("itemRefreshDevice");
        itemRefreshDevice.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                itemRefreshDeviceActionPerformed(evt);
            }
        });
        menuDevices.add(itemRefreshDevice);
        
        mainMenu.add(menuDevices);

        setJMenuBar(mainMenu);
        
        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(sclDevices, javax.swing.GroupLayout.DEFAULT_SIZE, 380, Short.MAX_VALUE)
                    .addComponent(lblInstruction))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(lblInstruction)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(sclDevices, javax.swing.GroupLayout.DEFAULT_SIZE, 237, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
	}
	
	private void showCachedDevices(){
		try{
			setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			
			if(LocalDevice.getLocalDevice()==null)
				return;

			DiscoveryAgent a=LocalDevice.getLocalDevice().getDiscoveryAgent();
			RemoteDevice[] arrCached=a.retrieveDevices(DiscoveryAgent.CACHED);
			RemoteDevice[] arrKnown=a.retrieveDevices(DiscoveryAgent.PREKNOWN);
			
			if(arrCached==null && arrKnown==null)
				return;
			
			Hashtable<String, RemoteDevice> arr=new Hashtable<String, RemoteDevice>();
			
			if(arrCached!=null){
				for(RemoteDevice d: arrCached)
					arr.put(d.getBluetoothAddress(), d);
			}
			
			if(arrKnown!=null){
				for(RemoteDevice d: arrKnown){
					if(!arr.containsKey(d.getBluetoothAddress()))
						arr.put(d.getBluetoothAddress(), d);
				}
			}
			
			showDevices(new ArrayList<RemoteDevice>(arr.values()));
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}
	
	private void itemRefreshDeviceActionPerformed(java.awt.event.ActionEvent evt) {
		try{
			if(LocalDevice.getLocalDevice()==null){
				JOptionPane.showMessageDialog(rootPane, "Bluetooth is not available.",
						Main.sysName, JOptionPane.ERROR_MESSAGE);
				return;
			}
			
			btAgent.startDiscovery();
			
			//change the cursor to let the user know the task is running;
			setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
		}catch(Exception ex){
			JOptionPane.showMessageDialog(rootPane, "Error starting bluetooth device discovery.",
					Main.sysName, JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private void lstDevicesValueChanged(javax.swing.event.ListSelectionEvent evt) {
		if (!evt.getValueIsAdjusting() && lstDevices.getSelectedIndex()!=-1) {
			if(Main.isDriveMounted()){
				JOptionPane.showMessageDialog(rootPane, "Drive is already mounted.\nPlease unmount before trying again.",
						Main.sysName, JOptionPane.ERROR_MESSAGE);
				lstDevices.clearSelection();
				return;
			}
			
			RemoteDevice d=arrDevices.get(lstDevices.getSelectedIndex());
			
			if(JOptionPane.showOptionDialog(rootPane, "Are you sure you want to connect to "
					+lstDevices.getSelectedValue() + "?", Main.sysName, JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, 
					null, new String[]{"Yes", "No"}, "No")==JOptionPane.NO_OPTION){
				lstDevices.clearSelection();
				return;
			}
			
			String connUrl=btAgent.findDeviceService(d); 			
		
		    if (connUrl==null){
		    	JOptionPane.showMessageDialog(rootPane, "Unable to locate bluetooth service in selected device.",
						Main.sysName, JOptionPane.ERROR_MESSAGE);
		    	lstDevices.clearSelection();
		    	return;
		    }
		    
		    try{
		    	StreamConnection c=(StreamConnection) Connector.open(connUrl);
		    	
		    	//if this is not run in a separate thread, the UI will get blocked
		    	//not feasible to put the codes in here because the server portion
		    	//uses the same code
		    	DeviceConnected dc=new DeviceConnected(c, d);
		    	dc.start();
		    } catch (Exception ex) {
		    	JOptionPane.showMessageDialog(rootPane, ex.getMessage(),
						Main.sysName, JOptionPane.ERROR_MESSAGE);
			}
		    
		    lstDevices.clearSelection();
		}
	}
	
	private List<RemoteDevice> arrDevices=null;
	private BTAgent btAgent=new BTAgent(this);

	@Override
	public void deviceDiscoveryCompleted(List<RemoteDevice> arrDevices) {
		lstDevicesModel.clear();

		showDevices(arrDevices);
		
		setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		
		JOptionPane.showMessageDialog(rootPane, "Device discovery complete.",
				Main.sysName, JOptionPane.INFORMATION_MESSAGE);
	}
	
	private void showDevices(List<RemoteDevice> arrDevices){
		this.arrDevices=arrDevices;
		
		String tmp;
		for(RemoteDevice d: arrDevices){
			tmp="["+d.getBluetoothAddress()+"] ";
			try{ tmp+=d.getFriendlyName(true); }
			catch(IOException ex){	tmp+="<unable to retrieve name>"; }
			
			lstDevicesModel.addElement(tmp);
		}
	}
}
