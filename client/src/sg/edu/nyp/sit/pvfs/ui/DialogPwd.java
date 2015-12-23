package sg.edu.nyp.sit.pvfs.ui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import javax.swing.JOptionPane;

import sg.edu.nyp.sit.pvfs.Main;

public class DialogPwd extends javax.swing.JDialog implements ActionListener, PropertyChangeListener {
	public  static final long serialVersionUID = 1L;

	public DialogPwd(java.awt.Frame parent) {
		super(parent, true);
		initComponents();
	}
    
    private JOptionPane optionPane;
    private String pwd;
    
    private String strBtnOK="OK";
    private String strBtnCancel="Cancel";
    
    private javax.swing.JPasswordField txtPwd;
    private javax.swing.JLabel lblMsg;
	
	private void initComponents() {
		lblMsg = new javax.swing.JLabel();
		txtPwd = new javax.swing.JPasswordField();
 
        setIconImage(Main.sysIcon);
        setLocationRelativeTo(null); 
        setModal(true);
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
                public void windowClosing(WindowEvent we) {
                /*
                 * Instead of directly closing the window,
                 * we're going to change the JOptionPane's
                 * value property.
                 */
                    optionPane.setValue(new Integer(
                                        JOptionPane.CLOSED_OPTION));
            }
        });
        
        lblMsg.setText("Enter password:"); 
        
        optionPane = new JOptionPane(new Object[]{lblMsg, txtPwd},
                JOptionPane.QUESTION_MESSAGE,
                JOptionPane.YES_NO_OPTION,
                null,
                new String[]{strBtnOK, strBtnCancel},
                strBtnOK);
        
        setContentPane(optionPane);
        
      //Ensure the text field always gets the first focus.
        addComponentListener(new ComponentAdapter() {
            public void componentShown(ComponentEvent ce) {
            	txtPwd.requestFocusInWindow();
            }
        });
        
      //Register an event handler that puts the text into the option pane.
        txtPwd.addActionListener(this);
        
      //Register an event handler that reacts to option pane state changes.
        optionPane.addPropertyChangeListener(this);
        
        pack();
    }
	
	public String getPwd(){
		return pwd;
	}

	/** This method reacts to state changes in the option pane. */
	@Override
	public void propertyChange(PropertyChangeEvent e) {
		String prop = e.getPropertyName();

        if (isVisible()
         && (e.getSource() == optionPane)
         && (JOptionPane.VALUE_PROPERTY.equals(prop) ||
             JOptionPane.INPUT_VALUE_PROPERTY.equals(prop))) {
            Object value = optionPane.getValue();

            if (value == JOptionPane.UNINITIALIZED_VALUE) {
                //ignore reset
                return;
            }

            //Reset the JOptionPane's value.
            //If you don't do this, then if the user
            //presses the same button next time, no
            //property change event will be fired.
            optionPane.setValue(
                    JOptionPane.UNINITIALIZED_VALUE);

            if (strBtnOK.equals(value)) {
            	if(txtPwd.getPassword()==null || txtPwd.getPassword().length==0){
            		JOptionPane.showMessageDialog(
                            DialogPwd.this, "Password cannot be blank.",
                            "Empty", JOptionPane.ERROR_MESSAGE);
            		pwd = null;
            		txtPwd.requestFocusInWindow();
            		return;
            	}
                
            	pwd = new String(txtPwd.getPassword());
            	done();
            } else { //user closed dialog or clicked cancel
                pwd = null;
                done();
            }
        }
	}
	
	public void done() {
        txtPwd.setText(null);
        setVisible(false);
    }

	/** This method handles events for the text field. */
	@Override
	public void actionPerformed(ActionEvent e) {
		optionPane.setValue(strBtnOK);
	}
}
