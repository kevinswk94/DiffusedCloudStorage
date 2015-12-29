package sg.edu.nyp.sit.svds.client;

import java.awt.AWTException;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.HashSet;

import javax.swing.ImageIcon;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sg.edu.nyp.sit.pvfs.virtualdisk.IVirtualDisk;
import sg.edu.nyp.sit.pvfs.virtualdisk.VirtualDiskFactory;
import sg.edu.nyp.sit.svds.Resources;
import sg.edu.nyp.sit.svds.metadata.User;

public class SVDSFileSystem
{
	public static final long serialVersionUID = 2L;

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(SVDSFileSystem.class);

	private static Image sysIcon;
	private static String sysName = "SVDS File System";

	private final MenuItem unmountItem = new MenuItem("Unmount");
	private final MenuItem mountItem = new MenuItem("Mount");

	private TrayIcon trayIcon = null;

	private char mountLetter;
	private IVirtualDisk fs = null;

	private User usr = new User("test", "test");

	public static void main(String[] args) throws Exception
	{
		sysIcon = new ImageIcon(Resources.findFile("harddrive.png"), sysName).getImage();
		(new SVDSFileSystem()).start();
	}

	public SVDSFileSystem()
	{
		try
		{
			UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
		} catch (Exception ex)
		{
			ex.printStackTrace();
		}

		if (!SystemTray.isSupported())
		{
			System.out.println("SystemTray is not supported! Application will exit.");
			return;
		}
	}

	public void start()
	{
		SwingUtilities.invokeLater(new Runnable()
		{
			public void run()
			{
				createAndShowGUI();
			}
		});
	}

	private void createAndShowGUI()
	{
		try
		{
			trayIcon = new TrayIcon(sysIcon);
		} catch (Exception ex)
		{
			System.out.println("Error locating icon! Application will exit.");
			return;
		}
		trayIcon.setImageAutoSize(true);

		final PopupMenu popup = new PopupMenu();
		final SystemTray tray = SystemTray.getSystemTray();

		final MenuItem exitItem = new MenuItem("Exit");

		unmountItem.setEnabled(false);
		mountItem.setEnabled(true);

		popup.add(mountItem);
		popup.add(unmountItem);
		popup.addSeparator();
		popup.add(exitItem);

		trayIcon.setPopupMenu(popup);

		try
		{
			tray.add(trayIcon);
		} catch (AWTException ex)
		{
			System.out.println("Error adding application to system tray! Application will exit.");
			return;
		}

		mountItem.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					mountDrive();
					mountItem.setEnabled(false);
					unmountItem.setEnabled(true);
					showTrayMsg("Virtual drive mounted successfully.");
				} catch (Exception ex)
				{
					ex.printStackTrace();
					showTrayError("Error mounting virtual drive.");
				}
			}
		});

		unmountItem.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					unmountDrive();
					mountItem.setEnabled(true);
					unmountItem.setEnabled(false);
					showTrayMsg("Virtual drive dismounted successfully.");
				} catch (Exception ex)
				{
					ex.printStackTrace();
					showTrayError("Error dismounting virtual drive.");
				}
			}
		});

		exitItem.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					unmountDrive();
					Thread.sleep(1000 * 3);
				} catch (Exception ex)
				{
					ex.printStackTrace();
				}
				tray.remove(trayIcon);
				System.exit(0);
			}
		});

		try
		{
			// mountDrive();
		} catch (Exception e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private void mountDrive() throws Exception
	{
		fs = VirtualDiskFactory.getInstance(usr);
		if (fs == null)
			throw new NullPointerException("Unable to get instance of virtual disk.");

		mountLetter = getAvailableDriveLetter();
		fs.mount(mountLetter);
	}

	private void unmountDrive() throws Exception
	{
		if (fs == null)
			throw new NullPointerException("Virtual drive is not mounted.");

		fs.unmount();

		if (fs.requireRestart())
		{
			// for work around, start a new process running and exit the current
			// process
			// this is because dokan is unable to mount twice in the same
			// process (even if
			// it is previously unmounted)
			try
			{
				// pause for 3 seconds so the ballon will stay for user to see
				Thread.sleep(1000 * 3);

				String path = Resources.findFile("start_svdsfs.bat");
				if (path.startsWith("/") || path.startsWith("\\"))
					path = path.substring(1);

				Runtime.getRuntime().exec("cmd /c start /min " + path.replace("/", "\\"));
			} catch (Exception ex)
			{
				ex.printStackTrace();
			} finally
			{
				System.exit(0);
			}
		}

		fs = null;
	}

	private char getAvailableDriveLetter() throws InstantiationException
	{
		HashSet<Character> usedDrives = new HashSet<Character>();
		for (File d : File.listRoots())
		{
			usedDrives.add(d.getPath().charAt(0));
		}

		char defLetter = 'T';
		if (!usedDrives.contains(defLetter))
			return Character.toLowerCase(defLetter);

		for (int i = 'D'; i <= 'Z'; i++)
		{
			if (!usedDrives.contains((char) i))
				return Character.toLowerCase((char) i);
		}

		throw new InstantiationException("Unable to locate free drive letter.");
	}

	public void showError(String msg)
	{
		JOptionPane.showMessageDialog(null, msg, sysName, JOptionPane.ERROR_MESSAGE);
	}

	public void showMsg(String msg)
	{
		JOptionPane.showMessageDialog(null, msg, sysName, JOptionPane.INFORMATION_MESSAGE);
	}

	public void showTrayError(String msg)
	{
		trayIcon.displayMessage(sysName, msg, TrayIcon.MessageType.ERROR);
	}

	public void showTrayMsg(String msg)
	{
		trayIcon.displayMessage(sysName, msg, TrayIcon.MessageType.INFO);
	}

}
