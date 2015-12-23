package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

import sg.edu.nyp.sit.svds.Resources;
import eldos.cbfs.CallbackFileSystem;
import eldos.cbfs.ECBFSError;
import eldos.cbfs.ServiceStatus;
import eldos.cbfs.boolRef;
import eldos.cbfs.longRef;

public class InstallEldos {
	public static final long serialVersionUID = 1L;

	private CallbackFileSystem cbfs = null;
	private static String driverFileName = "cbfs.cab";
	private static String driverPath = "";
	private static String defaultDriverPath = "C:\\Program Files (x86)\\EldoS\\Callback File System\\Drivers\\cbfs.cab";

	public static void main(String[] args) throws Exception {
		try {
			if (args.length < 1) {
				System.out
						.println("Please specify -i (install) or -u (uninstall)");
				return;
			}
			String action = args[0];
			if (args.length > 1)
				driverPath = args[1];
			else
				driverPath = defaultDriverPath;
			if (!action.equalsIgnoreCase("-i")
					&& !action.equalsIgnoreCase("-u")) {
				System.out
						.println("Please specify -i (install) or -u (uninstall)");
				return;
			}
			InstallEldos e = new InstallEldos();

			if (action.equalsIgnoreCase("-i")) {
				if (!e.isEldosInstalled()) {
					if (e.installDriver()) {
						System.out.println("driver installed. Please reboot");
					} else
						System.out.println("driver installed.");
				} else {
					System.out.println("driver already installed");
				}
			} else {
				if (e.uninstallDriver()) {
					System.out.println("driver uninstalled. Please reboot");
				} else {
					System.out.println("driver uninstalled");
				}
			}
			
			e = null;
		} catch (Exception ex) {
			ex.printStackTrace();
			return;
		}

	}

	public InstallEldos() {
		cbfs = new CallbackFileSystem();
		cbfs.setRegistrationKey(VirtualFS.sRegKey);
	}

	public boolean isEldosInstalled() throws Exception {
		boolRef installed = new boolRef(false);
		longRef versionHigh = new longRef();
		longRef versionLow = new longRef();
		ServiceStatus status = new ServiceStatus();

		try {
			cbfs.getModuleStatus(VirtualFS.mGuid,
					CallbackFileSystem.CBFS_MODULE_DRIVER, installed,
					versionHigh, versionLow, status);

			return installed.getValue();
		} catch (ECBFSError ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	public boolean uninstallDriver() throws Exception {
		//String driverPath = Resources.findFile(driverFileName);
		//if (driverPath.startsWith("/"))
		//	driverPath = driverPath.substring(1);
		System.out.println(driverPath);

		try {
			boolRef reboot = new boolRef(false);
			cbfs.uninstall(driverPath, VirtualFS.mGuid, "", reboot);
			return reboot.getValue();
		} catch (ECBFSError ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

	public boolean installDriver() throws Exception {
		//String driverPath = Resources.findFile(driverFileName);
		//if (driverPath.startsWith("/"))
		//	driverPath = driverPath.substring(1);
		System.out.println(driverPath);

		try {
			boolRef reboot = new boolRef(false);

			cbfs.install(
					driverPath,
					VirtualFS.mGuid,
					"",
					true,
					false,
					CallbackFileSystem.CBFS_MODULE_DRIVER
							| CallbackFileSystem.CBFS_MODULE_MOUNT_NOTIFIER_DLL
							| CallbackFileSystem.CBFS_MODULE_NET_REDIRECTOR_DLL,
					reboot);

			return reboot.getValue();
		} catch (ECBFSError ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
}
