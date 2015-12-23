Instructions for running the svdfs
----------------------------------

1. Ensure you have JDK 1.7x installed and available in the path
2. Unzip the file into a folder (e.g c:\svdfs) 
3. Run InstallEldos.bat (as admin) to install Eldos drivers. restart the computer. 
3. Run the following in the order: StartMaster.bat, StartStores, StartClient. 

This will start the master server, and 5 local slicestores, and then the file client. 

4. From the notification bar, you will notice a little icon (like a harddisk), right click and select mount. This will mount the virtual filesystem (e.g. T:\) 

5. Drag a file (any file) to the virtual drive. 

You can go to the slice stores storage directory to see the individual slices (in this demo, the IDA is configured to use 3/5 encoding). The storage directories are called testfs1, testfs2, etc.. 

Try deleting 1 or 2 slices. You should be still be able to open the file. Try deleting 3 slices, and you will see that the file is no more accessible. 

Note: you can change the port numbers used by changing the configuration files in the resources subfolder (such as MasterConfig.properties, SliceStoreConfig.properties, and svdsclient.properties). 

