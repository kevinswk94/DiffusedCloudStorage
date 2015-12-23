echo on
set installpath=c:\svdfs
set slicestorepath=%installpath%\slicestore
set libpath=%slicestorepath%\lib
set proppath=%slicestorepath%\resources
SET CLASSPATH=%slicestorepath%\svdsslicestore.jar;%libpath%\svdscore.jar;%libpath%\org.osgi.core.jar;%libpath%\org.restlet.jar;%libpath%\commons-logging-1.1.1.jar;%libpath%\log4j-1.2.9.jar;%libpath%\org.jsslutils.jar;%libpath%\org.restlet.ext.ssl.jar;%libpath%\javax.servlet.jar;%libpath%\org.eclipse.jetty.ajp.jar;%libpath%\org.eclipse.jetty.continuations.jar;%libpath%\org.eclipse.jetty.http.jar;%libpath%\org.eclipse.jetty.io.jar;%libpath%\org.eclipse.jetty.server.jar;%libpath%\org.eclipse.jetty.util.jar;%libpath%\org.restlet.ext.jetty.jar;resources;.
java -cp %CLASSPATH% -Djava.util.logging.config.file=resources\restlet-logging.properties sg.edu.nyp.sit.svds.filestore.Main -id fs1 -fport 7010 -sport 7011 -host localhost -config SliceStoreConfig.properties -path C:\\svdfs\\slicestore\\testfs1
