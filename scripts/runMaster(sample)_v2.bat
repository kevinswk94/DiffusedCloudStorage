set path=C:\Program Files\Java\jdk1.6.0_26\bin
set proppath=D:\Projects\CloudComputing\Development\DiffusedCloudStorage\resource
set libpath=D:\Projects\CloudComputing\Development\DiffusedCloudStorage\lib
set classpath=.;%libpath%\org.osgi.core.jar;%libpath%\org.restlet.jar;%libpath%\javax.servlet.jar;%libpath%\org.eclipse.jetty.ajp.jar;%libpath%\org.eclipse.jetty.continuations.jar;%libpath%\org.eclipse.jetty.http.jar;%libpath%\org.eclipse.jetty.io.jar;%libpath%\org.eclipse.jetty.server.jar;%libpath%\org.eclipse.jetty.util.jar;%libpath%\org.jsslutils.jar;%libpath%\org.restlet.ext.jetty.jar;%libpath%\org.restlet.ext.ssl.jar;%libpath%\commons-logging-1.1.1.jar;%libpath%\log4j-1.2.9.jar;%libpath%\httpclient-4.1.1.jar;%libpath%\httpcore-4.1.jar;%libpath%\aws-java-sdk-1.2.2.jar;%libpath%\mail-1.4.3.jar;%libpath%\stax-1.2.0.jar;%libpath%\microsoft-windowsazure-api-0.2.0.jar;%libpath%\commons-lang3-3.1.jar;%libpath%\jackson-core-asl-1.8.3.jar;%libpath%\jackson-jaxrs-1.8.3.jar;%libpath%\jackson-mapper-asl-1.8.3.jar;%libpath%\jackson-xc-1.8.3.jar;%libpath%\javax.inject-1.jar;%libpath%\jaxb-impl-2.2.3-1.jar;%libpath%\jersey-client-1.10-b02.jar;%libpath%\jersey-core-1.10-b02.jar;%libpath%\jersey-json-1.10-b02.jar;%libpath%\jettison-1.1.jar;%libpath%\stax-api-1.0.1.jar;%proppath%
java -Djava.util.logging.config.file=%proppath%\restlet-logging.properties sg.edu.nyp.sit.svds.master.Main -type main -ida_config IDAProp.properties -sys_config MasterConfig.properties
pause