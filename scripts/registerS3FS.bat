echo on
set installpath=c:\svdfs
set masterpath=%installpath%\master
set libpath=%masterpath%\lib
set proppath=%masterpath%\resources
set classpath=.;%masterpath%\svdsmaster.jar;%libpath%\svdscore.jar;%libpath%;%libpath%\commons-logging-1.1.1.jar;%libpath%\log4j-1.2.9.jar;%libpath%\aws-java-sdk-1.2.2.jar;%libpath%\jackson-core-asl-1.8.3.jar;%libpath%\mail-1.4.3.jar;%libpath%\stax-1.2.0.jar;%libpath%\stax-api-1.0.1.jar;%libpath%\commons-codec-1.4.jar;%libpath%\httpclient-4.1.1.jar;%libpath%\httpcore-4.1.jar
java -Djava.util.logging.config.file=%proppath%\restlet-logging.properties sg.edu.nyp.sit.svds.master.filestore.S3SliceStoreRegistration %masterpath%\resources\s3SliceStores(sample).txt localhost:9011 http
pause