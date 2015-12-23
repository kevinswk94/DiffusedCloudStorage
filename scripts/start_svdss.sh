export CLASSPATH=svdsslicestore.jar:lib/org.osgi.core.jar:lib/org.restlet.jar:lib/commons-logging-1.1.1.jar:lib/log4j-1.2.9.jar:lib/org.jsslutils.jar:lib/org.restlet.ext.ssl.jar:lib/javax.servlet.jar:lib/org.eclipse.jetty.ajp.jar:lib/org.eclipse.jetty.continuations.jar:lib/org.eclipse.jetty.http.jar:lib/org.eclipse.jetty.io.jar:lib/org.eclipse.jetty.server.jar:lib/org.eclipse.jetty.util.jar:lib/org.restlet.ext.jetty.jar:resources:.
java -cp $CLASSPATH -Djava.util.logging.config.file=resources/restlet-logging.properties sg.edu.nyp.sit.svds.filestore.Main -id $1 -fport 8010 -sport 8011 -host $2 -reghost $3 -config SliceStoreConfig.properties -path /home/user/slicestore/FS;
