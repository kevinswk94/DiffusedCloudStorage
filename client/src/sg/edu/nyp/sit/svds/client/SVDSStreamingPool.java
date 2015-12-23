package sg.edu.nyp.sit.svds.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SVDSStreamingPool {
	public static final long serialVersionUID = 1L;
	
	private static boolean isPoolStarted=false;
	private static ExecutorService pool=null;
	
	private static void initPool(){
		pool=Executors.newCachedThreadPool();
		isPoolStarted=true;
	}
	
	@SuppressWarnings("rawtypes")
	public static Future streamAndReturn(Runnable task){
		if(!isPoolStarted)
			initPool();
		
		return pool.submit(task);
	}
	
	public static void stream(Runnable task){
		if(!isPoolStarted)
			initPool();
		
		pool.execute(task);
	}
	
	public static void shutdown(){
		if(pool!=null) pool.shutdownNow();
		isPoolStarted=false;
	}
}
