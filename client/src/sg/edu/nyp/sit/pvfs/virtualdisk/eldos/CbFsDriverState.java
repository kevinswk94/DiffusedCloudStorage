package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

public enum CbFsDriverState {
	CBFS_SERVICE_STOPPED(1),
    CBFS_SERVICE_START_PENDING(2),
    CBFS_SERVICE_STOP_PENDING(3),
    CBFS_SERVICE_RUNNING(4),
    CBFS_SERVICE_CONTINUE_PENDING(5),
    CBFS_SERVICE_PAUSE_PENDING(6),
    CBFS_SERVICE_PAUSED(7);
    
    private long state;
    CbFsDriverState(long state){
    	this.state=state;
    }
    
    public long value(){
    	return state;
    }
}
