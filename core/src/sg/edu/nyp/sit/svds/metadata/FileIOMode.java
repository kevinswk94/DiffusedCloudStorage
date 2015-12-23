package sg.edu.nyp.sit.svds.metadata;

/**
 * Modes that the slice store supports.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 */
public enum FileIOMode{
	/**
	 * Streaming mode, allow random read/write, including non-random read/write.
	 */
	STREAM(1),
	/**
	 * Non-streaming mode, does not allow random read/write.
	 */
	NON_STREAM(0),
	/**
	 * Both streaming and non-streaming mode, used when selecting slice store servers
	 * to indicate all can be selected.
	 */
	BOTH(2),
	/**
	 * For file change mode to indicate that the file is not in the mist of changing mode.
	 */
	NONE(-1);
	
	private int mode;
	FileIOMode(int mode){ this.mode=mode; }
	/**
	 * Gets the integer representation of the mode.
	 * 
	 * @return Integer representation of the mode.
	 */
	public int value() { return mode; }
	/**
	 * Gets the enum value of the mode based on the integer representation of the mode.
	 * 
	 * @param mode Integer representation of the mode.
	 * @return enum value that matches the integer representation of the mode; or null if no matches is found.
	 */
	public static FileIOMode valueOf(int mode){
		switch(mode){
			case 0: return NON_STREAM;
			case 1: return STREAM;
			case 2: return BOTH;
			case -1: return NONE;
			default: return null;
		}
	}
}
