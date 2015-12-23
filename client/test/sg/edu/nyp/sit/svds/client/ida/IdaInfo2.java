package sg.edu.nyp.sit.svds.client.ida;

public class IdaInfo2 {
	private int inputStreamSize; //N
	private int outputSegmentCount = 5; //n
	private int lostThreshold = 2; //k

	public int getOutputSegmentCount() {
		return outputSegmentCount;
	}
	public void setOutputSegmentCount(int outputSegmentCount) {
		this.outputSegmentCount = outputSegmentCount;
	}
	public int getLostThreshold() {
		return lostThreshold;
	}
	public void setLostThreshold(int lostThreshold) {
		this.lostThreshold = lostThreshold;
	}

	public int getInputStreamSize() {
		return inputStreamSize;
	}
	public void setInputStreamSize(int inputStreamSize) {
		this.inputStreamSize = inputStreamSize;
	}
	public int getInputSegmentLength() {
		return outputSegmentCount - lostThreshold;
	}
	
	//N/m
	public int getOutputSegmentLength()
	{
		return getInputStreamSize()/getInputSegmentLength();
	}
	
}
