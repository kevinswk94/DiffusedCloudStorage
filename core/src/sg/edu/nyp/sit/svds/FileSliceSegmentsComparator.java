package sg.edu.nyp.sit.svds;

import java.util.Comparator;

import sg.edu.nyp.sit.svds.metadata.FileSliceInfo;

/**
 * Comparator class to sort the FileSliceInfo objects in a list by the offset property.
 * 
 * @author Victoria Chin
 * @version %I% %G%
 * @see java.util.Comparator
 */
public class FileSliceSegmentsComparator implements Comparator<FileSliceInfo> {
	public static final long serialVersionUID = 1L;
	/**
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(FileSliceInfo arg0, FileSliceInfo arg1) {
		return (arg0.getTimestamp()<arg1.getTimestamp() ? -1 :
			(arg0.getTimestamp()>arg1.getTimestamp() ? 1 :
				(arg0.getOffset()<arg1.getOffset() ? -1 : 
					(arg0.getOffset()==arg1.getOffset() ? 0 : 1))));
	}
}