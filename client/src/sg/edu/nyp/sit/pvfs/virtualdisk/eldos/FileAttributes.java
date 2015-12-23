package sg.edu.nyp.sit.pvfs.virtualdisk.eldos;

public enum FileAttributes {
    //     The file is read-only.
    READ_ONLY(1),
    //     The file is hidden, and thus is not included in an ordinary directory listing.
    HIDDEN(2),
    //     The file is a system file. The file is part of the operating system or is
    //     used exclusively by the operating system.
    SYSTEM(4),
    //     The file is a directory.
    DIRECTORY(16),
    //     The file's archive status. Applications use this attribute to mark files
    //     for backup or removal.
    ARCHIVE(32),
    //     Reserved for future use.
    DEVICE(64),
    //     The file is normal and has no other attributes set. This attribute is valid
    //     only if used alone.
    NORMAL(128),
    //     The file is temporary. File systems attempt to keep all of the data in memory
    //     for quicker access rather than flushing the data back to mass storage. A
    //     temporary file should be deleted by the application as soon as it is no longer
    //     needed.
    TEMPORARY(256),
    //     The file is a sparse file. Sparse files are typically large files whose data
    //     are mostly zeros.
    SPARSE_FILE(512),

    //     The file contains a reparse point, which is a block of user-defined data
    //     associated with a file or a directory.
    REPARSE_POINT(1024),
    //     The file is compressed.
    COMPRESSED(2048),
    //     The file is offline. The data of the file is not immediately available.
    OFFLINE(4096),
    //     The file will not be indexed by the operating system's content indexing service.
    NOT_CONTENT_INDEXED(8192),
    //     The file or directory is encrypted. For a file, this means that all data
    //     in the file is encrypted. For a directory, this means that encryption is
    //     the default for newly created files and directories.
    ENCRYPTED(16384);
    
    private int value;
    FileAttributes(int value){
    	this.value=value;
    }
    public int value(){
    	return value;
    }
    
    public static FileAttributes valueOf(int value){
    	switch(value){
    		case 1: return READ_ONLY;
    		case 2: return HIDDEN;
    		case 4: return SYSTEM;
    		case 16: return DIRECTORY;
    		case 32: return ARCHIVE;
    		case 64: return DEVICE;
    		case 128: return NORMAL;
    		case 256: return TEMPORARY;
    		case 512: return SPARSE_FILE;
    		case 1024: return REPARSE_POINT;
    		case 2048: return COMPRESSED;
    		case 4096: return OFFLINE;
    		case 8192: return NOT_CONTENT_INDEXED;
    		case 16384: return ENCRYPTED;
    		default: throw new NullPointerException("Invalid file attribute value.");
    	}
    }
}
