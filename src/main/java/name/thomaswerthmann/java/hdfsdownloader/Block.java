package name.thomaswerthmann.java.hdfsdownloader;

/**
 * internal class to store information related to a copy-block - not necessarily
 * a HDFS block
 */
class Block {
	final long offset;
	final long length;

	public Block(long offset, long length) {
		this.offset = offset;
		this.length = length;
	}
}