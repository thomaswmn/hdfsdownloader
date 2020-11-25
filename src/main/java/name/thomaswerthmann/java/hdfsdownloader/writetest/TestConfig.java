package name.thomaswerthmann.java.hdfsdownloader.writetest;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class TestConfig {
	final static String OPTION_BASE = "writetest";

	final static String OPTION_NUM_BYTES = ".numbytes";
	final static long DEFAULT_NUM_BYTES = 6l * 1024 * 1024 * 1024; // 6G

	final static String OPTION_FILENAME = ".filename";
	final static String DEFAULT_FILENAME = "/tmp/writetest.out";

	final static long getLongProp(String name, long defValue) {
		return Long.valueOf(System.getProperty(OPTION_BASE + name, Long.toString(defValue)));
	}

	final static String getStringProp(String name, String defValue) {
		return System.getProperty(OPTION_BASE + name, defValue);
	}

	enum WriteMode {
		MMAP, CHANNEL
	}

	final TestConfig.WriteMode mode;
	final long numBytes;
	final File outFile;
	final boolean doDelete;
	final int numIterations = 10;
	final int bufSize;
	final int numThreads;
	final int fallocateMode;

	public TestConfig(WriteMode mode, boolean doDelete, int bufSize, int numThreads, int fallocateMode) {
		this.mode = mode;
		this.numBytes = getLongProp(OPTION_NUM_BYTES, DEFAULT_NUM_BYTES);
		this.outFile = new File(getStringProp(OPTION_FILENAME, DEFAULT_FILENAME));
		this.doDelete = doDelete;
		this.bufSize = bufSize;
		this.numThreads = numThreads;
		this.fallocateMode = fallocateMode;
	}

	@Override
	public String toString() {
		return "size: " + numBytes + " mode: " + mode.toString() + " delete: " + doDelete + " buffer: " + bufSize
				+ (numThreads > 0 ? " (" + numThreads + " threads)" : "")
				+ (fallocateMode > -1 ? " fallocate=" + fallocateMode : "");
	}

	public static List<TestConfig> all() {
		return Arrays.asList(new TestConfig[] { //
				new TestConfig(WriteMode.CHANNEL, true, 128 * 1024, 0, -1), //
				new TestConfig(WriteMode.CHANNEL, true, 128 * 1024, 0, 0), //
				new TestConfig(WriteMode.CHANNEL, true, 128 * 1024, 0, 1), //
				new TestConfig(WriteMode.MMAP, true, 128 * 1024, 0, -1), //
				new TestConfig(WriteMode.MMAP, true, 128 * 1024, 0, 0), //
				new TestConfig(WriteMode.MMAP, true, 128 * 1024, 0, 1), //
				new TestConfig(WriteMode.CHANNEL, false, 128 * 1024, 0, -1), //
				new TestConfig(WriteMode.CHANNEL, false, 128 * 1024, 1, -1), //
				new TestConfig(WriteMode.CHANNEL, false, 128 * 1024, 2, -1), //
				new TestConfig(WriteMode.CHANNEL, false, 128 * 1024, 4, -1), //

		});
	}

}
