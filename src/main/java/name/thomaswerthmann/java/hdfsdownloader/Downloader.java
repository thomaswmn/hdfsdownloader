package name.thomaswerthmann.java.hdfsdownloader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;

public class Downloader {
	private static final String OPTION_BASE = "hdfsdownloader";

	private static final String NUM_THREAD_OPTION = OPTION_BASE + ".threads";
	private static final int NUM_THREAD_DEFAULT = 0;

	private static final String MAX_BLOCK_SIZE_OPTION = OPTION_BASE + ".maxblocksize";
	private static final long MAX_BLOCK_SIZE_DEFAULT = Long.MAX_VALUE; // unlimited, see MAX_MMAP_SIZE

	/**
	 * this buffer here is used for copying, io.file.buffer.size determines the
	 * input buffer
	 */
	private static final String READ_BUFFER_OPTION = OPTION_BASE + ".readbuffer";
	private static final int READ_BUFFER_DEFAULT = 4 * 1024; // 4 kiB

	private static final String VERIFY_CHECKSUM_OPTION = OPTION_BASE + ".checksum.verify";
	private static final boolean VERIFY_CHECKSUM_DEFAULT = true;

	private static final String UNMAP_OPTION = OPTION_BASE + ".unmap";
	private static final boolean UNMAP_DEFAULT = false;

	private static final String NEW_FILESYSTEM_INSTANCE_OPTION = OPTION_BASE + ".filesystem.new";
	private static final boolean NEW_FILESYSTEM_INSTANCE_DEFAULT = false;

	final static long MAX_MMAP_SIZE = 1024 * 1024 * 1024; // 1 GiB, only applied in case buffer > Integer.MAX_SIZE

	private final ThreadLocal<byte[]> bufferPool;

	// max length of a copy block - should not exceed Integer.MAX_VALUE
	final long maxBlockSize;

	final boolean doUnmap;
	final boolean verifyChecksum;
	final boolean recreateFileSytemInstances;
	final int numThreads;
	final Configuration conf;

	public Downloader(Configuration conf) {
		this.conf = conf;
		numThreads = conf.getInt(NUM_THREAD_OPTION, NUM_THREAD_DEFAULT);
		maxBlockSize = conf.getLong(MAX_BLOCK_SIZE_OPTION, MAX_BLOCK_SIZE_DEFAULT);
		bufferPool = ThreadLocal.withInitial(() -> new byte[conf.getInt(READ_BUFFER_OPTION, READ_BUFFER_DEFAULT)]);
		verifyChecksum = conf.getBoolean(VERIFY_CHECKSUM_OPTION, VERIFY_CHECKSUM_DEFAULT);
		doUnmap = conf.getBoolean(UNMAP_OPTION, UNMAP_DEFAULT);
		recreateFileSytemInstances = conf.getBoolean(NEW_FILESYSTEM_INSTANCE_OPTION, NEW_FILESYSTEM_INSTANCE_DEFAULT);
	}

	public int getNumThreads() {
		return numThreads;
	}

	public void copyBlockwise(String file, String outFile)
			throws IOException, InterruptedException, URISyntaxException {
		try (final FileSystem fileSystem = FileSystem.get(new URI(file), conf)) {
			fileSystem.setVerifyChecksum(verifyChecksum);

			// get list of blocks / "tasks" for copy jobs
			final List<Block> blockList = buildBlockList(fileSystem, file);
			try (RandomAccessFile raFile = new RandomAccessFile(outFile, "rw")) {
				try (final FileChannel localFile = raFile.getChannel()) {
					if (numThreads >= 1) {
						final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

						final List<Future<Object>> futures = blockList.stream()
								.map(b -> copyToLocalWrapper(fileSystem, file, b, localFile)).map(threadPool::submit)
								.collect(Collectors.toList());

						// now shutdown and wait for all tasks to complete
						threadPool.shutdown();
						threadPool.awaitTermination(10, TimeUnit.MINUTES); // arbitrary timeout value

						// check the futures for exceptions
						for (Future<Object> f : futures) {
							try {
								f.get(); // result not relevant
							} catch (InterruptedException | ExecutionException e) {
								// delete file to avoid partial content
								new File(outFile).delete();
								// how to handle this?
								e.printStackTrace();
							}
						}

					} else {
						for (Block block : blockList) {
							copyToLocal(fileSystem, file, block, localFile);
						}
					}
				}
			}
		}
	}

	/*
	 * copies whole file to local fs - not used any more, serves as reference
	 */
	public void copyToLocal(String file, String outFile) throws IOException, URISyntaxException {
		try (final FileSystem fileSystem = FileSystem.get(new URI(file), conf)) {
			fileSystem.setVerifyChecksum(verifyChecksum);

			final Path path = new Path(file);
			if (!fileSystem.exists(path)) {
				System.out.println("File " + file + " does not exists");
				return;
			}
			try (final FSDataInputStream in = fileSystem.open(path)) {
				try (final OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outFile)))) {
					byte[] b = new byte[1024];
					int numBytes = 0;
					while ((numBytes = in.read(b)) > 0) {
						out.write(b, 0, numBytes);
					}
				}
			}
		}
	}

	/** create a list of blocks to copy from the file specified as parameter */
	private List<Block> buildBlockList(FileSystem fileSystem, String file) throws IOException {
		final Path path = new Path(file);
		final FileStatus stat = fileSystem.getFileStatus(path); // might throw FileNotFoundException
		final BlockLocation[] blocks = fileSystem.getFileBlockLocations(stat, 0, stat.getLen());
		final List<Block> list = BlockUtils.splitBlocks(
				Arrays.stream(blocks).map(bl -> new Block(bl.getOffset(), bl.getLength())).collect(Collectors.toList()),
				maxBlockSize, stat.getLen());
		// shuffle the list to avoid artificially split blocks to be read simultaneously
		Collections.shuffle(list);
		return list;
	}

	/**
	 * wrap {@link #copyToLocal(FileSystem, String, Block, FileChannel)} into a
	 * {@link Callable}, for multi-threading
	 */
	private Callable<Object> copyToLocalWrapper(FileSystem fileSystem, String file, Block block,
			FileChannel localFile) {
		return new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				copyToLocal(fileSystem, file, block, localFile);
				return null;
			}
		};
	}

	private void copyToLocal(FileSystem fileSystemExisting, String file, Block block, FileChannel localFile)
			throws IOException, URISyntaxException {

		final FileSystem fileSystem;
		if (recreateFileSytemInstances) {
			fileSystem = FileSystem.newInstance(new URI(file), conf);
			fileSystem.setVerifyChecksum(verifyChecksum);
		} else
			fileSystem = fileSystemExisting;

		try (final FSDataInputStream in = fileSystem.open(new Path(file))) {
			in.seek(block.offset);

			final List<Block> origBlockList = Collections.singletonList(block);
			final List<Block> blockList = block.length > Integer.MAX_VALUE
					? BlockUtils.splitBlocks(origBlockList, MAX_MMAP_SIZE, block.length)
					: origBlockList;
			final boolean useByteBufferCopy = in.hasCapability(StreamCapabilities.READBYTEBUFFER);

			for (Block mmBlock : blockList) {
				final MappedByteBuffer localBuf = localFile.map(MapMode.READ_WRITE, mmBlock.offset, mmBlock.length);

				if (useByteBufferCopy)
					copyBlockByteBuffer(in, localBuf, mmBlock.length);
				else
					copyBlockBytearray(in, localBuf, mmBlock.length);

				if (doUnmap)
					doUnmap(localBuf);
			}
		}

		if (recreateFileSytemInstances)
			fileSystem.close();
	}

	private static void doUnmap(MappedByteBuffer localBuf) {
		try { // try to clean the buffer to unmap the memory
			Method cleaner = localBuf.getClass().getMethod("cleaner");
			cleaner.setAccessible(true);
			Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
			clean.setAccessible(true);
			clean.invoke(cleaner.invoke(localBuf));
		} catch (Exception e) {
			// ignore - unmapping is just best effort, here
		}
	}

	/**
	 * 
	 * @param in       input stream, seeked to correct position to start
	 * @param localBuf output as memory mapped byte buffer
	 * @param numBytes number of bytes to copy
	 * @throws IOException
	 */
	private void copyBlockBytearray(FSDataInputStream in, MappedByteBuffer localBuf, long numBytes) throws IOException {
		final byte[] buf = bufferPool.get();

		long remaining = numBytes;
		while (remaining > 0) {
			final int bytes = in.read(buf);
			assert bytes > 0; // should not be 0 and not EOF
			final int bytesToWrite = (int) Math.min(bytes, remaining);
			remaining -= bytesToWrite;
			assert remaining >= 0;

			localBuf.put(buf, 0, bytesToWrite);
		}
	}

	private void copyBlockByteBuffer(FSDataInputStream in, MappedByteBuffer localBuf, long numBytes)
			throws IOException {
		while (localBuf.hasRemaining()) {
			final int bytes = in.read(localBuf);
			assert bytes > 0; // should not be 0 and not EOF
		}
	}

}
