/*
 * Copyright 2020 Thomas Werthmann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
	private static final String OPTION_BASE = "hdfsdownloader";

	private static final String NUM_THREAD_OPTION = OPTION_BASE + ".threads";
	private static final int NUM_THREAD_DEFAULT = 0;

	private static final String MAX_BLOCK_SIZE_OPTION = OPTION_BASE + ".maxblocksize";
	private static final long MAX_BLOCK_SIZE_DEFAULT = 1024 * 1024 * 1024; // 1 GiB

	private static final String READ_BUFFER_OPTION = OPTION_BASE + ".readbuffer";
	private static final int READ_BUFFER_DEFAULT = 4 * 1024; // 4 kiB

	private static final String VERIFY_CHECKSUM_OPTION = OPTION_BASE + ".checksum.verify";
	private static final boolean VERIFY_CHECKSUM_DEFAULT = true;

	// size of the buffer for reading
	private static int READ_BUF;

	// max length of a copy block - should not exceed Integer.MAX_VALUE
	static long MAX_BLOCK_SIZE;

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		final Configuration conf = new Configuration();
		String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		final String file = extraArgs[0]; // HDFS URI syntax
		final String outFile = extraArgs[1]; // local path

		final int numThreads = conf.getInt(NUM_THREAD_OPTION, NUM_THREAD_DEFAULT);
		MAX_BLOCK_SIZE = conf.getLong(MAX_BLOCK_SIZE_OPTION, MAX_BLOCK_SIZE_DEFAULT);
		READ_BUF = conf.getInt(READ_BUFFER_OPTION, READ_BUFFER_DEFAULT);
		final boolean verifyChecksum = conf.getBoolean(VERIFY_CHECKSUM_OPTION, VERIFY_CHECKSUM_DEFAULT);

		try (final FileSystem fileSystem = FileSystem.get(new URI(file), conf)) {
			fileSystem.setVerifyChecksum(verifyChecksum);
			final long timeStart = System.currentTimeMillis();
			// copyToLocal(fileSystem, file, outFile);
			copyBlockwise(fileSystem, file, outFile, numThreads);
			final long timeEnd = System.currentTimeMillis();
			printDurationAndThroughput(timeStart, timeEnd, getFileSize(fileSystem, file));
		}
	}

	private static void printDurationAndThroughput(long timeStart, long timeEnd, long fileSize) {
		final double duration = (timeEnd - timeStart) / 1000.0;
		final double bytesPerSecond = fileSize / duration;
		final double mebiBytesPerSecond = bytesPerSecond / 1024 / 1024;
		System.out.println(String.format("transferred %,.1f MiB in %,.1f s --> %,.1f MiB/s", fileSize / 1024.0 / 1024.0,
				duration, mebiBytesPerSecond));
	}

	private static long getFileSize(FileSystem fileSystem, String file) throws IllegalArgumentException, IOException {
		return fileSystem.getFileStatus(new Path(file)).getLen();
	}

	private static void copyBlockwise(FileSystem fileSystem, String file, String outFile, int numThreads)
			throws IOException, InterruptedException {
		// get list of blocks / "tasks" for copy jobs
		final List<Block> blockList = buildBlockList(fileSystem, file);
		try (RandomAccessFile raFile = new RandomAccessFile(outFile, "rw")) {
			try (final FileChannel localFile = raFile.getChannel()) {
				if (numThreads >= 1) {
					final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

					final List<Future<Object>> futures = blockList.stream().map(block -> new Callable<Object>() {
						@Override
						public Object call() throws IOException {
							copyToLocal(fileSystem, file, block, localFile);
							return null;
						}
					}).map(threadPool::submit).collect(Collectors.toList());

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

	/*
	 * copies whole file to local fs - not used any more, serves as reference
	 */
	@SuppressWarnings("unused")
	private static void copyToLocal(FileSystem fileSystem, String file, String outFile) throws IOException {
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

	/**
	 * internal class to store information related to a copy-block - not necessarily
	 * a HDFS block
	 */
	private static class Block {
		final long offset;
		final long length;

		public Block(long offset, long length) {
			this.offset = offset;
			this.length = length;
		}
	}

	private static List<Block> buildBlockList(FileSystem fileSystem, String file) throws IOException {
		final Path path = new Path(file);
		final FileStatus stat = fileSystem.getFileStatus(path); // might throw FileNotFoundException
		final BlockLocation[] blocks = fileSystem.getFileBlockLocations(stat, 0, stat.getLen());
		final List<Block> list = Arrays.stream(blocks).map(bl -> new Block(bl.getOffset(), bl.getLength()))
				.flatMap(Main::splitBlock).collect(Collectors.toList());
		// now check the list
		final long totalLength = list.stream().mapToLong(b -> b.length).sum();
		if (totalLength != stat.getLen())
			throw new RuntimeException();
		final long maxLength = list.stream().mapToLong(b -> b.length).max().getAsLong();
		if (maxLength > MAX_BLOCK_SIZE)
			throw new RuntimeException();
		// check no overlap
		final boolean overlap = list.stream().anyMatch(first -> list.stream().anyMatch(second -> (first != second
				&& first.offset <= second.offset && first.offset + first.length > second.offset)));
		if (overlap)
			throw new RuntimeException();
		// shuffle the list to avoid artificially split blocks to be read simultaneously
		Collections.shuffle(list);
		return list;
	}

	// in case a block is longer than MAX_BLOCK_LENGTH, split it into parts not
	// larger than that
	private static Stream<Block> splitBlock(Block longBlock) {
		if (longBlock.length > MAX_BLOCK_SIZE) {
			final Block first = new Block(longBlock.offset, MAX_BLOCK_SIZE);
			final Block remainder = new Block(longBlock.offset + MAX_BLOCK_SIZE, longBlock.length - MAX_BLOCK_SIZE);
			return Stream.concat(Collections.singleton(first).stream(), splitBlock(remainder));

		} else {
			return Collections.singleton(longBlock).stream();
		}
	}

	final static ThreadLocal<byte[]> bufferPool = ThreadLocal.withInitial(() -> new byte[READ_BUF]);

	private static void copyToLocal(FileSystem fileSystem, String file, Block block, FileChannel localFile)
			throws IOException {
		try (final FSDataInputStream in = fileSystem.open(new Path(file), READ_BUF)) {
			in.seek(block.offset);
			final MappedByteBuffer localBuf = localFile.map(MapMode.READ_WRITE, block.offset, block.length);

			if (in.hasCapability(StreamCapabilities.READBYTEBUFFER))
				copyBlockByteBuffer(in, localBuf, block.length);
			else
				copyBlockBytearray(in, localBuf, block.length);

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
	}

	/**
	 * 
	 * @param in       input stream, seeked to correct position to start
	 * @param localBuf output as memory mapped byte buffer
	 * @param numBytes number of bytes to copy
	 * @throws IOException
	 */
	private static void copyBlockBytearray(FSDataInputStream in, MappedByteBuffer localBuf, long numBytes)
			throws IOException {
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

	private static void copyBlockByteBuffer(FSDataInputStream in, MappedByteBuffer localBuf, long numBytes)
			throws IOException {
		while (localBuf.hasRemaining()) {
			final int bytes = in.read(localBuf);
			assert bytes > 0; // should not be 0 and not EOF
		}
	}

}
