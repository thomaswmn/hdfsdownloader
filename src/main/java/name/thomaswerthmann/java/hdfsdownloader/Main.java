package name.thomaswerthmann.java.hdfsdownloader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	private static int READ_BUF = 1024 * 64;

	public static void main(String[] args) throws IOException, InterruptedException {
		final String file = args[0];
		final String outFile = args[1];
		final int numThreads = Integer.parseInt(args[2]);

		final Configuration conf = new Configuration();
		// optionally set configuration here
		try (final FileSystem fileSystem = FileSystem.get(conf)) {
			// copyToLocal(fileSystem, file);
			final List<Block> blockList = buildBlockList(fileSystem, file);
			try (RandomAccessFile raFile = new RandomAccessFile(outFile, "rw")) {
				try (final FileChannel localFile = raFile.getChannel()) {

					if (numThreads >= 1) {
						final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
						blockList.stream().map(block -> new Runnable() {
							@Override
							public void run() {
								try {
									copyToLocal(fileSystem, file, block, localFile);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}).forEach(threadPool::execute);
						threadPool.shutdown();
						threadPool.awaitTermination(10, TimeUnit.MINUTES);
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
	 * copies whole file to local fs
	 */
	private static void copyToLocal(String file, FileSystem fileSystem, String outFile) throws IOException {
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
		if (maxLength > MAX_BLOCK_LENGTH)
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

//	final static long MAX_BLOCK_LENGTH = 1024 * 1024 * 1024; // 1GiB
	final static long MAX_BLOCK_LENGTH = 128 * 1024 * 1024; // 128MiB

	// in case a block is longer than MAX_BLOCK_LENGTH, split it into parts not
	// larger than that
	private static Stream<Block> splitBlock(Block longBlock) {
		if (longBlock.length > MAX_BLOCK_LENGTH) {
			final Block first = new Block(longBlock.offset, MAX_BLOCK_LENGTH);
			final Block remainder = new Block(longBlock.offset + MAX_BLOCK_LENGTH, longBlock.length - MAX_BLOCK_LENGTH);
			return Stream.concat(Collections.singleton(first).stream(), splitBlock(remainder));

		} else {
			return Collections.singleton(longBlock).stream();
		}
	}

	private static void copyToLocal(FileSystem fileSystem, String file, Block block, FileChannel localFile)
			throws IOException {
		// final ByteBuffer buf = ByteBuffer.allocateDirect(READ_BUF);
		final byte[] buf = new byte[READ_BUF];
		try (final FSDataInputStream in = fileSystem.open(new Path(file), READ_BUF)) {
			in.seek(block.offset);
			long remaining = block.length;
			final MappedByteBuffer localBuf = localFile.map(MapMode.READ_WRITE, block.offset, block.length);
			while (remaining > 0) {
				final int bytes = in.read(buf);
				assert bytes > 0; // should not be 0 and not EOF
				final int bytesToWrite = (int) Math.min(bytes, remaining);
				remaining -= bytesToWrite;
				assert remaining >= 0;

				localBuf.put(buf, 0, bytesToWrite);
			}

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
}
