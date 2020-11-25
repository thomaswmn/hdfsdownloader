package name.thomaswerthmann.java.hdfsdownloader.writetest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import name.thomaswerthmann.java.hdfsdownloader.FallocateHelper;

public class WriteTest {

	final static int MAX_MMAP_SIZE = 1024 * 1024 * 1024; // 1G

	public static void main(String[] args) throws IOException, InterruptedException {
		for (TestConfig conf : TestConfig.all())
			new WriteTest(conf).run();
	}

	private final TestConfig conf;
	private long pauseAndGCTime = 0;

	public WriteTest(TestConfig conf) {
		this.conf = conf;
	}

	public void run() throws IOException, InterruptedException {
		// delete file automatically after program is done
		conf.outFile.deleteOnExit();
		// warmup
		for (int i = 0; i < conf.numIterations; i++)
			runSingle();
		pauseAndGCTime = 0;
		final long startTime = System.currentTimeMillis();
		for (int i = 0; i < conf.numIterations; i++)
			runSingle();
		final long endTime = System.currentTimeMillis();
		final double duration = 1.0 * (endTime - startTime - pauseAndGCTime) / conf.numIterations / 1000.0;
		final double gigaBytesPerSecond = conf.numBytes / duration / (1024 * 1024 * 1024);
		System.out.println(conf.toString() + "   duration " + duration + " s (" + gigaBytesPerSecond + "GiB/s)");
	}

	public void runSingle() throws IOException, InterruptedException {
		if (conf.doDelete && conf.outFile.isFile())
			conf.outFile.delete();
		switch (conf.mode) {
		case MMAP:
			runSingleMmap();
			break;
		case CHANNEL:
			if (conf.numThreads > 0)
				runSingleMTChannel();
			else
				runSingleChannel();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		final long pauseStart = System.currentTimeMillis();
		{
			// trigger GC here - if not done here, and memory is not unmapped (for MMAP
			// operation), this causes a SIGBUS. Too much memory mmapped on my desktop
			// machine???
			System.gc();
			Thread.sleep(500); // try to wait some time, so that the system can delete it's files
		}
		final long pauseEnd = System.currentTimeMillis();
		pauseAndGCTime += (pauseEnd - pauseStart);
	}

	private void applyFallocate(FileChannel ch) throws IOException {
		if (conf.fallocateMode > -1)
			FallocateHelper.fallocate(ch, 0, conf.numBytes, conf.fallocateMode);
	}

	public void runSingleMmap() throws IOException {
		if (conf.fallocateMode > -1)
			try (FileChannel ch = FileChannel.open(conf.outFile.toPath(), StandardOpenOption.WRITE,
					StandardOpenOption.CREATE)) {
				applyFallocate(ch);
			}

		try (FileChannel ch = FileChannel.open(conf.outFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ,
				StandardOpenOption.CREATE)) {
			final ByteBuffer buf = ByteBuffer.allocateDirect(conf.bufSize);
			long pos = 0;
			long remaining = conf.numBytes;
			while (remaining > 0) {
				final long mapSize = Math.min(remaining, MAX_MMAP_SIZE);
				final MappedByteBuffer map = ch.map(MapMode.READ_WRITE, pos, mapSize);
				assert map.remaining() == mapSize;
				while (map.hasRemaining()) {
					buf.clear();
					if (buf.remaining() > map.remaining())
						buf.limit(map.remaining());
					map.put(buf);
				}
				pos += mapSize;
				remaining -= mapSize;
				assert pos <= conf.numBytes;
				assert remaining >= 0;
				// map.force();
//				Downloader.doUnmap(map);
			}
		}
	}

	public void runSingleChannel() throws IOException {
		try (FileChannel ch = FileChannel.open(conf.outFile.toPath(), StandardOpenOption.WRITE,
				StandardOpenOption.CREATE)) {
			applyFallocate(ch);
			final ByteBuffer buf = ByteBuffer.allocateDirect(conf.bufSize);
			long remaining = conf.numBytes;
			while (remaining > 0) {
				buf.clear();
				if (remaining < buf.remaining())
					buf.limit((int) remaining);
				assert remaining >= buf.remaining();

				final int written = ch.write(buf);
				remaining -= written;
				assert remaining >= 0;
			}
		}
	}

	public void runSingleMTChannel() throws IOException, InterruptedException {
		try (FileChannel ch = FileChannel.open(conf.outFile.toPath(), StandardOpenOption.WRITE,
				StandardOpenOption.CREATE)) {
			applyFallocate(ch);
			final ExecutorService threadPool = Executors.newFixedThreadPool(conf.numThreads);
			final long bytesPerThread = conf.numBytes / conf.numThreads;

			final List<Callable<Long>> jobs = IntStream.range(0, conf.numThreads).mapToObj(i -> {
				final long initialOffset = i * bytesPerThread;
				return new Callable<Long>() {
					@Override
					public Long call() throws Exception {
						final ByteBuffer buf = ByteBuffer.allocateDirect(conf.bufSize);
						long remaining = bytesPerThread;
						long numWritten = 0;
						while (remaining > 0) {
							buf.clear();
							if (remaining < buf.remaining())
								buf.limit((int) remaining);
							assert remaining >= buf.remaining();

							final int written = ch.write(buf, initialOffset + numWritten);
							remaining -= written;
							assert remaining >= 0;
							numWritten += written;
						}
						return numWritten;
					}
				};
			}).collect(Collectors.toList());
			final List<Future<Long>> results = threadPool.invokeAll(jobs);
			threadPool.shutdown();
			threadPool.awaitTermination(10, TimeUnit.MINUTES);
			final long totalBytes = results.stream().mapToLong(t -> {
				try {
					return t.get();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}).sum();
			assert totalBytes == conf.numBytes;
		}
	}

}
