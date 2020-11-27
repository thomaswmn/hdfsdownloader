package name.thomaswerthmann.java.hdfsdownloader.writetest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import name.thomaswerthmann.java.hdfsdownloader.FallocateHelper;
import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

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
		case ASYNC:
			runSingleAsync();
			break;
		case DIRECT:
			runSingleDirectIO();
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

	/**
	 * Using the Java {@link AsynchronousFileChannel} API. Note that on Linux, this
	 * uses a thread pool and normal blocking IO.
	 * 
	 * @throws IOException
	 */
	public void runSingleAsync() throws IOException {
		try (AsynchronousFileChannel ch = AsynchronousFileChannel.open(conf.outFile.toPath(), StandardOpenOption.WRITE,
				StandardOpenOption.CREATE)) {
			assert conf.fallocateMode == -1 : "ASYNC + fallocate not supported";

			final ByteBuffer buf = ByteBuffer.allocateDirect(conf.bufSize);
			long remaining = conf.numBytes;
			long offset = 0;

			AtomicLong inflight = new AtomicLong(0);

			final CompletionHandler<Integer, Integer> handler = new CompletionHandler<Integer, Integer>() {
				@Override
				public void completed(Integer result, Integer attachment) {
					inflight.getAndDecrement();
					if (result.intValue() != attachment.intValue())
						throw new RuntimeException("short write");
				}

				@Override
				public void failed(Throwable exc, Integer attachment) {
					inflight.getAndDecrement();
					throw new RuntimeException("write failed", exc);
				}
			};

			while (remaining > 0) {
				final long size = Math.min(remaining, buf.capacity());
				submitAsyncWrite(ch, buf.duplicate(), size, offset, handler);
				inflight.getAndIncrement();

				remaining -= size;
				offset += size;
				assert remaining >= 0;
			}

			while (inflight.get() > 0)
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}

	private void submitAsyncWrite(AsynchronousFileChannel ch, ByteBuffer buf, long numBytes, long offset,
			CompletionHandler<Integer, Integer> handler) {

		buf.clear();
		if (numBytes < buf.remaining())
			buf.limit((int) numBytes);
		assert numBytes == buf.remaining();

		ch.write(buf, offset, buf.remaining(), handler);
	}

	public void runSingleDirectIO() throws IOException {
		try (final BufferedChannel<AlignedDirectByteBuffer> ch = DirectIoByteChannel.getChannel(conf.outFile, false)) {

			final AlignedDirectByteBuffer buf = AlignedDirectByteBuffer
					.allocate(DirectIoLib.getLibForPath(conf.outFile.getAbsolutePath()), conf.bufSize);

			long remaining = conf.numBytes;
			long offset = 0;
			while (remaining > 0) {
				buf.clear();
				if (remaining < buf.remaining())
					buf.limit((int) remaining);
				assert remaining >= buf.remaining();

				final int written = ch.write(buf, offset);
				remaining -= written;
				offset += written;
				assert remaining >= 0;
			}
		}
	}
}
