package name.thomaswerthmann.java.hdfsdownloader.writetest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;

public class WriteTest {

	final static int MAX_MMAP_SIZE = 1024 * 1024 * 1024; // 1G

	public static void main(String[] args) throws IOException {
		for (TestConfig conf : TestConfig.all())
			new WriteTest(conf).run();
	}

	private final TestConfig conf;

	public WriteTest(TestConfig conf) {
		this.conf = conf;
	}

	public void run() throws IOException {
		// delete file automatically after program is done
		conf.outFile.deleteOnExit();
		// warmup
		for (int i = 0; i < conf.numIterations; i++)
			runSingle();
		final long startTime = System.currentTimeMillis();
		for (int i = 0; i < conf.numIterations; i++)
			runSingle();
		final long endTime = System.currentTimeMillis();
		final double duration = 1.0 * (endTime - startTime) / conf.numIterations / 1000.0;
		final double gigaBytesPerSecond = conf.numBytes / duration / (1024 * 1024 * 1024);
		System.out.println(conf.toString() + "   duration " + duration + " s (" + gigaBytesPerSecond + "GiB/s)");
	}

	public void runSingle() throws IOException {
		if (conf.doDelete && conf.outFile.isFile())
			conf.outFile.delete();
		switch (conf.mode) {
		case MMAP:
			runSingleMmap();
			break;
		case CHANNEL:
			runSingleChannel();
			break;
		default:
			throw new UnsupportedOperationException();
		}
		// trigger GC here - if not done here, and memory is not unmapped (for MMAP
		// operation), this causes a SIGBUS. Too much memory mmapped on my desktop
		// machine???
		System.gc();
	}

	public void runSingleMmap() throws IOException {
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
}
