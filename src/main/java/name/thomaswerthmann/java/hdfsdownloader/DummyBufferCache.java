package name.thomaswerthmann.java.hdfsdownloader;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DummyBufferCache {
	private static ThreadLocal<Map<Integer, ByteBuffer>> CACHE = ThreadLocal.withInitial(HashMap::new);

	public static ByteBuffer get(int size) {
		final Map<Integer, ByteBuffer> cache = CACHE.get();
		if (cache.containsKey(size)) {
			final ByteBuffer buf = cache.get(size);
			buf.rewind();
			return buf;
		} else {
			final ByteBuffer buf = ByteBuffer.allocateDirect(size);
			cache.put(size, buf);
			return buf;
		}
	}
}
