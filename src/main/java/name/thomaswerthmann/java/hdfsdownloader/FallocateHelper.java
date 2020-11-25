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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.EnumSet;

import com.sun.jna.Native;
import com.sun.jna.Platform;

public class FallocateHelper {
	/** see falloc.h */
	static enum Mode {
		KEEP_SIZE(0x01), PUNCH_HOLE(0x02), COLLAPSE_RANGE(0x08), ZERO_RANGE(0x10), UNSHARE_RANGE(0x40),
		INSERT_RANGE(0x20);

		int flag;

		Mode(int flag) {
			this.flag = flag;
		}
	}

	static {
		if (!Platform.isLinux())
			throw new RuntimeException("fallocate only supported on Linux");
		Native.register(Platform.C_LIBRARY_NAME);
	}

	private static native int fallocate(int fd, int mode, long offset, long length);

	public static void fallocate(FileChannel channel, long offset, long length, EnumSet<Mode> mode) throws IOException {
		fallocate(channel, offset, length, getCombinedMode(mode));
	}

	public static void fallocate(FileChannel channel, long offset, long length, int mode) throws IOException {
		final int fd = FileDescriptorHelper.getDescriptor(channel);
		final int result = FallocateHelper.fallocate(fd, mode, offset, length);
		final int errno = result == 0 ? 0 : Native.getLastError();
		if (errno != 0)
			throw new IOException("fallocate failed with errno = " + errno);
	}

	private static int getCombinedMode(EnumSet<Mode> mode) {
		return mode.stream().mapToInt(m -> m.flag).sum();
	}



}
