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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		final Configuration conf = new Configuration();
		final String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		final String file = extraArgs[0]; // HDFS URI syntax
		final String outFile = extraArgs[1]; // local path

		final long timeStart = System.currentTimeMillis();
		final Downloader d = new Downloader(conf);
		// d.copyToLocal(file, outFile);
		d.copyBlockwise(file, outFile);
		final long timeEnd = System.currentTimeMillis();
		try (final FileSystem fileSystem = FileSystem.get(new URI(file), conf)) {
			printDurationAndThroughput(timeStart, timeEnd, getFileSize(fileSystem, file), d.getNumThreads(),
					d.getFallocateDurationMillis());
		}
	}

	private static void printDurationAndThroughput(long timeStart, long timeEnd, long fileSize, int numThreads,
			long fallocateDurationMillis) {
		final double duration = (timeEnd - timeStart) / 1000.0;
		final double bytesPerSecond = fileSize / duration;
		final double mebiBytesPerSecond = bytesPerSecond / 1024 / 1024;
		System.out.println(String.format(
				"transferred %,.1f MiB using %d threads in %,.1f s --> %,.1f MiB/s (fallocate took %,.1f s)",
				fileSize / 1024.0 / 1024.0, numThreads, duration, mebiBytesPerSecond,
				fallocateDurationMillis / 1000.0));
	}

	private static long getFileSize(FileSystem fileSystem, String file) throws IllegalArgumentException, IOException {
		return fileSystem.getFileStatus(new Path(file)).getLen();
	}

}
