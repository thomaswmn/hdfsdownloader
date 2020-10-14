package name.thomaswerthmann.java.hdfsdownloader;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BlockUtils {

	/**
	 * split a list of blocks into potentially smaller blocks, such that no block is
	 * larger than the specified size
	 * 
	 * @param blocks
	 * @param maxSize
	 * @param totalLengthForChecks set to negative value to skip this check
	 * @return
	 */
	static List<Block> splitBlocks(List<Block> blocks, long maxSize, long totalLengthForChecks) {
		final List<Block> list = blocks.stream().flatMap(b -> BlockUtils.splitBlock(b, maxSize))
				.collect(Collectors.toList());
		// now check the list
		final long totalLength = list.stream().mapToLong(b -> b.length).sum();
		if (totalLengthForChecks > 0 && totalLength != totalLengthForChecks)
			throw new RuntimeException();
		final long maxLength = list.stream().mapToLong(b -> b.length).max().getAsLong();
		if (maxLength > maxSize)
			throw new RuntimeException();
		// check no overlap
		final boolean overlap = list.stream().anyMatch(first -> list.stream().anyMatch(second -> (first != second
				&& first.offset <= second.offset && first.offset + first.length > second.offset)));
		if (overlap)
			throw new RuntimeException();
		return list;
	}

	// in case a block is longer than MAX_BLOCK_LENGTH, split it into parts not
	// larger than that
	private static Stream<Block> splitBlock(Block longBlock, long maxSize) {
		if (longBlock.length > maxSize) {
			final Block first = new Block(longBlock.offset, maxSize);
			final Block remainder = new Block(longBlock.offset + maxSize,
					longBlock.length - maxSize);
			return Stream.concat(Collections.singleton(first).stream(), splitBlock(remainder, maxSize));
		} else {
			return Collections.singleton(longBlock).stream();
		}
	}

}
