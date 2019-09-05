package govind.incubator.network.buffer;

import com.google.common.io.Closeables;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 基于Netty的零拷贝实现数据直接拷贝到指定Channel而不需要通过中间缓
 * 存区多用一次内存拷贝。
 *
 * LazyFileRegion只有在region准备被传输时才会创建FileChannel,而Netty
 * 默认的{@link io.netty.channel.DefaultFileRegion}需要显示创建。
 * 默认Netty不支持lazy级别的创建方式！
 */
public class LazyFileRegion extends AbstractReferenceCounted implements FileRegion{
	private final File file;
	private final long offset;
	private final long length;

	private FileChannel fileChannel;
	private long numBytesTransferred = 0L;

	/**
	 * @param file   需要拷贝的文件
	 * @param offset 开始拷贝的文件位置
	 * @param length 从offset开始拷贝的内容长度
	 */
	public LazyFileRegion(File file, long offset, long length) {
		this.file = file;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public long position() {
		return offset;
	}

	@Override
	public long transfered() {
		return numBytesTransferred;
	}

	@Override
	public long count() {
		return length;
	}

	@Override
	public long transferTo(WritableByteChannel target, long position) throws IOException {
		if (fileChannel == null) {
			fileChannel = new FileInputStream(file).getChannel();
		}

		long count = this.length - position;
		if (count < 0 || position < 0) {
			throw new IllegalArgumentException("position out of range: " + position + "(expected: 0-" + (length - 1) + ")");
		}

		if (count == 0) {
			return 0L;
		}

		long written = fileChannel.transferTo(this.offset + position,  count, target);
		if (written > 0) {
			numBytesTransferred += written;
		}
		return written;
	}

	@Override
	protected void deallocate() {
		Closeables.closeQuietly(fileChannel);
	}
}

