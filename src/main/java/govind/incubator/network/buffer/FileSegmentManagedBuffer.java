package govind.incubator.network.buffer;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import govind.incubator.network.util.LimitInputStream;
import io.netty.channel.DefaultFileRegion;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 文件中的某部分数据
 */
public class FileSegmentManagedBuffer extends ManagedBuffer{

	/**
	 * 懒加载
	 */
 	static boolean lazyFileDescription = true;

	/**
	 * 使用直接映射内存最小大小
	 */
	static int MEMEPRY_MAP_BYTES = 2 * 1024 * 1024;

	/**
	 * 数据所在文件
	 */
	final File file;

	/**
	 * 从文件offset开始的数据
	 */
	final long offset;

	/**
	 * 从文件offset开始，长度为length的数据
	 */
	final long length;

	public FileSegmentManagedBuffer(File file, long offset, long length) {
		this.file = file;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public long size() {
		return length;
	}

	@Override
	public ManagedBuffer retain() {
		return this;
	}

	@Override
	public ManagedBuffer release() {
		return this;
	}

	@Override
	public InputStream createInputStream() throws IOException {
		FileInputStream fis = null;

		try {
			fis = new FileInputStream(file);
			ByteStreams.skipFully(fis, offset);
			return new LimitInputStream(fis, length);
		} catch (IOException e) {
			try {
				if (fis != null) {
					long size = file.length();
					throw new IOException("Error in reading" + this + "(actual file length " + size +")");
				}
			} catch (IOException e1) {
			} finally {
				Closeables.closeQuietly(fis);
			}
		} finally {
			Closeables.closeQuietly(fis);
		}
		return null;
	}

	@Override
	public Object nettyByteBuf() throws IOException {
		if (lazyFileDescription) {
			//return new LazyFileRegion(file, offset, length);
			return new DefaultFileRegion(file, offset, length);
		} else {
			return new DefaultFileRegion(file, offset, length);
		}
	}

	@Override
	public ByteBuffer nioByteBuffer() throws IOException {
		FileChannel fileChannel = null;
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			fileChannel = raf.getChannel();

			if (length <= MEMEPRY_MAP_BYTES) {
				//采用拷贝方式
				fileChannel.position(offset);
				ByteBuffer buffer = ByteBuffer.allocate((int) length);

				while (buffer.remaining() != -1) {
					if(fileChannel.read(buffer) == -1) {
						//读取到文件末尾
						throw new IOException(String.format("Reach EOF before filling buffer\n " +
								"offset=%s\nfiles=%s\nbu.remaining=%s", offset, file.getAbsolutePath(), buffer.remaining()));
					}
				}
				buffer.flip();
				return buffer;
			} else {
				//由于map操作消耗资源太多，因此只有对于大文件才进行直接映射
				return fileChannel.map(MapMode.READ_ONLY, offset, length);
			}
		} catch (IOException e) {
			if (fileChannel != null) {
				long size = fileChannel.size();
				throw new IOException("Error in reading" + this + "(actual file length " + size +")");
			}
		} finally {
			Closeables.closeQuietly(fileChannel);
		}
		return null;
	}

	public File getFile() {
		return file;
	}

	public long getOffset() {
		return offset;
	}

	public long getLength() {
		return length;
	}

	@Override
	public String toString() {
		return "FileSegmentManagedBuffer{" +
				"file=" + file +
				", offset=" + offset +
				", length=" + length +
				'}';
	}
}
