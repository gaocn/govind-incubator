package govind.incubator.network.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * 【Zero-Copy】
 * https://www.linuxjournal.com/article/6345?page=0,0
 * 在了解什么是零拷贝之前，首先看一下在没有零拷贝前，服务端将本地存储的将一个文件通过
 * 网络发送给客户端需要经历的过程，实现数据传输的伪代码如下
 * 	<code>
 *  	read(file, tmp_buf, len);
 *  	write(socket, tmp_buf, len);
 *
 *  	File.read(fileDesc, buf, len);
 * 		Socket.send(socket, buf, len);
 * 	</code>
 * 只需要两次系统调用，实际在OS内部至少需要4次拷贝，同时需要至少4次用户态/内核态上下
 * 文切换，其大致过程如下：
 * ----------------------------------------------------------------------
 * 1、read系统调用导致用户态->内核态切换，然后通过DMA将磁盘上的数据拷贝到内核中的某
 * 块内存缓存区中；
 * 2、将内核中拷贝的数据拷贝到用户态的某块内存缓存区中，然后read系统调用返回。
 * 3、write系统调用用户态->内核态切换，第三次拷贝从用户态缓存区中将数据拷贝到内核态
 * 与socket关联的内核缓存区中；
 * 4、write系统调用结束，同时完成第四次上下文切换。第四次拷贝在write系统调用返回后
 * 异步执行，是DMA将内核缓存区中准备发送的数据拷贝到协议栈中。为什么是异步？因为系统
 * 调用返回后并不保证数据被立即发送，实际上会被缓存到TCP/IP协议栈的发送缓存区中等待
 * 被发送，这个过程是存在延迟的！
 * ----------------------------------------------------------------------
 *
 * 可以看出上述过程中，有很多次拷贝是没有必要的，避免不必要的拷贝常用的方法有：
 * 1、nmap
 * 	<code>
 *  	tmp_buf = nmap(file, len);
 *  	write(socket, tmp_buf, len);
 * 	</code>
 *	(1) nmap系统调用导致文件内存被DMA拷贝到内核缓存区中，且该缓存区的内容与用户进程
 *	共享，不需要在用户和内核空间拷贝。
 *	(2)write系统调用将上述内核缓冲区中的数据拷贝到与socket关联的内核缓冲区中。
 *	(3)write系统调用结束，同时完成第三次拷贝，DMA将内核缓存区中准备发送的数据拷贝到
 *	协议栈中。
 *
 * 使用nmap可以减少用户态到内核态的内存拷贝，但是采用nmap+write的问题是：当向nmap文
 * 件写入数据的同时另一个进程对文件执行了truncate操作，会导致memory access错误，会
 * 导致写入数据进程被kill掉。
 *
 * 2、sendfile(socket, file, len) or transferTo(position, count, writableChannel);
 * sendfile不仅能够减少拷贝次数同时也减少用户态/内核态切换次数，只需要一次系统调用。
 *	(1)sendfile系统调用导致文件内存被DMA直接拷贝到与socket关联内核缓存区中。
 *	(2)sendfile系统调用结束，同时异步进行第二次拷贝，DMA将内核缓存区中准备发送的数
 * 据拷贝到协议栈中。
 *
 * 3、在linuxn kernel version 2.4以后，上述的一次拷贝也可以避免，通过修改socket
 * buffer让其可以关联描述符(buffer descriptor)来实现数据传输。
 * 	(1)sendfile系统调用导致文件内存被DMA直接拷贝到内核缓存区中。
 * 	(2)不需要将数据拷贝到socket缓存区，而是将一个描述符(whereabouts, len)添加到
 * 	socket buffer中，而是有DMA直接将内核缓存区数据直接拷贝到协议栈缓存区中。
 *
 * 从OS角度看，因为数据不需要在内核缓存区、用户缓冲区中拷贝，因此认为是Zero-Copy!
 * 需要注意的是， sendfile and mmap发送的文件最大不能超过2G！
 *
 * 【Zero-Copy性能测试】
 *	https://developer.ibm.com/articles/j-zerocopy/
 *  File size	Normal file transfer (ms)	transferTo (ms)
 *   7MB			156							45
 *   21MB			337							128
 *   63MB			843							387
 *   98MB			1320						617
 *   200MB			2124						1150
 *   350MB			3631						1762
 *   700MB			13498						4422
 *   1GB			18399						8537
 *
 * The transferTo() API brings down the time approximately 65 percent
 * compared to the traditional approach.
 */
public class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

	/**
	 * 每次传输数据的上限，当传输的数据大于256K时，按照256K分批发送。
	 *
	 * PS：该值不能太大否则底层会进行频繁内存拷贝，因为TCP SEND BUFFER大小固定，若发送数据大于
	 * 发送缓冲区大小，则会进行多次拷贝！
	 */
	private final static int NIO_BUFFER_LIMIT =  256 * 1024;

	private final ByteBuf header;
	private final Object body;
	private final int headerLength;
	private final long bodyLength;

	/**
	 * 记录每次已传输的字节数
	 */
	private long totalBytesTransferred;

	public MessageWithHeader(ByteBuf header, Object body, int headerLength, long bodyLength) {

		assert body instanceof ByteBuf || body instanceof FileRegion: "body类型只能为ByteBuf或FileRegion之一！";

		this.header = header;
		this.body = body;
		this.headerLength = headerLength;
		this.bodyLength = bodyLength;
	}

	@Override
	public long position() {
		return 0;
	}

	@Override
	public long transfered() {
		return totalBytesTransferred;
	}

	@Override
	public long count() {
		return headerLength +  bodyLength;
	}

	/**
	 * 将数据发送到Channel的另一端，有两种方式：
	 * 1、若是ByteBuf则采用拷贝方式发送到缓存区，拷贝直接数组时有两种情况:
	 * 	(1)、若字节数组长度小于NIO_BUFFER_LIMIT，则只需要一次发送；
	 * 	(2)、若字节数组长度大于NIO_BUFFER_LIMIT，则按照NIO_BUFFER_LIMIT拆分，分批发送。
	 * 2、若为FileRegion对象，则采用Zero-Copy方式传输；
	 * @param target
	 * @param position
	 * @return
	 * @throws IOException
	 */
	@Override
	public long transferTo(WritableByteChannel target, long position) throws IOException {

		assert position == totalBytesTransferred : "非法传输位置！";

		//1. 发送header
		long writtenBytesOfHeader = 0L;
		if (header.readableBytes() > 0) {
			writtenBytesOfHeader = copyByteBuf(header, target);
			totalBytesTransferred += writtenBytesOfHeader;
			if (header.readableBytes() > 0) {
				return writtenBytesOfHeader;
			}
		}

		//2. 发送body，若body为ByteBuf对象则采用拷贝方式，若body为FileRegion对象则采用Zero-Copy发送
		long writtenBytesOfBody = 0L;
		if (body instanceof FileRegion) {
			writtenBytesOfBody = ((FileRegion)body).transferTo(target,totalBytesTransferred - headerLength);
		} else if(body instanceof ByteBuf) {
			writtenBytesOfBody = copyByteBuf((ByteBuf)body, target);
		}
		totalBytesTransferred += writtenBytesOfBody;

		return writtenBytesOfHeader + writtenBytesOfBody;
	}

	/**
	 * 将ByteBuf中的数据拷贝到指定channel中，底层基于ByteBuffer实现
	 * @param buf
	 * @param target
	 * @return 写入channel中的字节数
	 */
	private long copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
		ByteBuffer nioBuffer = buf.nioBuffer();
		int writtenBytes =
				nioBuffer.remaining() < NIO_BUFFER_LIMIT ?
				target.write(nioBuffer) :
				writeNioBuffer(target, nioBuffer);
		buf.skipBytes(writtenBytes);
		return 0;
	}

	/**
	 * ByteBuffer中的数据超过{@link #NIO_BUFFER_LIMIT}分批将ByteBuffer中
	 * 的字节写到指定channel中
	 * @param target
	 * @param buffer
	 * @return 写入到channel的直接数
	 */
	private int writeNioBuffer(WritableByteChannel target, ByteBuffer buffer) throws IOException {
		int originalLimit = buffer.limit();
		int writtenBytes = 0;

		try {
			int bytesToWrite = Math.min(NIO_BUFFER_LIMIT, buffer.remaining());
			buffer.limit(buffer.position() + bytesToWrite);
			writtenBytes = target.write(buffer);
		} finally {
			buffer.limit(originalLimit);
		}
		return writtenBytes;
	}


	@Override
	protected void deallocate() {
		header.release();
		ReferenceCountUtil.release(body);
	}
}
