package govind.incubator.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 不可变字节数据的抽象，对外提供具体视图用于操作数据，具体视图如下：
 * <ol>
 *     <li>
 * 			{@link FileSegmentManagedBuffer} 字节数据作为文件的一部分；
 *     </li>
 *     <li>
 *          {@link NettyManagedBuffer} 字节数据以Netty ByteBuf形式提供；
 *     </li>
 *     <li>
 *          {@link NioManagedBuffer} 字节数据以NIO ByteBuffer形式提供；
 *     </li>
 * </ol>
 */
abstract public class ManagedBuffer {

	/**
	 * 字节数
	 * @return
	 */
	abstract public long size();

	/**
	 * 若该缓冲区支持引用计数，则引用计数加1
	 * @return
	 */
	abstract public  ManagedBuffer retain();

	/**
	 * 若该缓冲区支持引用计数，则引用计数减1
	 * @return
	 */
	abstract public ManagedBuffer release();

	/**
	 * 将当前缓冲区转换为InputStream，底层实现不一定会读取字节长度，需要调用者
	 * 负责读取数据时不会超限！
	 * @return
	 * @throws IOException
	 */
	abstract public InputStream createInputStream()throws IOException;

	/**
	 * 将当前缓冲区数据转换为Netty对象
	 * @return
	 * @throws IOException
	 */
	abstract public Object nettyByteBuf() throws IOException;

	/**
	 * 将当前缓冲区数据转换为NIO ByteBuffer，对返回数据的position、limit的
	 * 修改不会影响当前缓冲区。
	 *
	 * TODO 该过程可能需要内存拷贝 、内存映射，消耗性能！
	 *
	 * @return
	 * @throws IOException
	 */
	abstract public ByteBuffer nioByteBuffer() throws IOException;
}
