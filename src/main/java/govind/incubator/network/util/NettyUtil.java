package govind.incubator.network.util;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import govind.incubator.network.protocol.codec.TransportFrameDecoder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadFactory;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 *
 * 用于创建
 *
 */
public class NettyUtil {

	public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
		return new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat(threadPoolPrefix + "-%d")
				.build();
	}

	public static Class<? extends Channel> getClientChannelClass(IOMode ioMode) {
		switch (ioMode) {
			case NIO:
				return NioSocketChannel.class;
			case EPOLL:
				return EpollSocketChannel.class;
			default:
				throw new IllegalArgumentException("未知枚举类型");
		}
	}

	public static Class<? extends ServerChannel> getServerChannelClasss(IOMode ioMode) {
		switch (ioMode) {
			case NIO:
				return NioServerSocketChannel.class;
			case EPOLL:
				return EpollServerSocketChannel.class;
			default:
				throw new IllegalArgumentException("未知枚举类型");
		}
	}

	public static EventLoopGroup createEventLoopGroup(IOMode ioMode, int numThreads,String threadPrefix) {
		ThreadFactory factory = createThreadFactory(threadPrefix);
		switch (ioMode) {
			case NIO:
				return new NioEventLoopGroup(numThreads, factory);
			case EPOLL:
				return new EpollEventLoopGroup(numThreads, factory);
			default:
				throw new IllegalArgumentException("未知枚举类型");
		}
	}

	public static TransportFrameDecoder createFrameDecoder() {
		return new TransportFrameDecoder();
	}

	public static String getRemoteAddress(Channel channel) {
		if (channel != null && channel.remoteAddress() != null) {
			return channel.remoteAddress().toString();
		}
		return "<unknown>";
	}


	public static PooledByteBufAllocator createPooledByteBufAllocator(boolean allowDirectBufs, boolean allowCache, int numCores) {
		numCores = numCores == 0 ? Runtime.getRuntime().availableProcessors() : numCores;

		return new PooledByteBufAllocator(allowDirectBufs,
				Math.min(numCores, getPrivateStaticFiled("DEFAULT_NUM_HEAP_ARENA")),
				Math.min(allowDirectBufs?numCores:0, getPrivateStaticFiled("DEFAULT_NUM_DIRECT_ARENA")),
				getPrivateStaticFiled("DEFAULT_PAGE_SIZE"),
				getPrivateStaticFiled("DEFAULT_MAX_ORDER"),
				allowCache ? getPrivateStaticFiled("DEFAULT_TINY_CACHE_SIZE") : 0,
				allowCache ? getPrivateStaticFiled("DEFAULT_SMALL_CACHE_SIZE") : 0,
				allowCache ? getPrivateStaticFiled("DEFAULT_NORMAL_CACHE_SIZE") : 0);

	}

	private static int getPrivateStaticFiled(String name) {
		try {
			Field field = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
			field.setAccessible(true);
			return field.getInt(null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Convert the given byte buffer to a string. The resulting string can be
	 * converted back to the same byte buffer through stringToBytes(String).
	 */
	public static String bytesToString(ByteBuffer b) {
		return Unpooled.wrappedBuffer(b).toString(Charsets.UTF_8);
	}

	/**
	 * Convert the given string to a byte buffer. The resulting buffer can be
	 * converted back to the same string through {@link #bytesToString(ByteBuffer)}.
	 */
	public static ByteBuffer stringToBytes(String s) {
		return Unpooled.wrappedBuffer(s.getBytes(Charsets.UTF_8)).nioBuffer();
	}

	public static String getLocalHost() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static byte[] bufToArray(ByteBuffer buffer) {
		byte[] data;
		if (buffer.hasArray() &&
			buffer.arrayOffset() == 0 &&
			buffer.array().length == buffer.remaining()) {
			return buffer.array();
		} else {
			data = new byte[buffer.remaining()];
			buffer.get(data);
			return data;
		}
	}
}
