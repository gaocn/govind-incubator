package govind.incubator.network.client;

import com.google.common.util.concurrent.SettableFuture;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.StreamCallback;
import govind.incubator.network.handler.TransportResponseHandler;
import govind.incubator.network.protocol.*;
import govind.incubator.network.util.NettyUtil;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import jersey.repackaged.com.google.common.base.MoreObjects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 1、根据协商的streamId从服务端获取连续Stream Chunk，该API用于实
 *	现大容量数据的高效传输，将大数据拆分为几KB到几MB不等的块进行传输。
 *
 * 2、虽然该TransportClient是用于实现从服务端获取Stream数据，有关
 * 流的配置需要在该层之上进行，通常采用`sendRpc`方法用于在进行流数据
 * 传输前实现客户端与服务端的协商，以完成流相关配置。
 *
 * 例如：
 * {{
 * 	client.sendRpc(new OpenFile("/foo")) --&gt; return streamId=100
 * 	client.fetchChunk(streamid=100,chunkIdx=0, callback)
 * 	client.fetchChunk(streamid=100,chunkIdx=0, callback)
 *  ...
 *  client.sendRpc(new CloseStream(100));
 * }}
 *
 * 3、使用TransportClientFactory创建TransportClient实例，同一个
 * TransportClient实例可以被用于多个流的传输，当某个流只能被一个
 * TransportClient实例消费以避免响应内容的无序！
 *
 * 4、TransportClient负责将请求发送给服务端，ResponseHandler负责
 * 处理来自服务端的响应。
 *
 * 并发问题：TransportClient线程安全，支持多线程分发调用。
 *
 */
@Slf4j
public class TransportClient implements Closeable {
	private String clientId;
	private final Channel channel;
	private final TransportResponseHandler responseHandler;

	/**
	 * 标记当前客户端是否超时（在一段时间内没有收发数据）
	 */
	private volatile boolean timedOut;

	public TransportClient(SocketChannel ch, TransportResponseHandler responseHandler) {
		this.channel = ch;
		this.responseHandler = responseHandler;
	}

	/******************客户端发送请求的方法*******************/

	/**
	 * 向服务端发送一条opaque Rpc消息，不期待回复，也不保证消息被送达！
	 * @param message
	 */
	public void send(ByteBuffer message) {
		channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
	}

	/**
	 * 异步方式先服务端发送请求，响应结果通过回调函数处理
	 * @param message
	 * @param callback
	 * @return
	 */
	public long sendRpcAsync(ByteBuffer message, final RpcCallback callback) {
		final String serverAddr = NettyUtil.getRemoteAddress(channel);
		final long startTime = System.currentTimeMillis();

		log.debug("异步发送RPC请求到{}", serverAddr);

		final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
		responseHandler.addRpcRequest(requestId, callback);

		channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
				.addListener(future -> {
					if (future.isSuccess()) {
						long timeTaked = System.currentTimeMillis() - startTime;
						log.debug("异步发送RPC请求{}到{}，耗时：{}ms", requestId, serverAddr, timeTaked);
					} else {
						String error = String.format("异步发送RPC请求%d到%s，失败：%s",requestId, serverAddr, future.cause().getMessage());
						log.info(error);
						responseHandler.removeRpcRequest(requestId);
						channel.close();

						try {
							callback.onFailure(new IOException(error, future.cause()));
						} catch (Exception e) {
							log.error("调用RpcCallback处理器时抛出异常：{}", e.getMessage());
						}
					}
				});
		return requestId;
	}

	/**
	 * @param message
	 * @param timeoutMs
	 * @return
	 */
	public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
		final SettableFuture<ByteBuffer> result = SettableFuture.create();

		sendRpcAsync(message, new RpcCallback() {
			@Override
			public void onSuccess(ByteBuffer response) {
				ByteBuffer copy = ByteBuffer.allocate(response.remaining());
				copy.put(response);
				//flip to make it readable
				copy.flip();
				result.set(copy);
			}

			@Override
			public void onFailure(Throwable cause) {
				result.setException(cause);
			}
		});

		try {
			return result.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 根据pre-negotiated streamId从服务端请求一块数据。 chunk索
	 * 引从0开始递增，请求同一个chunk数据多次是可以的，虽然有些流可
	 * 能不支持这一特性。
	 *
	 * 对于并发的多个chunk请求，chunk数据按照请求顺序返回，同时假设
	 * 多个chunk请求为同一个客户端发出！
	 *
	 * @param streamId 用于标识服务端中StreamManager中的一个流，
	 *                     流标识需要客户端与服务端协商确定。
	 * @param chunkIdx 流中某一个块标识，开始为0
	 * @param callback 在成功获取流或获取失败时的回调函数
	 */
	public void fetchChunk(long streamId, int chunkIdx, ChunkReceivedCallback callback) {
		final String serverAddr =  NettyUtil.getRemoteAddress(channel);
		final long startTime = System.currentTimeMillis();
		log.debug("异步发送ChunkFetch请求到{}", serverAddr);

		final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIdx);
		responseHandler.addFetchRequest(streamChunkId,  callback);

		channel.writeAndFlush(new ChunkFetchRequest(streamChunkId))
				.addListener(future -> {
					if (future.isSuccess()) {
						long timeTaked = System.currentTimeMillis();
						log.info("异步发送ChunkFetch请求到{}，耗时：{}ms", serverAddr, timeTaked);
					} else {
						String error = String.format("异步发送ChunkFetch请求到%s失败：%s", serverAddr, future.cause().getMessage());
						log.debug(error);

						responseHandler.removeFetchRequest(streamChunkId);
						channel.close();

						try {
							callback.onFailure(chunkIdx, future.cause());
						} catch (Exception e) {
							log.error("调用ChunkReceivedCallback处理器时抛出异常：{}", e.getMessage());
						}
					}
				});
	}

	/**
	 * 向服务端请求指定streamId的流数据
	 * @param streamId 要获取的流
	 * @param callback 返回的流数据的回调函数
	 */
	public void stream(final String streamId, final StreamCallback callback) {
		final String remoteAddr = NettyUtil.getRemoteAddress(channel);
		final long startTime  = System.currentTimeMillis();
		log.debug("发送Stream请求{}给{}",streamId, remoteAddr);

		/**
		 * 加锁的原因：确保添加回调函数同时发送StreamRequest请求成
		 * 功后，再处理下一个StreamRequest，以保证回调函数顺序添加
		 * 到队列中，这样在响应时就能够按照请求顺序处理；
		 */
		synchronized (this) {
			responseHandler.addStreamRequest(callback);
			channel.writeAndFlush(new StreamRequest(streamId))
					.addListener(future -> {
						if (future.isSuccess()) {
							long timeTaked = System.currentTimeMillis() - startTime;
							log.info("发送Stream请求{}给{}，耗时：{}ms",streamId, remoteAddr, timeTaked);
						} else {
							String error = String.format("发送Stream请求%s给%s，异常：%dms", streamId, remoteAddr, future.cause().getMessage());
							log.error(error);
							channel.close();

							try {
								callback.onFailure(streamId, future.cause());
							} catch (IOException e) {
								log.error("调用StreamCallback#onFailure时抛出异常：{}", e.getMessage());
							}
						}
					});
		}
	}

	/******************获取客户端状态的方法*******************/

	public boolean isActive() {
		return !timedOut && (channel.isActive() || channel.isOpen());
	}

	/**
	 * 当启用认证功能时，根据clientId实现
	 * @return client id，当认证功能未启用时返回null
	 */
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		if (StringUtils.isNoneEmpty(clientId)) {
			throw new IllegalArgumentException("clientId已存在，不能修改");
		}
		this.clientId = clientId;
	}

	/**
	 * 通道空闲超时，被主动断开连接；
	 */
	public void timeout() {
		this.timedOut = true;
	}

	public TransportResponseHandler getResponseHandler() {
		return responseHandler;
	}

	public Channel getChannel() {
		return channel;
	}

	public SocketAddress getSocketAddress() {
		return channel.remoteAddress();
	}

	/**
	 * 通道在10s内关闭
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("", channel.remoteAddress().toString())
				.add("clientId", clientId)
				.add("isActive", isActive())
				.toString();
	}
}
