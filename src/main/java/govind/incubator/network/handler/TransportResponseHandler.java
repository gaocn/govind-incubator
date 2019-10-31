package govind.incubator.network.handler;

import govind.incubator.network.inteceptor.StreamInteceptor;
import govind.incubator.network.protocol.*;
import govind.incubator.network.protocol.codec.TransportFrameDecoder;
import govind.incubator.network.util.NettyUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-20
 *
 * 处理服务端的响应，内部维护每一类响应与对应回调函数的对应关系
 *
 * 并发：线程安全，可以在多个线程中调用。
 */
@Slf4j
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
	/**
	 * 响应消息处理器关联的通道
	 */
	private final Channel associatedChannel;

	/**
	 * 响应消息处理器关联的Rpc回调函数
	 */
	private final Map<Long, RpcCallback> outstandingRpcs;

	/**
	 * 响应消息处理器关联的ChunkFetch回调函数
	 */
	private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

	/**
	 * 响应消息处理关联的流请求回调函数
	 */
	private final Queue<StreamCallback> streamCallbacks;

	/**
	 * 上一次 Rpc请求或Chunk请求的时间，单位纳秒
	 */
	private final AtomicLong timeOfLastRequestInNanos;

	/**
	 * 通道关联的流通道是否处于打开状态
	 */
	private volatile boolean streamActive;

	public TransportResponseHandler(Channel associatedChannel) {
		this.associatedChannel = associatedChannel;
		this.outstandingRpcs = new ConcurrentHashMap<>();
		this.outstandingFetches = new ConcurrentHashMap<>();
		this.streamCallbacks = new ConcurrentLinkedDeque<>();
		this.timeOfLastRequestInNanos = new AtomicLong(0L);
	}

	/*******************注册回调函数的接口*******************/

	public long getTimeOfLastRequestInNanos() {
		return timeOfLastRequestInNanos.get();
	}

	public void updateTimeOfLastRequest() {
		timeOfLastRequestInNanos.set(System.nanoTime());
	}

	public void addRpcRequest(long requestId, RpcCallback callback) {
		updateTimeOfLastRequest();
		outstandingRpcs.put(requestId, callback);
	}

	public void removeRpcRequest(long requestId) {
		outstandingRpcs.remove(requestId);
	}

	public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
		updateTimeOfLastRequest();
		outstandingFetches.put(streamChunkId, callback);
	}

	public void removeFetchRequest(StreamChunkId streamChunkId) {
		outstandingFetches.remove(streamChunkId);
	}

	public void addStreamRequest(StreamCallback callback) {
		updateTimeOfLastRequest();
		streamCallbacks.offer(callback);
	}

	public void deactiveStream() {
		streamActive =  false;
	}

	public int numOfOutstandingRequests() {
		return outstandingRpcs.size() + outstandingFetches.size() +
				streamCallbacks.size() + (streamActive ? 1 : 0);
	}

	/*******************覆写方法，处理响应*******************/

	@Override
	public void handler(ResponseMessage message) throws Exception {
		if (message instanceof RpcResponse)  {
			processRpcResponse((RpcResponse)message);
		} else if (message instanceof RpcFailure) {
			processRpcFailure((RpcFailure)message);
		} else if (message instanceof StreamResponse) {
			processStreamResponse((StreamResponse)message);
		} else if (message instanceof StreamFailure) {
			processStreamFailure((StreamFailure)message);
		} else if (message instanceof ChunkFetchSuccess) {
			processChunkResponse((ChunkFetchSuccess)message);
		} else if (message instanceof ChunkFetchFailure) {
			processChunkFailure((ChunkFetchFailure)message);
		}else {
			log.error("不支持的消息类型：{}", message.type());
		}
	}

	@Override
	public void exceptionCaught(Throwable cause) throws Exception {
		if (numOfOutstandingRequests() > 0) {
			final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
			log.error("来自{}的连接异常，尚有{}个请求未被处理", remoteAddr, numOfOutstandingRequests());
			failOutstandingRequest(cause);
		}
	}

	@Override
	public void channelUnregistered() {
		if (numOfOutstandingRequests() > 0) {
			final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
			log.error("来自{}的连接异常，尚有{}个请求未被处理", remoteAddr, numOfOutstandingRequests());

			failOutstandingRequest(new IOException("来自" + remoteAddr + "的连接被关闭"));
		}
	}

	/***************** private method *********************/
	/**
	 * 当捕获到异常或连接被终止时，调用所有请求的failure回调函数，通
	 * 知处理过程中出现问题，同时清理队列中的请求。
	 * @param cause
	 */
	private  void failOutstandingRequest(Throwable cause) {
		outstandingRpcs.forEach((requestId, callback) ->{
			callback.onFailure(cause);
		} );

		outstandingFetches.forEach((streamChunkId, callback) -> {
			callback.onFailure(streamChunkId.chunkIdx, cause);
		});

		//请求已处理的请求
		outstandingRpcs.clear();
		outstandingFetches.clear();
	}

	private void processRpcResponse(RpcResponse resp) {
		final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
		RpcCallback callback = outstandingRpcs.get(resp.requestId);

		if (callback != null) {
			outstandingRpcs.remove(resp.requestId);
			try {
				callback.onSuccess(resp.body().nioByteBuffer());
			} catch (IOException e) {
				callback.onFailure(e.getCause());
			} finally {
				resp.body().release();
			}
		} else {
			log.warn("忽略来自{}({} bytes)的Rpc响应{}，因为没有注册对应的处理器", remoteAddr, resp.body().size(), resp.requestId);
			resp.body().release();
		}
	}

	private void processRpcFailure(RpcFailure resp) {
		final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
		RpcCallback callback = outstandingRpcs.get(resp.requestId);

		if (callback != null) {
			outstandingRpcs.remove(resp.requestId);
			callback.onFailure(new RuntimeException(resp.error));
		} else {
			log.warn("忽略来自{}({} bytes)的Rpc响应{}，因为没有注册对应的处理器", remoteAddr, resp.body().size(), resp.requestId);
		}
	}

	private void processStreamResponse(StreamResponse resp) {
		final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
		StreamCallback callback = streamCallbacks.poll();

		if (callback != null) {
			if (resp.byteCount > 0) {
				try {
					StreamInteceptor inteceptor = new StreamInteceptor(this, resp.streamId, resp.byteCount, callback);
					TransportFrameDecoder frameDecoder = (TransportFrameDecoder) associatedChannel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
					frameDecoder.setInteceptor(inteceptor);
					log.debug("安装StreamInterceptor: {}", inteceptor);
					streamActive = true;
				} catch (Exception e) {
					log.error("安装StreamInterceptor出错");
					deactiveStream();
				}
			} else {
				try {
					callback.onComplete(resp.streamId);
				} catch (IOException e) {
					log.warn("流处理出错，调用onComplete方法时出错：{}", e.getMessage());
				}
			}
		} else {
			log.error("来自{}的流处理出错，没有找到对应的回调函数", remoteAddr);
		}
	}

	private void processStreamFailure(StreamFailure resp) {
		StreamCallback callback = streamCallbacks.poll();

		if (callback != null) {
			try {
				callback.onFailure(resp.streamId, new RuntimeException(resp.error));
			} catch (IOException e) {
				log.warn("调用Stream的回调函数onFailure时出错：{}", e.getMessage());
			}
		} else {
			log.error("流处理出错，没有找到对应的回调函数");
		}
	}

	private void processChunkResponse(ChunkFetchSuccess resp) {
		final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
		ChunkReceivedCallback callback = outstandingFetches.get(resp.streamChunkId);

		if (callback != null) {
			outstandingFetches.remove(resp.streamChunkId);
			try {
				callback.onSuccess(resp.streamChunkId.chunkIdx, resp.body());
			} finally {
				resp.body().release();
			}
		} else {
			log.warn("忽略来自{}({} bytes)的ChunkFetch响应{}，因为没有注册对应的处理器", remoteAddr, resp.body().size(), resp.streamChunkId);
			resp.body().release();
		}

	}

	private void processChunkFailure(ChunkFetchFailure resp) {
		final String remoteAddr = NettyUtil.getRemoteAddress(associatedChannel);
		ChunkReceivedCallback callback = outstandingFetches.get(resp.streamChunkId);
		if (callback != null) {
			outstandingFetches.remove(resp.streamChunkId);
			callback.onFailure(resp.streamChunkId.chunkIdx, new RuntimeException(String.format("获取Chunk(%s)出错：%s", resp.streamChunkId, resp.error)));
		} else {
			log.warn("忽略来自{}({} bytes)的ChunkFetch响应{}，因为没有注册对应的处理器", remoteAddr, resp.body().size(), resp.streamChunkId);
			resp.body().release();
		}
	}
}
