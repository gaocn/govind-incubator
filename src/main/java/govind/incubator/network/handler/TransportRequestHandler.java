package govind.incubator.network.handler;

import com.google.common.base.Throwables;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.protocol.*;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;


/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-20
 * <p>
 * 请求消息处理器
 */
@Slf4j
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
	/**
	 * 关联的Channel
	 */
	private final Channel associatedChannel;

	/**
	 * 关联client实例
	 */
	private final TransportClient requestClient;

	/**
	 * Rpc消息处理器
	 */
	private final RpcHandler rpcHandler;

	/**
	 * 数据流管理器
	 */
	private final StreamManager streamManager;

	/**
	 * 构造器
	 *
	 * @param associatedChannel 关联通道
	 * @param requestClient     关联client
	 * @param rpcHandler        关联Rpc处理器
	 */
	public TransportRequestHandler(Channel associatedChannel, TransportClient requestClient, RpcHandler rpcHandler) {
		this.associatedChannel = associatedChannel;
		this.requestClient = requestClient;
		this.rpcHandler = rpcHandler;
		this.streamManager = rpcHandler.getStreamManager();
	}

	@Override
	public void handler(RequestMessage message) throws Exception {
		if (message instanceof OneWayMessage) {
			processOneWayMessge((OneWayMessage) message);
		} else if (message instanceof RpcRequest) {
			processRpcRequest((RpcRequest) message);
		} else if (message instanceof StreamRequest) {
			processStreamRequest((StreamRequest) message);
		} else if (message instanceof ChunkFetchRequest) {
			processChunkFetchRequest((ChunkFetchRequest) message);
		} else {
			throw new IllegalArgumentException("不支持的消息类型：{}" + message.type());
		}
	}

	@Override
	public void exceptionCaught(Throwable cause) throws Exception {
		rpcHandler.exceptionCaught(cause, requestClient);
	}

	@Override
	public void channelUnregistered() {
		if (streamManager != null) {
			try {
				streamManager.connectionTerminated(associatedChannel);
			} catch (Exception e) {
				log.info("关闭StreamManger失败，原因：{}", Throwables.getStackTraceAsString(e));
			}
		}
		rpcHandler.connectionTerminated(requestClient);
	}

	/***************** private method *********************/

	private void processOneWayMessge(OneWayMessage req) {
		try {
			rpcHandler.receive(requestClient, req.body().nioByteBuffer());
		} catch (Exception e) {
			log.error("RpcHandler处理One-Way-Message时出错：{}", Throwables.getStackTraceAsString(e));
		} finally {
			req.body().release();
		}
	}

	private void processRpcRequest(RpcRequest req) {
		try {
			rpcHandler.receive(requestClient, req.body().nioByteBuffer(), new RpcCallback() {
				@Override
				public void onSuccess(ByteBuffer response) {
					respond(new RpcResponse(new NioManagedBuffer(response), req.requestId));
				}

				@Override
				public void onFailure(Throwable cause) {
					respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(cause)));
				}
			});
		} catch (Exception e) {
			log.error("Rpc请求id为{}, RpcHandler处理RpcRequest时出错:{}", req.requestId, Throwables.getStackTraceAsString(e));
			respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
		} finally {
			req.body().release();
		}
	}

	private void processStreamRequest(StreamRequest req) {
		final String clientAddr = associatedChannel.remoteAddress().toString();
		ManagedBuffer buffer = null;

		try {
			buffer = streamManager.openStream(req.streamId);
		} catch (Exception e) {
			log.error("为客户端{}打开流streamId={}出错：{}", clientAddr, req.streamId, e.getMessage());
			respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
			return;
		} finally {
		}
		respond(new StreamResponse(buffer, req.streamId, buffer.size()));
	}

	private void processChunkFetchRequest(ChunkFetchRequest req) {
		final String clientAddr = associatedChannel.remoteAddress().toString();
		log.debug("接收来自{}的块数据请求：{}", clientAddr, req.streamChunkId);

		ManagedBuffer buffer = null;

		try {
			streamManager.checkAuthorization(requestClient, req.streamChunkId.streamId);
			streamManager.registerChannle(associatedChannel, req.streamChunkId.streamId);
			buffer = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIdx);
		} catch (Exception e) {
			log.error("为来自{}的请求打开块{}失败：{}", clientAddr, req.streamChunkId, e.getMessage());
			respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
			return;
		}
		respond(new ChunkFetchSuccess(buffer, req.streamChunkId));
	}

	/**
	 * 服务端处理过程出现错误，将错误信息返回给客户端，若在发送过程出
	 * 现错误，则将记录日志同时关闭channel
	 *
	 * @param resp
	 */
	private void respond(final Encodable resp) {
		final String clientAddr = associatedChannel.remoteAddress().toString();
		associatedChannel.writeAndFlush(resp)
				.addListener(future -> {
					if (future.isSuccess()) {
						log.info("成功给客户端{}发送消息{}", clientAddr, resp);
					} else {
						log.error("给客户端{}发送消息失败: {}", clientAddr, Throwables.getStackTraceAsString(future.cause()));
						associatedChannel.close();
					}
				});
	}
}
