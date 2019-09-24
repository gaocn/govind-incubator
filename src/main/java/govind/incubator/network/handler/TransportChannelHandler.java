package govind.incubator.network.handler;

import govind.incubator.network.TransportClient;
import govind.incubator.network.protocol.Message;
import govind.incubator.network.protocol.RequestMessage;
import govind.incubator.network.protocol.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-24
 *
 * 委托{@link TransportRequestHandler}和{@link TransportResponseHandler}
 * 根据消息类型{@link Message}调用响应处理器进行处理。
 *
 * 所有传输层的通道都是双向的，当客户端初始化Netty通道时发送请求消息，
 * 服务端会产生对应该的响应。同样在服务端也有一个处理器，使得服务端也
 * 可以发送请求消息给客户端。也就是客户端和服务端都需要ResponseHandler
 * 和RequestHandler！
 *
 * 传输层通过{@link io.netty.handler.timeout.IdleStateHandler}
 * 处理超时事件，当通道上有超过`requestTimeoutNs`时间是空闲的则认为
 * outstandingRpc请求和outstandingFetch请求超时。
 *
 * 由于是全双工通信，因此当客户端持续不断发送数据但没有响应时，不会产生
 * 超时事件。
 *
 */
@Slf4j
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
	private TransportClient client;
	private TransportRequestHandler  requestHandler;
	private TransportResponseHandler responseHandler;

	private long requestTimeoutNS;
	private boolean closeIdleConnections;

	public TransportChannelHandler(TransportClient client, TransportRequestHandler requestHandler, TransportResponseHandler responseHandler, long requestTimeoutSec, boolean closeIdleConnections) {
		this.client = client;
		this.requestHandler = requestHandler;
		this.responseHandler = responseHandler;
		this.requestTimeoutNS = requestTimeoutNS * 1000L * 1000;
		this.closeIdleConnections = closeIdleConnections;
	}


	public TransportClient getClient() {
		return client;
	}

	public TransportRequestHandler getRequestHandler() {
		return requestHandler;
	}

	public TransportResponseHandler getResponseHandler() {
		return responseHandler;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
		if (msg instanceof RequestMessage) {
			requestHandler.handler((RequestMessage) msg);
		} else {
			responseHandler.handler((ResponseMessage) msg);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.warn("与{}的连接发生异常：{}", ctx.channel().remoteAddress(), cause.getMessage());

		requestHandler.exceptionCaught(cause);
		responseHandler.exceptionCaught(cause);
		ctx.close();
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		try {
			requestHandler.channelUnregistered();
		} catch (Exception e) {
			log.error("在注销通道时，请求处理器发生异常：{}", e.getMessage());
		}

		try {
			responseHandler.channelUnregistered();
		} catch (Exception e) {
			log.error("在注销通道时，响应处理器发生异常：{}", e.getMessage());
		}
	}

	/**
	 * 通道空闲（读、写）时被触发
	 * @param ctx
	 * @param evt
	 * @throws Exception
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent stateEvent = (IdleStateEvent) evt;
			/** TODO
			 *See class comment for timeout semantics. In addition to ensuring we only
			 * timeout while there are outstanding requests, we also do a secondary
			 * consistency check to ensure there's no race between the idle timeout
			 * and incrementing the numOutstandingRequests
			 *
			 *To avoid a race between TransportClientFactory.createClient() and this code
			 *  which could result in an inactive client being returned, this needs to run
			 *  in a synchronized block.
			 */
			synchronized (this) {
				boolean isActualOverdue = System.nanoTime() - responseHandler.getTimeOfLastRequestInNanos()  > requestTimeoutNS;

				//读写空闲，且响应超时
				if (stateEvent.state() == IdleState.ALL_IDLE && isActualOverdue) {
					if (responseHandler.numOfOutstandingRequests() > 0) {
						log.error("来自{}的连接已经空闲了{} ms，并且还有未处理完的请求，因此认为客户端已经断开，下面主动断开连接！",ctx.channel().remoteAddress(), requestTimeoutNS/ 1000 / 1000);

						client.timeout();
						ctx.close();
					} else if(closeIdleConnections) {
						client.timeout();
						ctx.close();
					}
				}
			}
		}
		ctx.fireUserEventTriggered(evt);
	}
}
