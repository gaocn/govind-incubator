package govind.incubator.network.util;

import com.google.common.collect.Lists;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientBootstrap;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.TransportChannelHandler;
import govind.incubator.network.handler.TransportRequestHandler;
import govind.incubator.network.handler.TransportResponseHandler;
import govind.incubator.network.protocol.codec.MessageDecoder;
import govind.incubator.network.protocol.codec.MessageEncoder;
import govind.incubator.network.protocol.codec.TransportFrameDecoder;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.server.TransportServerBootstrap;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 * <p>
 * 主要作用：
 * 1、创建TransportServer、TransportClientFactory；
 * 2、负责使用TransportChannelHandler装配pipeline；
 * <p>
 * <p>
 * TransportClient提供两种通讯协议：
 * 1、control-plane用于实现Rpc通信；
 * 2、data-plane用于实现chunk fetching；
 * <p>
 * 针对Rpc协议的处理是在TransportContext之外由用户控制，通常用于配置
 * stream，以便在data-plane进行数据传输时使用零拷贝加速网络传输速率。
 * <p>
 * TransportServer、TransportClientFactory会为每个Channel创建一个
 * TransportChannelHandler实例，每个实例中都包含一个TransportClient
 * 以便服务端进程将消息发回非客户端。
 */
@Slf4j
public class TransportContext {
	private final TransportConf conf;
	private final RpcHandler rpcHandler;
	private final boolean closeIdleConnections;

	private final MessageEncoder encoder;
	private final MessageDecoder decoder;

	/**
	 * 2019-10-08 16:47:33 [govind-server-1hread] [ERROR] TransportContext:
	 * 初始化channel pipeline时出错：govind.incubator.network.protocol.codec
	 * .TransportFrameDecoder is not a @Sharable handler, so can't be added
	 * or removed multiple times.
	 * io.netty.channel.ChannelPipelineException: govind.incubator.network.
	 * protocol.codec.TransportFrameDecoder is not a @Sharable handler, so
	 * can't be added or removed multiple times.
	 */
	//private final TransportFrameDecoder frameDecoder;

	public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
		this(conf, rpcHandler, false);
	}

	public TransportContext(TransportConf conf, RpcHandler rpcHandler, boolean closeIdleConnections) {
		this.conf = conf;
		this.rpcHandler = rpcHandler;
		this.closeIdleConnections = closeIdleConnections;
		this.encoder = new MessageEncoder();
		this.decoder = new MessageDecoder();
		//frameDecoder = NettyUtil.createFrameDecoder();
	}

	public TransportConf getConf() {
		return conf;
	}

	public TransportChannelHandler initializePipeline(SocketChannel channel) {
		return initializePipeline(channel, rpcHandler);
	}

	/**
	 * 使用MessageEncoder和MessageDecoder初始化pipeline，同时添加
	 * TransportChannelHandler用于处理请求和响应。
	 *
	 * @param ch    需要初始化的通道
	 * @param rpcHandler 用于当前Channel上的RPC回调
	 * @return
	 */
	public TransportChannelHandler initializePipeline(SocketChannel ch, RpcHandler rpcHandler) {
		try {
			TransportChannelHandler channelHandler = createChannelHandler(ch, rpcHandler);
			ch.pipeline()
					.addLast("encoder", encoder)
					.addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtil.createFrameDecoder())
					.addLast("decoder", decoder)
					.addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMS() / 1000))
					.addLast("handler", channelHandler);
			return channelHandler;
		} catch (Exception e) {
			log.error("初始化channel pipeline时出错：{}", e.getMessage());
			throw e;
		}
	}

	/**
	 * 创建用于客户端、服务端的(请求、响应)消息处理器，channel要确保
	 * 已经成功创建，否则有些配置如remoteAddr可能不可用！
	 * @param ch
	 * @param rpcHandler
	 * @return
	 */
	private TransportChannelHandler createChannelHandler(SocketChannel ch, RpcHandler rpcHandler) {
		TransportResponseHandler responseHandler = new TransportResponseHandler(ch);
		TransportClient client = new TransportClient(ch, responseHandler);
		TransportRequestHandler requestHandler = new TransportRequestHandler(ch, client, rpcHandler);
		return new TransportChannelHandler(client, requestHandler, responseHandler, conf.connectionTimeoutMS(), closeIdleConnections);
	}


	/*************************工厂方法***********************/
	public TransportServer createServer(String host, int port, List<TransportServerBootstrap> bootstraps) {
		return new TransportServer(host, port, this, rpcHandler, bootstraps);
	}

	public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
		return createServer(null, port, bootstraps);
	}

	public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
		return createServer(0, bootstraps);
	}

	public TransportServer createServer() {
		return createServer(Lists.newArrayList());
	}


	public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
		return new TransportClientFactory(this, bootstraps);
	}

	public TransportClientFactory createClientFactory() {
		return createClientFactory(Lists.newArrayList());
	}
}
