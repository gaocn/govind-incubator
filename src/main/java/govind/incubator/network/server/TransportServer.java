package govind.incubator.network.server;

import com.google.common.io.Closeables;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.util.IOMode;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 */
@Slf4j
public class TransportServer implements Closeable {
	private final TransportConf conf;
	private final TransportContext context;
	private final RpcHandler appRpcHandler;
	private final List<TransportServerBootstrap> bootstraps;

	private int  port = -1;
	private ChannelFuture future;
	private ServerBootstrap bootstrap;

	public TransportServer(String hostToBind, int portToBind, TransportContext context, RpcHandler rpcHandler, List<TransportServerBootstrap> bootstraps) {
		this.context = context;
		this.appRpcHandler = rpcHandler;
		this.bootstraps = bootstraps;
		this.port = portToBind;
		this.conf = context.getConf();

		try {
			init(hostToBind, portToBind);
		} catch (Exception e) {
			Closeables.closeQuietly(this);
			throw e;
		}
	}

	private void init(String hostToBind, int portToBind) {
		log.debug("服务端初始化<IP={}, port={}>......", hostToBind, portToBind);

		IOMode ioMode = IOMode.valueOf(conf.ioMode());
		EventLoopGroup bossGroup = NettyUtil.createEventLoopGroup(ioMode, conf.serverThreads(), "govind-server");
		EventLoopGroup workGroup = bossGroup;
		Class<? extends ServerChannel> serverChannelClasss = NettyUtil.getServerChannelClasss(ioMode);

		PooledByteBufAllocator allocator = NettyUtil.createPooledByteBufAllocator(conf.preferDirectBufs(), true, conf.serverThreads());

		bootstrap = new ServerBootstrap()
				.group(bossGroup, workGroup)
				.channel(serverChannelClasss)
				.option(ChannelOption.ALLOCATOR, allocator)
				.childOption(ChannelOption.ALLOCATOR, allocator);
		if (conf.backlog() > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, conf.backlog());
		}

		if (conf.receiveBuffer() > 0) {
			bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuffer());
		}

		if (conf.sendBuffer() > 0) {
			bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuffer());
		}

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				RpcHandler rpcHandler = appRpcHandler;
				for (TransportServerBootstrap  bootstrap : bootstraps) {
					rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
				}
				context.initializePipeline(ch, rpcHandler);
			}
		});

		InetSocketAddress address =  hostToBind == null ?
				new InetSocketAddress(portToBind) :
				new InetSocketAddress(hostToBind, portToBind);

		future = bootstrap.bind(address);
		future.syncUninterruptibly();
		port = ((InetSocketAddress)future.channel().localAddress()).getPort();

		log.info("服务器成功启动，监听端口为：{}", port);
	}

	public int getPort() {
		if (port == -1) {
			throw new IllegalArgumentException("服务端尚未初始化");
		}
		return port;
	}

	@Override
	public void close() {
		if (future != null) {
			future.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
		}

		if (bootstrap != null && bootstrap.group() != null) {
			bootstrap.group().shutdownGracefully();
		}

		if (bootstrap != null && bootstrap.childGroup() != null) {
			bootstrap.childGroup().shutdownGracefully();
		}

		bootstrap = null;
	}
}
