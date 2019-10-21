package govind.incubator.network.client;

import com.google.common.io.Closeables;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.TransportChannelHandler;
import govind.incubator.network.util.IOMode;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 *
 * TransportClient的工厂类:
 *	1、内部维护一个连接池，对于相同的remote连接，返回同一个TransportClient；
 *	2、所有的TransportClient实例共享单一线程的线程池；
 *
 * TransportClient会尽可能重用，在创建TransportClient实例前需要将
 * 提供的{@link TransportClientBootstrap}进行装配。
 *
 */
@Slf4j
public class TransportClientFactory implements Closeable {

	/**
	 * 保存连接相同目的地址的TransportClient池
	 */
	private static class ClientPool {
		TransportClient[] clients;
		Object[] locks;

		public ClientPool(int size) {
			this.clients = new TransportClient[size];
			this.locks = new Object[size];

			for (int i = 0; i < size; i++) {
				locks[i] = new Object();
			}
		}
	}


	private final TransportConf conf;
	private final TransportContext context;
	private final List<TransportClientBootstrap> bootstraps;
	private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPools;

	/**
	 * 随机从ClientPool中选择一个TransportClient
	 */
	private final Random random;
	private final int numConnectionPerPeer;

	private final Class<? extends Channel> socketChannelClz;
	private EventLoopGroup workerGroup;
	private PooledByteBufAllocator pooledAllocator;

	public TransportClientFactory(TransportContext context, List<TransportClientBootstrap> bootstraps) {
		this.context = context;
		this.bootstraps = bootstraps;

		this.conf = context.getConf();
		this.connectionPools = new ConcurrentHashMap<>();
		this.random = new Random();
		this.numConnectionPerPeer = conf.numConnectionsPerPeer();

		IOMode ioMode = IOMode.valueOf(conf.ioMode());
		socketChannelClz = NettyUtil.getClientChannelClass(ioMode);
		workerGroup = NettyUtil.createEventLoopGroup(ioMode, conf.clientThreads(), "govind-client");

		this.pooledAllocator = NettyUtil.createPooledByteBufAllocator(conf.preferDirectBufs(), false, conf.clientThreads());
	}

	/**
	 * 建一个连接指host/port的TransportClient实例
	 *
	 * 为每个SocketAddress(remoteHost, remotePort)>维护一个
	 * TransportClient池，集合个数由numConnectionPerPeer决定，每
	 * 次从中随机选择一个。若池中没有，则会创建一个实例并将其加入池中。
	 *
	 * 说明：
	 * 	1、该方法会阻塞至成功与远端建立连接并且完全启动；
	 * 	2、该方法是线程安全的；
	 *
	 * @param remoteHost 服务器地址
	 * @param remotePort 服务器端口
	 * @return
	 * @throws IOException
	 */
	public TransportClient createClient(String remoteHost,  int remotePort) throws IOException{
		final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);

		ClientPool clientPool = connectionPools.get(address);
		if (clientPool == null) {
			connectionPools.putIfAbsent(address, new ClientPool(numConnectionPerPeer));
			clientPool = connectionPools.get(address);
		}

		int clientIdx = random.nextInt(numConnectionPerPeer);
		TransportClient cachedClient = clientPool.clients[clientIdx];
		if (cachedClient != null && cachedClient.isActive()) {
			TransportChannelHandler channelHandler = cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);

			synchronized (channelHandler) {
				channelHandler.getResponseHandler().updateTimeOfLastRequest();
			}

			if (cachedClient.isActive()) {
				log.debug("返回缓存的连接到{}的TransportClient：{}", address, cachedClient);
				return cachedClient;
			}
		}

		//!! 池中没有可用的TransportClient实例，则尝试创建后添加到池中
		synchronized (clientPool.locks[clientIdx]) {
			cachedClient = clientPool.clients[clientIdx];
			if (cachedClient != null) {
				if (cachedClient.isActive()) {
					log.debug("TransportClient已经被创建，且处于活动状态，可以直接使用");
					return cachedClient;
				} else {
					log.debug("TransportClient处于非活动状态，不可用，因此将重新创建实例");
				}
			}

			clientPool.clients[clientIdx] = createClient(address);
			return clientPool.clients[clientIdx];
		}
	}

	/**
	 * 根据SocketAddress创建TransportClient实例
	 *
	 * PS：该方法创建的实例没有添加到缓存池中！
	 *
	 * @param address
	 * @return
	 * @throws IOException
	 */
	public TransportClient  createClient(InetSocketAddress address) throws IOException {
		log.debug("创建TransportClient，目标地址为：{}", address);

		Bootstrap bootstrap = new Bootstrap();

		bootstrap.group(workerGroup)
				.channel(socketChannelClz)
				//禁止Nagle算法，因为不想要报文等待
				.option(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMS())
				.option(ChannelOption.ALLOCATOR, pooledAllocator);

		//由于需要将TransportClient和ch取出来，因此需要采用下面方式
		final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
		final AtomicReference<Channel> chRef = new AtomicReference<>();
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				TransportChannelHandler channelHandler = context.initializePipeline(ch);

				clientRef.set(channelHandler.getClient());
				chRef.set(ch);
			}
		});

		long preConnect = System.currentTimeMillis();
		ChannelFuture future = bootstrap.connect(address);

		if (!future.awaitUninterruptibly(conf.connectionTimeoutMS())) {
			throw new IOException(String.format("连接%s超时(%s ms)", address, conf.connectionTimeoutMS()));
		} else if (future.cause() != null){
			throw new IOException(String.format("连接%s出错：%s", address, future.cause().getMessage()));
		}

		TransportClient client = clientRef.get();
		Channel channel = chRef.get();
		assert client != null : "channel future成功连接，但是TransportClient为null！";

		//在返回TransportClient之前，为每个client执行bootstraps装配操作
		long preBootstrap = System.currentTimeMillis();
		log.info("成功连接{}, 开始为TransportClient装配bootstraps....", address);

		try {
			for (TransportClientBootstrap bs : bootstraps) {
				bs.doBootstrap(client, channel);
			}
		} catch (RuntimeException e) {
			long bootstrapTimeMs = (System.currentTimeMillis() - preBootstrap) / 1000;
			log.error("Exception while bootstrapping client after {} ms, exception: ", bootstrapTimeMs, e.getMessage());

			client.close();
			throw new RuntimeException(e);
		}

		long postBootstrap = System.currentTimeMillis();
		log.info("成功创建到{}的连接，总耗时：{}ms，其中bootstraps耗时：{}", address,
				(postBootstrap- preConnect)/1000, (postBootstrap - preBootstrap)/1000);
		return client;
	}

	/**
	 * 创建一个不进行池化的TransportClient实例
	 */
	public TransportClient createUnmanagedClient(String remoteHost, int remotePort) throws IOException {
		final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
		return createClient(address);
	}

	@Override
	public void close() throws IOException {
		for (ClientPool clientPool : connectionPools.values()) {
			for (int i = 0; i < clientPool.clients.length; i++) {
				if (clientPool.clients[i] != null) {
					Closeables.closeQuietly(clientPool.clients[i]);
					clientPool.clients[i] = null;
				}
			}
		}
		connectionPools.clear();

		if (workerGroup != null) {
			workerGroup.shutdownGracefully();
			workerGroup = null;
		}
	}
}
