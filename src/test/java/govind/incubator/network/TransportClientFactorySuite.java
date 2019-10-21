package govind.incubator.network;

import com.google.common.io.Closeables;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.ConfigProvider;
import govind.incubator.network.conf.MapConfigProvider;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.NoOpRpcHandler;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Maps;
import sun.rmi.transport.Transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-21
 */
public class TransportClientFactorySuite {
	private TransportConf conf;
	private TransportContext context;
	private TransportServer server1;
	private TransportServer server2;

	@Before
	public void setup() {
		conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		RpcHandler rpcHandler = new NoOpRpcHandler();
		context = new TransportContext(conf, rpcHandler);
		server1 = context.createServer();
		server2 = context.createServer();
	}

	@After
	public void teardown() {
		Closeables.closeQuietly(server1);
		Closeables.closeQuietly(server2);
	}

	/**
	 * 向某个服务器请求建立多个客户端连接，测试是否满足{@code maxConnections}的限制
	 * @param maxConnections
	 * @param concurrent true表示采用多线程并行创建多个client
	 */
	private void testClientReuse(final int maxConnections, boolean concurrent) throws InterruptedException, IOException {
		Map<String, String> configMap = Maps.newHashMap();
		configMap.put("govind.network.shuffle.io.numConnectionsPerPeer", Integer.toString(maxConnections));

		TransportConf conf = new TransportConf(new MapConfigProvider(configMap), "shuffle");
		NoOpRpcHandler handler = new NoOpRpcHandler();
		TransportContext context = new TransportContext(conf, handler);

		final TransportClientFactory clientFactory = context.createClientFactory();
		final Set<TransportClient> clients = Collections.synchronizedSet(new HashSet<>());

		final AtomicInteger failed = new AtomicInteger();
		Thread[] attempts = new Thread[maxConnections * 10];

		//构建多个线程创建客户端连接
		for (int i = 0; i < attempts.length; i++) {
			attempts[i] = new Thread(() -> {
				try {
					TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server1.getPort());
					assert client.isActive();
					clients.add(client);
				} catch (IOException e) {
					failed.incrementAndGet();
				}
			});

			if (concurrent) {
				attempts[i].start();
			} else {
				attempts[i].run();
			}

		}

		//等待所有线程执行完成
		for (Thread attempt : attempts) {
			attempt.join();
		}

		assert failed.get() == 0;
		assert clients.size() == maxConnections;

		for (TransportClient client : clients) {
			client.close();
		}
	}

	@Test
	public void reuseClientsUptoConfigVariable() throws Exception {
		testClientReuse(1, false);
		testClientReuse(2, false);
		testClientReuse(3, false);
		testClientReuse(4, false);
	}

	@Test
	public void reuseClientsUptoConfigVariableConcurrent()  throws Exception {
		testClientReuse(1, true);
		testClientReuse(2, true);
		testClientReuse(3, true);
		testClientReuse(4, true);
	}

	@Test
	public void returnDifferentClientsForDifferentServers() throws Exception {
		TransportClientFactory factory = context.createClientFactory();
		TransportClient client1 = factory.createClient(NettyUtil.getLocalHost(), server1.getPort());
		TransportClient client2 = factory.createClient(NettyUtil.getLocalHost(), server2.getPort());

		assertTrue(client1.isActive());
		assertTrue(client2.isActive());
		assertTrue(client1 != client2);
		factory.close();
	}

	@Test
	public void neverReturnInactiveClient() throws Exception {
		TransportClientFactory factory = context.createClientFactory();
		TransportClient client1 = factory.createClient(NettyUtil.getLocalHost(), server1.getPort());
		client1.close();

		long  start = System.currentTimeMillis();
		while (client1.isActive() && (System.currentTimeMillis() - start) < 3000) {
			Thread.sleep(10);
		}
		assertFalse(client1.isActive());

		TransportClient client2 = factory.createClient(NettyUtil.getLocalHost(), server1.getPort());
		assertFalse(client1 == client2);
		assertTrue(client2.isActive());
		factory.close();
	}

	@Test
	public void closeBlockClientsWithFactory() throws Exception {
		TransportClientFactory factory = context.createClientFactory();
		TransportClient c1 = factory.createClient(NettyUtil.getLocalHost(), server1.getPort());
		TransportClient c2 = factory.createClient(NettyUtil.getLocalHost(), server2.getPort());

		assertTrue(c1.isActive());
		assertTrue(c2.isActive());
		factory.close();

		assertFalse(c1.isActive());
		assertFalse(c2.isActive());
	}

	@Test
	public void closeIdleConnectionForRequestTimeout() throws Exception {
		TransportConf conf = new TransportConf(new ConfigProvider() {
			@Override
			public String get(String name) {
				if ("govind.network.shuffle.io.connectionTimeout".equals(name)) {
					return "1";
				}
				String value = System.getProperty(name);
				if (value == null) {
					throw new NoSuchElementException(name);
				}
				return value;
			}
		}, "shuffle");

		TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
		TransportClientFactory factory = context.createClientFactory();

		try {
			TransportClient c1 = factory.createClient(NettyUtil.getLocalHost(), server1.getPort());
			assertTrue(c1.isActive());

			long expiredTime = System.currentTimeMillis() + 10000; //10s
			while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
				Thread.sleep(10);
			}
			assertFalse(c1.isActive());
		} finally {
			factory.close();
		}
	}

}
