package govind.incubator.network;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.MapConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.TransportContext;
import io.netty.channel.Channel;
import org.apache.spark.network.client.RpcResponseCallback;
import org.junit.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-9
 *
 * 1、验证请求由于网络超时没有响应时，请求会返回失败同时关闭网络连接。
 *
 * 2、单元测试中，使用2s作为连接超时时间，在测试中可以设置松弛值以便
 * 保证在不同测试环境中的稳定性。
 *
 */
public class RequestTimeoutIntegrationSuite {
	private TransportServer server;
	private TransportClientFactory clientFactory;

	private StreamManager defaultManager;
	private TransportConf conf;

	/**
	 * 为了确保测试时，由于错误的测试用例导致进程被永久hang住，这里用60s代替“永久”
	 */
	private final int FOREVER = 60 * 1000;

	@Before
	public void setup() {
		Map<String, String> configMap = Maps.newHashMap();
		configMap.put("govind.network.shuffle.io.connectionTimeout", "2");
		conf = new TransportConf(new MapConfigProvider(configMap), "shuffle");

		defaultManager = new StreamManager() {
			@Override
			public ManagedBuffer getChunk(long streamId, int chunkIdx) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void connectionTerminated(Channel channel) {

			}
		};
	}

	@After
	public void tearDown() throws IOException {
		if (server != null) {
			server.close();
		}

		if (clientFactory != null) {
			clientFactory.close();
		}
	}

	/**
	 * 测试步骤：
	 * 1、第一个请求可以很快得到响应；
	 * 2、第二个请求等待时间超过设置的超时时间
	 */
	@Test
	public void timeoutInactiveRequests() throws IOException, InterruptedException {
		final Semaphore semaphore = new Semaphore(1);
		final int responseSize = 16;
		RpcHandler rpcHandler = new RpcHandler() {
			@Override
			public StreamManager getStreamManager() {
				return defaultManager;
			}

			@Override
			public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
				try {
					semaphore.tryAcquire(FOREVER, TimeUnit.SECONDS);
					callback.onSuccess(ByteBuffer.allocate(responseSize));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};

		TransportContext context = new TransportContext(conf, rpcHandler);
		server = context.createServer();
		clientFactory = context.createClientFactory();
		TransportClient client = clientFactory.createClient(getLocalHost(), server.getPort());

		//1. 第一个快速响应请求
		TestCallback callback0 = new TestCallback();
		synchronized (callback0) {
			client.sendRpcAsync(ByteBuffer.allocate(0), callback0);
			callback0.wait(FOREVER);
			assertEquals(responseSize, callback0.successLength);
		}

		//2. 第二个请求在2s后超时，响应错误信息应该为IOException
		TestCallback callback1 = new TestCallback();
		synchronized (callback1) {
			client.sendRpcAsync(ByteBuffer.allocate(0), callback1);
			callback1.wait(4*1000);
			assert(callback1.failure != null);
			assert(callback1.failure instanceof IOException);
		}
		semaphore.release();
	}

	/**
	 * 超时未响应导致连接关闭，同时导致当前TransportClient实例失效，
	 * 若要发送需要通过TransportClientFactory获取一个新的实例。
	 */
	@Test
	public void timeoutCleanlyClosesClient() throws IOException, InterruptedException {
		final Semaphore semaphore = new Semaphore(0);
		final int responseSize =  16;

		RpcHandler rpcHandler = new RpcHandler() {
			@Override
			public StreamManager getStreamManager() {
				return defaultManager;
			}

			@Override
			public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
				try {
					semaphore.tryAcquire(FOREVER, TimeUnit.SECONDS);
					callback.onSuccess(ByteBuffer.allocate(responseSize));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};

		TransportContext context = new TransportContext(conf, rpcHandler);
		server = context.createServer();
		clientFactory = context.createClientFactory();

		//第一个请求最终会失败
		TransportClient client0 = clientFactory.createClient(getLocalHost(), server.getPort());
		TestCallback callback0 = new TestCallback();
		synchronized (callback0) {
			client0.sendRpcAsync(ByteBuffer.allocate(0), callback0);
			callback0.wait(FOREVER);
			assert(callback0.failure instanceof IOException);
			assert (!client0.isActive());
		}

		//增加信号量，第二个请求会立刻得到响应
		semaphore.release(2);
		TransportClient client1 = clientFactory.createClient(getLocalHost(), server.getPort());
		TestCallback callback1 = new TestCallback();
		synchronized (callback1) {
			client1.sendRpcAsync(ByteBuffer.allocate(0), callback1);
			callback1.wait(FOREVER);
			assertEquals(responseSize, callback1.successLength);
			assertNull(callback1.failure);
		}
	}

	/**
	 * 测试相对于LAST Request end超时响应，测试在FetchChunk和Rpc
	 * 请求相对于上一次响应的超时处理机制。
	 */
	@Test
	public void furtherRequestsDelay() throws IOException, InterruptedException {
		byte[] response = new byte[16];
		final StreamManager  manager = new StreamManager() {
			@Override
			public ManagedBuffer getChunk(long streamId, int chunkIdx) {
				Uninterruptibles.sleepUninterruptibly(FOREVER, TimeUnit.SECONDS);
				return new NioManagedBuffer(ByteBuffer.wrap(response));
			}

			@Override
			public void connectionTerminated(Channel channel) {
			}
		};
		RpcHandler rpcHandler = new RpcHandler() {
			@Override
			public StreamManager getStreamManager() {
				return manager;
			}

			@Override
			public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
				throw new UnsupportedOperationException();
			}
		};

		TransportContext context = new TransportContext(conf, rpcHandler);
		server  = context.createServer();
		clientFactory = context.createClientFactory();

		TransportClient client0 = clientFactory.createClient(getLocalHost(), server.getPort());
		//发送第一个请求，这个请求最终会失败
		TestCallback callback0 = new TestCallback();
		client0.fetchChunk(0, 0, callback0);
		Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);


		//在第一个请求失败前，发送第二个请求
		TestCallback callback1 = new TestCallback();
		client0.fetchChunk(0,  1, callback1);
		Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

		synchronized (callback0) {
			// not complete yet, but should complete soon
			assertEquals(-1, callback0.successLength);
			//assertNull(callback0.failure);
			//callback0.wait(2  * 1000);
			assertTrue(callback0.failure instanceof IOException);
		}

		synchronized (callback1) {
			// failed at same time as previous
			assert (callback1.failure instanceof IOException);
		}
	}

	/**
	 * 1、若成功响应，则只记录返回数据的长度；
	 * 2、若响应失败，则只记录错误的原因；
	 * 3、在响应成功、失败时，调用notifyAll通知其他等待调用的线程。
	 */
	private static class TestCallback implements RpcCallback, ChunkReceivedCallback {

		int successLength = -1;
		Throwable failure;

		@Override
		public void onSuccess(int chunkIdx, ManagedBuffer buffer) {
			synchronized (this) {
				try {
					successLength = buffer.nioByteBuffer().remaining();
					this.notifyAll();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void onFailure(int chunkIdx, Throwable cause) {
			synchronized (this) {
				failure = cause;
				this.notifyAll();
			}
		}

		@Override
		public void onSuccess(ByteBuffer byteBuffer) {
			synchronized (this) {
				successLength =  byteBuffer.remaining();
				this.notifyAll();
			}
		}

		@Override
		public void onFailure(Throwable cause) {
			synchronized (this) {
				failure = cause;
				this.notifyAll();
			}
		}
	}

	private String getLocalHost() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}
}
