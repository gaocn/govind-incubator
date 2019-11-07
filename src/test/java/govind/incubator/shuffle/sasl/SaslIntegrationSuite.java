package govind.incubator.shuffle.sasl;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.*;
import govind.incubator.network.sasl.SaslClientBootstrap;
import govind.incubator.network.sasl.SaslServerBootstrap;
import govind.incubator.network.sasl.SecretKeyHolder;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import govind.incubator.shuffle.BlockFetchingListener;
import govind.incubator.shuffle.ExternalShuffleBlockHandler;
import govind.incubator.shuffle.ExternalShuffleBlockResolver;
import govind.incubator.shuffle.OneForOneBlockFetcher;
import govind.incubator.shuffle.protocol.*;
import govind.incubator.shuffle.protocol.BlockTransferMessage.Decoder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 */
public class SaslIntegrationSuite {
	//为了确保在配置低、负载高的机器上可以正常运行，使用一个长的超时时间，
	// 通常情况下会再次之前很早结束
	private final static long TIMEOUT_MS = 10_000;

	static TransportServer server;
	static TransportConf conf;
	static TransportContext context;
	static SecretKeyHolder secretKeyHolder;

	TransportClientFactory clientFactory;

	@BeforeClass
	public static void beforeAll() {
		conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		context = new TransportContext(conf, new TestRpcHandler());

		secretKeyHolder = mock(SecretKeyHolder.class);
		when(secretKeyHolder.getSaslUser(eq("app-1"))).thenReturn("app1");
		when(secretKeyHolder.getSecretKey(eq("app-1"))).thenReturn("app1");
		when(secretKeyHolder.getSaslUser(eq("app-2"))).thenReturn("app2");
		when(secretKeyHolder.getSecretKey(eq("app-2"))).thenReturn("app2");
		when(secretKeyHolder.getSaslUser(anyString())).thenReturn("other-app");
		when(secretKeyHolder.getSecretKey(anyString())).thenReturn("other-passwd");

		SaslServerBootstrap bootstrap = new SaslServerBootstrap(conf, secretKeyHolder);
		server = context.createServer(Arrays.asList(bootstrap));
	}

	@AfterClass
	public static void afterAll() {
		Closeables.closeQuietly(server);
	}

	@After
	public void afterEach() {
		Closeables.closeQuietly(clientFactory);
	}

	@Test
	public void testGoodClient() throws IOException {
		clientFactory = context.createClientFactory(
				Lists.newArrayList(
						new SaslClientBootstrap("app-1", conf, secretKeyHolder)
				)
		);

		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());
		String msg = "Hello,World!";
		ByteBuffer resp = client.sendRpcSync(NettyUtil.stringToBytes(msg), TIMEOUT_MS);
		assertEquals(msg, NettyUtil.bytesToString(resp));
	}


	@Test
	public void testBadClient() {

		SecretKeyHolder secretKeyHolder1 = mock(SecretKeyHolder.class);
		when(secretKeyHolder1.getSaslUser(anyString())).thenReturn("wrong-user");
		when(secretKeyHolder1.getSecretKey(anyString())).thenReturn("wrong-passwd");

		clientFactory = context.createClientFactory(
				Lists.newArrayList(
						new SaslClientBootstrap("uknown-app", conf, secretKeyHolder1)
				)
		);

		try {
			TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());
			client.sendRpcSync(NettyUtil.stringToBytes("Hello, World!"), TIMEOUT_MS);
			fail("应该抛出异常");
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("Mismatched response"));
		}
	}

	@Test
	public void  testNoSaslClient() throws IOException {
		clientFactory = context.createClientFactory( Lists.newArrayList());
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());

		try {
			client.sendRpcSync(ByteBuffer.allocate(13), TIMEOUT_MS);
			fail("由于没有安全认证，因此应该会抛出异常");
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("期望接收的是SaslMessage消息"));
		}

		try {
			//测试：即使猜对了包头标识，但是后续的解析也会失效
			client.sendRpcSync(ByteBuffer.wrap(new byte[]{(byte) 0xEA}), TIMEOUT_MS);
			fail("由于无法解析消息，因此应该会抛出异常");
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("java.lang.IndexOutOfBoundsException"));
		}
	}

	@Test
	public void testNoSaslServer() {
		TransportServer noSaslServer = context.createServer();
		TransportClientFactory clientFactory = context.createClientFactory(
				Lists.newArrayList(
						new SaslClientBootstrap("app", conf, secretKeyHolder)
				)
		);

		TransportClient client = null;
		try {
			client = clientFactory.createClient(NettyUtil.getLocalHost(), noSaslServer.getPort());
			client.sendRpcSync(NettyUtil.stringToBytes("Hello, World"), TIMEOUT_MS);
			fail("服务端应该无法解析消息");
		} catch (Exception e) {
			assertTrue(e.getMessage(), e.getMessage().contains("Digest-challenge format violation"));
		} finally {
			Closeables.closeQuietly(noSaslServer);
		}
	}


	/**
	 * 测试shuffle服务能够基于SASL进行认证、加密
	 */
	@Test
	public void testAppIsolation() {
		ExternalShuffleBlockResolver blockResolver = mock(ExternalShuffleBlockResolver.class);
		ExternalShuffleBlockHandler blockHandler = new ExternalShuffleBlockHandler(blockResolver, new OneForOneStreamManager());
		SaslServerBootstrap serverBootstrap = new SaslServerBootstrap(conf, secretKeyHolder);
		TransportContext serverCtx = new TransportContext(conf, blockHandler);
		TransportServer blockServer = serverCtx.createServer(Arrays.asList(serverBootstrap));

		TransportClient client1 = null;
		TransportClient client2 = null;
		TransportClientFactory  clientFactory2 = null;

		try {

			//1、创建client并使用不同的app请求blocks
			clientFactory = serverCtx.createClientFactory(Lists.newArrayList(
					new SaslClientBootstrap("app-1", conf, secretKeyHolder)
			));

			client1 = clientFactory.createClient(NettyUtil.getLocalHost(), blockServer.getPort());

			AtomicReference<Throwable> exception = new AtomicReference<>();

			final Semaphore failedBlocks = new Semaphore(0);
			BlockFetchingListener listener = new BlockFetchingListener() {
				@Override
				public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
				}

				@Override
				public void onBlockFetchFailure(String blockId, Throwable cause) {
					exception.set(cause);
					failedBlocks.release();
				}
			};

			String[] blockIds = new String[]{"shuffle_2_3_4", "shuffle_6_7_8"};
			OneForOneBlockFetcher blockFetcher = new OneForOneBlockFetcher(client1, "app-2", "exec-0", blockIds, listener);

			blockFetcher.start();
			if (failedBlocks.tryAcquire(blockIds.length, 2, TimeUnit.SECONDS)) {
				checkSecurityException(exception.get());
			} else {
				fail("应该会抛出SecurityException");
			}

			// 2、注册ExecutorShuffle信息，以便能够
			ExecutorShuffleInfo shuffleInfo = new ExecutorShuffleInfo(new String[]{
					System.getProperty("java.io.tmpdir")}, 1,
					"org.apache.spark.shuffle.sort.SortShuffleManager"
			);

			RegisterExecutor registerExecutor = new RegisterExecutor("app-1", "exec-0", shuffleInfo);
			client1.sendRpcSync(registerExecutor.toByteBuffer(),  TIMEOUT_MS);


			//请求blocks，但是不会获取blocks而是保持stream open
			OpenBlock openBlock = new OpenBlock("app-1", "exec-0", blockIds);
			ByteBuffer resp = client1.sendRpcSync(openBlock.toByteBuffer(), TIMEOUT_MS);
			StreamHandle streamHandle = (StreamHandle) Decoder.fromByteByffer(resp);
			long streamId = streamHandle.streamId;

			//创建另一个不同app 的client2，，并尝试获取前一个app client1打开的stream
			clientFactory2 = serverCtx.createClientFactory(Lists.newArrayList(
					new SaslClientBootstrap("app-2", conf, secretKeyHolder)
			));
			client2 = clientFactory2.createClient(NettyUtil.getLocalHost(), blockServer.getPort());
			exception.set(null);
			ChunkReceivedCallback chunkReceivedCallback = new ChunkReceivedCallback() {
				@Override
				public void onSuccess(int chunkIdx, ManagedBuffer buffer) {
				}

				@Override
				public void onFailure(int chunkIdx, Throwable cause) {
					exception.set(cause);
					failedBlocks.release();
				}
			};
			client2.fetchChunk(streamId, 0, chunkReceivedCallback);

			if (failedBlocks.tryAcquire(1, 2, TimeUnit.SECONDS)) {
				checkSecurityException(exception.get());
			} else {
				fail("应该会抛出SecurityException");
			}
		} catch (Exception e) {
		} finally {
			Closeables.closeQuietly(client1);
			Closeables.closeQuietly(client2);
			Closeables.closeQuietly(clientFactory2);
			Closeables.closeQuietly(blockServer);
		}
	}

	private void checkSecurityException(Throwable cause) {
		assertNotNull("没有异常信息", cause);
		assertTrue("应该是SecurityException实例",
				cause.getMessage().contains(SecurityException.class.getName()));
	}

	static class TestRpcHandler extends RpcHandler {
		@Override
		public StreamManager getStreamManager() {
			return new OneForOneStreamManager();
		}

		@Override
		public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
			callback.onSuccess(msg);
		}
	}
}
