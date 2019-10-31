package govind.incubator.network;

import clojure.lang.Obj;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import govind.incubator.network.buffer.FileSegmentManagedBuffer;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientBootstrap;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.sasl.*;
import govind.incubator.network.sasl.SaslEncryption.EncryptedMessage;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.server.TransportServerBootstrap;
import govind.incubator.network.util.ByteArrayWritableChannel;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.security.sasl.SaslException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-31
 * <p>
 * Jointly tests GovindSaslClient and GovindSaslServer, as both are black boxes.
 */
public class SaslSuite {

	/**
	 * 定义一个密钥为appId的SecretKeyHolder
	 */
	private SecretKeyHolder secretKeyHolder = new SecretKeyHolder() {
		@Override
		public String getSaslUser(String appId) {
			return "user";
		}

		@Override
		public String getSecretKey(String appId) {
			return appId;
		}
	};


	@Test
	public void testMatching() {
		GovindSaslClient saslClient = new GovindSaslClient(secretKeyHolder, "shared-secret", false);
		GovindSaslServer saslServer = new GovindSaslServer("shared-secret", secretKeyHolder, false);

		assertFalse(saslClient.isComplete());
		assertFalse(saslServer.isComplete());

		byte[] token = saslClient.firstToken();
		while (!saslClient.isComplete()) {
			token = saslClient.response(saslServer.response(token));
		}
		assertTrue(saslServer.isComplete());

		//disposal should invalidate
		saslServer.dispose();
		assertFalse(saslServer.isComplete());
		saslClient.dispose();
		assertFalse(saslClient.isComplete());
	}

	@Test
	public void testNonMatching() {
		GovindSaslClient saslClient = new GovindSaslClient(secretKeyHolder, "my-secret", false);
		GovindSaslServer saslServer = new GovindSaslServer("your-secret", secretKeyHolder, false);

		assertFalse(saslClient.isComplete());
		assertFalse(saslServer.isComplete());

		byte[] token = saslClient.firstToken();
		try {
			while (!saslServer.isComplete()) {
				token = saslClient.response(saslServer.response(token));
			}
			fail("should not have complete!");
		} catch (Exception e) {
			assertTrue(e.getMessage().contains("Mismatched response"));
			assertFalse(saslClient.isComplete());
			assertFalse(saslServer.isComplete());
		}
	}

	@Test
	public void testSaslAuthentication() throws Throwable {
		testBasicSasl(false);
	}

	@Test
	public void testSaslEncryption() throws Throwable {
		testBasicSasl(true);
	}

	private void testBasicSasl(boolean encrypt) throws Throwable {
		RpcHandler rpcHandler = mock(RpcHandler.class);
		doAnswer((Answer<Void>) invocation -> {
			ByteBuffer message = (ByteBuffer) invocation.getArguments()[1];
			RpcCallback cb = (RpcCallback) invocation.getArguments()[2];
			assertEquals("Ping", NettyUtil.bytesToString(message));
			cb.onSuccess(NettyUtil.stringToBytes("Pong"));
			return null;
		})
				.when(rpcHandler)
				.receive(any(TransportClient.class), any(ByteBuffer.class), any(RpcCallback.class));

		SaslTestCtx testCtx = null;
		try {
			testCtx = new SaslTestCtx(rpcHandler, encrypt, false);

			ByteBuffer resp = testCtx.client.sendRpcSync(NettyUtil.stringToBytes("Ping"), TimeUnit.SECONDS.toMillis(10));
			assertEquals("Pong", NettyUtil.bytesToString(resp));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			testCtx.close();

			//应该有两个终止事件：一个来自client，另一个来自server
			Throwable error = null;
			long deadline = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(20, TimeUnit.SECONDS);
			while (deadline > System.currentTimeMillis()) {
				try {
					verify(rpcHandler, times(2))
							.connectionTerminated(any(TransportClient.class));
					error = null;
					break;
				} catch (Exception e) {
					error = e;
					TimeUnit.MILLISECONDS.sleep(100);
				}
			}
			if (error != null) {
				throw error;
			}
		}
	}


	@Test
	public void testEncryptedMessage() throws Exception {
		SaslEncryptionBackend backend = mock(SaslEncryptionBackend.class);
		byte[] data = new byte[1024];
		new Random().nextBytes(data);
		when(backend.wrap(any(byte[].class), anyInt(), anyInt())).thenReturn(data);

		ByteBuf msg = Unpooled.buffer();

		try {
			msg.writeBytes(data);

			//Create a channel with a really small buffer compared to the data.
			// This means that on each call, the outbound data will not be fully
			// written, so the write() method should return a dummy count to keep 
			// the channel alive when possible.
			ByteArrayWritableChannel channel = new ByteArrayWritableChannel(32);

			EncryptedMessage emsg = new EncryptedMessage(backend, msg, 1024);
			long count = emsg.transferTo(channel, emsg.transfered());
			assertTrue(count < data.length);
			assertTrue(count > 0);

			// Here, the output buffer is full so nothing should be transferred.
			assertEquals(0, emsg.transferTo(channel, emsg.transfered()));

			//Now there's room in the buffer, but not enough to transfer all
			// the remaining data,so the dummy count should be returned.
			channel.reset();
			assertEquals(1, emsg.transferTo(channel, emsg.transfered()));

			//Eventually, the whole message should be transferred.
			for (int i = 0; i < data.length / 32 - 2; i++) {
				channel.reset();
				assertEquals(1, emsg.transferTo(channel, emsg.transfered()));
			}

			channel.reset();
			count = emsg.transferTo(channel, emsg.transfered());
			assertTrue("Unexpected count:" + count, count > 1 && count < data.length);
			assertEquals(data.length, emsg.transfered());
		} finally {
			msg.release();
		}
	}


	@Test
	public void testEncryptedMessageChunking() throws Exception {
		File file = File.createTempFile("sasltest", "txt");

		try {
			TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");

			byte[] data = new byte[8 * 1024];
			new Random().nextBytes(data);
			Files.write(data, file);

			SaslEncryptionBackend backend = mock(SaslEncryptionBackend.class);
			// It doesn't really matter what we return here, as long as it's not null.
			when(backend.wrap(any(byte[].class), anyInt(), anyInt())).thenReturn(data);

			FileSegmentManagedBuffer msg = new FileSegmentManagedBuffer(file, 0, file.length());
			EncryptedMessage emsg = new EncryptedMessage(backend, msg.nettyByteBuf(), 1024);

			ByteArrayWritableChannel channel = new ByteArrayWritableChannel(data.length);
			while (emsg.transfered() < emsg.count()) {
				channel.reset();
				emsg.transferTo(channel, emsg.transfered());
			}

			verify(backend, times(8)).wrap(any(byte[].class), anyInt(), anyInt());

		} finally {
			file.delete();
		}
	}


	@Test
	public void testFileRegionEncryption() throws Exception {
		final String blockSizeConf = "network.sasl.maxEncryptedBlockSize";
		System.setProperty(blockSizeConf, "1024");

		final File file = File.createTempFile("sasltest", "txt");
		final AtomicReference<ManagedBuffer> response = new AtomicReference<>();
		SaslTestCtx testCtx = null;

		try {
			TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
			StreamManager streamManager = mock(StreamManager.class);
			when(streamManager.getChunk(anyLong(), anyInt()))
			.thenAnswer((Answer<ManagedBuffer>) invocationOnMock ->
					new FileSegmentManagedBuffer(file,0, file.length()));

			RpcHandler rpcHandler = mock(RpcHandler.class);
			when(rpcHandler.getStreamManager()).thenReturn(streamManager);

			byte[] data = new byte[8 * 1024];
			new Random().nextBytes(data);
			Files.write(data, file);

			testCtx = new SaslTestCtx(rpcHandler, true, false);

			final Object lock = new Object();

			ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
			doAnswer((Answer<Void>) invocation -> {
				response.set((ManagedBuffer)invocation.getArguments()[1]);
				response.get().retain();
				synchronized (lock) {
					lock.notifyAll();
				}
				return null;
			}).when(callback).onSuccess(anyInt(), any(ManagedBuffer.class));

			synchronized (lock) {
				testCtx.client.fetchChunk(0, 0, callback);
				lock.wait(10 * 1000);
			}

			verify(callback, times(1)).onSuccess(anyInt(),any(ManagedBuffer.class));
			verify(callback, never()).onFailure(anyInt(), any(Throwable.class));

			byte[] received = ByteStreams.toByteArray(response.get().createInputStream());
			assertTrue(Arrays.equals(data, received));
		} finally {

			file.delete();
			if (testCtx != null) {
				testCtx.close();
			}

			if (response.get() != null) {
				response.get().release();
			}

			System.clearProperty(blockSizeConf);
		}


	}

	@Test
	public void testServerAlwaysEncrypt() throws Exception {
		final String alwaysEncryptConfName = "network.sasl.serverAlwaysEncrypt";
		System.setProperty(alwaysEncryptConfName, "true");

		SaslTestCtx testCtx = null;
		try {
			testCtx = new SaslTestCtx(mock(RpcHandler.class), false, false);
			fail("没有加密传输，应该会抛出异常");
		} catch (Exception e){
			assertTrue(e.getCause() instanceof SaslException);
		} finally {
			if (testCtx != null) {
				testCtx.close();
			}
			System.clearProperty(alwaysEncryptConfName);
		}
	}

	@Test
	public void testDataEncryptionIsActuallyEnabled() throws Exception {
		// This test sets up an encrypted connection but then, using a client
		// bootstrap, removes the encryption handler from the client side. This
		// should cause the server to not be able to understand RPCs sent to it
		// and thus close the connection.
		SaslTestCtx testCtx = null;
		try {
			testCtx = new SaslTestCtx(mock(RpcHandler.class), true, true);
			testCtx.client.sendRpcSync(NettyUtil.stringToBytes("Ping"), TimeUnit.SECONDS.toMillis(10));
			fail("发送Rpc消息给服务端应该失败");
		} catch (Exception e){
			assertFalse(e.getCause() instanceof TimeoutException);
		} finally {
			if (testCtx != null) {
				testCtx.close();
			}
		}
	}

	@Test
	public void testRpcHandlerDelegate() throws Exception {
		//Tests all delegates exception for receive(), which is more
		// complicated and already handled by all other tests.
		RpcHandler rpcHandler = mock(RpcHandler.class);
		SaslRpcHandler saslRpcHandler = new SaslRpcHandler(null, null, rpcHandler, null);

		saslRpcHandler.getStreamManager();
		verify(rpcHandler).getStreamManager();

		saslRpcHandler.connectionTerminated(null);
		verify(rpcHandler).connectionTerminated(any());

		saslRpcHandler.exceptionCaught(null, null);
		verify(rpcHandler).exceptionCaught(any(), any());
	}

	private static class SaslTestCtx {
		final TransportClient client;
		final TransportServer server;

		final boolean encrypt;
		final boolean disableClientEncryption;
		final EncryptionCheckerBootstrap checker;

		public SaslTestCtx(RpcHandler rpcHandler, boolean encrypt, boolean disableClientEncryption) throws Exception {
			this.encrypt = encrypt;
			this.disableClientEncryption = disableClientEncryption;

			TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
			SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
			when(keyHolder.getSaslUser(anyString())).thenReturn("user");
			when(keyHolder.getSecretKey(anyString())).thenReturn("secret");

			TransportContext context = new TransportContext(conf, rpcHandler);
			this.checker = new EncryptionCheckerBootstrap();
			this.server = context.createServer(Arrays.asList(new SaslServerBootstrap(conf, keyHolder), checker));

			try {
				List<TransportClientBootstrap> clientBootstraps = Lists.newArrayList();
				clientBootstraps.add(new SaslClientBootstrap(encrypt, "user", conf, keyHolder));
				if (disableClientEncryption) {
					clientBootstraps.add(new EncrptionDisablerBootstrap());
				}

				this.client = context
						.createClientFactory(clientBootstraps)
						.createClient(NettyUtil.getLocalHost(), server.getPort());
			} catch (Exception e) {
				close();
				throw e;
			}
		}

		public void close() throws IOException {
			if (!disableClientEncryption) {
				assertEquals(encrypt, checker.foundEncryptionHandler);
			}
			if (client != null) {
				client.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}

	static class EncryptionCheckerBootstrap extends ChannelOutboundHandlerAdapter implements TransportServerBootstrap {

		boolean foundEncryptionHandler;

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			if (!foundEncryptionHandler) {
				foundEncryptionHandler = ctx.channel().pipeline().get(SaslEncryption.ENCRYPTION_HANDLER_NAME) != null;
			}
			ctx.write(msg, promise);
		}

		@Override
		public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
			channel.pipeline().addFirst("encryptionChecker", this);
			return rpcHandler;
		}
	}

	static class EncrptionDisablerBootstrap implements TransportClientBootstrap {

		@Override
		public void doBootstrap(TransportClient transportClient, Channel channel) throws RuntimeException {
			channel.pipeline().remove(SaslEncryption.ENCRYPTION_HANDLER_NAME);
		}
	}
}
