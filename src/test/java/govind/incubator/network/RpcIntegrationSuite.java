package govind.incubator.network;

import com.google.common.collect.Sets;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.OneForOneStreamManager;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-9
 */
public class RpcIntegrationSuite {
	static TransportServer server;
	static TransportClientFactory clientFactory;
	static RpcHandler rpcHandler;
	static List<String> oneWayMsgs;

	@BeforeClass
	public static void setup() {
		TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");

		rpcHandler = new RpcHandler() {
			@Override
			public StreamManager getStreamManager() {
				return new OneForOneStreamManager();
			}

			@Override
			public void receive(TransportClient client, ByteBuffer message, RpcCallback callback) {
				String msg = NettyUtil.bytesToString(message);
				String[] parts = msg.split("/");

				if (parts[0].equals("hello")) {
					callback.onSuccess(NettyUtil.stringToBytes("Hello, " + parts[1] + "!"));
				} else if (parts[0].equals("return error")) {
					callback.onFailure(new RuntimeException("Returned: " + parts[1]));
				} else if(parts[0].equals("throw error")) {
					throw new RuntimeException("Thrown: " + parts[1]);
				}
			}

			@Override
			public void receive(TransportClient client, ByteBuffer msg) {
				oneWayMsgs.add(NettyUtil.bytesToString(msg));
			}
		};

		TransportContext context = new TransportContext(conf, rpcHandler);
		server = context.createServer();
		clientFactory = context.createClientFactory();
		oneWayMsgs = new ArrayList<>();
	}

	@AfterClass
	public static void tearDown() throws IOException {
		server.close();
		clientFactory.close();
	}

	class RpcResult {
		public Set<String> successMessages;
		public Set<String> errorMessages;
	}

	private RpcResult sendRpc(String ... commands) throws Exception {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());
		final Semaphore semaphore = new Semaphore(0);
		final RpcResult res = new RpcResult();
		res.successMessages = Collections.synchronizedSet(new HashSet<>());
		res.errorMessages = Collections.synchronizedSet(new HashSet<>());

		RpcCallback callback = new RpcCallback() {
			@Override
			public void onSuccess(ByteBuffer response) {
				res.successMessages.add(NettyUtil.bytesToString(response));
				semaphore.release();
			}

			@Override
			public void onFailure(Throwable cause) {
				res.errorMessages.add(cause.getMessage());
				semaphore.release();
			}
		};

		for (String command : commands) {
			client.sendRpcAsync(NettyUtil.stringToBytes(command), callback);
		}

		if (!semaphore.tryAcquire(commands.length,  5, TimeUnit.SECONDS)) {
			fail("从服务端获取结果超时");
		}
		client.close();
		return res;
	}

	@Test
	public void singleRPC() throws Exception {
		RpcResult res = sendRpc("hello/Aaron");
		assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
		assertTrue(res.errorMessages.isEmpty());
	}

	@Test
	public void doubleRPC() throws Exception {
		RpcResult res = sendRpc("hello/Aaron", "hello/Reynold");
		assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!","Hello, Reynold!"));
		assertTrue(res.errorMessages.isEmpty());
	}

	@Test
	public void returnErrorRPC() throws Exception {
		RpcResult res = sendRpc("return error/OK");
		assertTrue(res.successMessages.isEmpty());
		assertErrorsContain(res.errorMessages, Sets.newHashSet("Returned: OK"));
	}

	@Test
	public void throwErrorRPC() throws Exception {
		RpcResult res = sendRpc("throw error/uh-oh");
		assertTrue(res.successMessages.isEmpty());
		assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
	}

	@Test
	public void doubleTrouble() throws Exception {
		RpcResult res = sendRpc("hello/Aaron", "throw error/OK");
		assertTrue(res.successMessages.isEmpty());
		assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh","Thrown: OK"));
	}

	@Test
	public void sendSuccessAndFailure() throws Exception {
		RpcResult res = sendRpc("throw error/uh-oh", "hello/Aaron");
		assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
		assertErrorsContain(res.errorMessages, Sets.newHashSet("Thrown: uh-oh"));
	}

	private void assertErrorsContain(Set<String> errors, Set<String> contains) {
		assertEquals(errors.size(), contains.size());

		Set<String>  remainingErrors = Sets.newHashSet(errors);
		for (String contain : contains) {
			Iterator<String> iter = remainingErrors.iterator();
			boolean  foundMatch = false;
			while (iter.hasNext()) {
				if (iter.next().contains(contain)) {
					iter.remove();
					foundMatch = true;
					break;
				}
			}
			assertTrue("Could not find error containing " + contain + "; errors: " + errors, foundMatch);
		}
		assertTrue(remainingErrors.isEmpty());
	}
}
