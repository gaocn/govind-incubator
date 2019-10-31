package govind.incubator.network;

import com.google.common.io.Files;
import govind.incubator.network.buffer.FileSegmentManagedBuffer;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamCallback;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import io.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-9
 */
public class StreamSuite {
	private static final String[] STREAMS = {"largeBuffer","smallBuffer","emptyBuffer","file"};

	private static TransportServer server;
	private static TransportClientFactory clientFactory;
	private static File testFile;
	private static File tempDir;

	private static ByteBuffer emptyBuffer;
	private static ByteBuffer smallBuffer;
	private static ByteBuffer largeBuffer;

	private static ByteBuffer createBuffer(int bufSize) {
		ByteBuffer buffer = ByteBuffer.allocate(bufSize);
		for (int i = 0; i < bufSize; i++) {
			buffer.put((byte)i);
		}
		buffer.flip();
		return buffer;
	}

	@BeforeClass
	public static void setup() throws IOException {
		tempDir = Files.createTempDir();
		emptyBuffer = createBuffer(0);
		smallBuffer = createBuffer(100);
		largeBuffer = createBuffer(100000);

		testFile = File.createTempFile("stream-test-file","txt", tempDir);

		FileOutputStream fos = new FileOutputStream(testFile);
		try {
			Random rand = new Random();
			for (int i = 0; i < 512; i++) {
				byte[] fileContent = new byte[1024];
				rand.nextBytes(fileContent);
				fos.write(fileContent);
			}
		} finally {
			fos.close();
		}


		TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		StreamManager manager = new StreamManager() {
			@Override
			public ManagedBuffer getChunk(long streamId, int chunkIdx) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void connectionTerminated(Channel channel) {
			}

			@Override
			public ManagedBuffer openStream(String streamId) {
				switch (streamId) {
					case "largeBuffer":
						return new NioManagedBuffer(largeBuffer);
					case "smallBuffer":
						return new NioManagedBuffer(smallBuffer);
					case "emptyBuffer":
						return new NioManagedBuffer(emptyBuffer);
					case "file":
						return new FileSegmentManagedBuffer(testFile, 0, testFile.length());
					default:
						throw new IllegalArgumentException("Invalid Stream: " + streamId);
				}
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
		server = context.createServer();
		clientFactory = context.createClientFactory();
	}

	@AfterClass
	public static void teatDown() throws IOException {
		server.close();
		clientFactory.close();
		if (tempDir != null) {
			for (File file : tempDir.listFiles()) {
				file.delete();
			}
			tempDir.delete();
		}
	}

	@Test
	public void testZeroLengthStream() throws Throwable {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());

		try {
			StreamTask task = new StreamTask(client, "emptyBuffer", TimeUnit.SECONDS.toMillis(5));

			task.run();
			task.check();
		} finally {
			client.close();
		}
	}

	@Test
	public void testSmallLengthStream() throws Throwable {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());
		try {
			StreamTask task = new StreamTask(client, "smallBuffer", TimeUnit.SECONDS.toMillis(20));
			task.run();
			task.check();
		} finally {
			client.close();
		}
	}

	@Test
	public void testLargeLengthStream() throws Throwable {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());

		try {
			StreamTask task = new StreamTask(client, "largeBuffer", TimeUnit.SECONDS.toMillis(5));
			task.run();
			task.check();
		} finally {
			client.close();
		}
	}

	@Test
	public void testFileStream() throws Throwable {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());
		try {
			StreamTask task = new StreamTask(client, "file", TimeUnit.SECONDS.toMillis(5));
			task.run();
			task.check();
		} finally {
			client.close();
		}
	}

	@Test
	public void testMultipleStream() throws Throwable {
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());

		try {
			for (int i = 0; i < 20; i++) {
				String streamType = STREAMS[i % STREAMS.length];
				StreamTask task = new StreamTask(client, streamType, TimeUnit.SECONDS.toMillis(5));
				task.run();
				task.check();
			}
		} finally {
			client.close();
		}
	}

	@Test
	public void testConcurrentStreams() throws Throwable {
		ExecutorService executor = Executors.newFixedThreadPool(20);
		TransportClient client = clientFactory.createClient(NettyUtil.getLocalHost(), server.getPort());

		try {
			ArrayList<StreamTask> tasks = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				StreamTask task = new StreamTask(client, STREAMS[i % STREAMS.length], TimeUnit.SECONDS.toMillis(5));
				tasks.add(task);
				executor.submit(task);
			}
			executor.shutdown();

			assertTrue("等待超时，线程池任务未执行完毕", executor.awaitTermination(30, TimeUnit.SECONDS));
			for (StreamTask task : tasks) {
				task.check();
			}
		}  finally {
			executor.shutdownNow();
			client.close();
		}
	}

	private static class StreamTask implements Runnable {

		private final TransportClient client;
		private final String streamId;
		private final long timeoutMS;
		private Throwable cause;

		public StreamTask(TransportClient client, String streamId, long timeoutMS) {
			this.client = client;
			this.streamId = streamId;
			this.timeoutMS = timeoutMS;
		}

		@Override
		public void run() {
			ByteBuffer srcBuffer = null;
			OutputStream out = null;
			File outFile = null;

			try {
				ByteArrayOutputStream baos = null;

				switch (streamId) {
					case "largeBuffer":
						baos = new ByteArrayOutputStream();
						out = baos;
						srcBuffer = largeBuffer;
						break;
					case "smallBuffer":
						baos = new ByteArrayOutputStream();
						out = baos;
						srcBuffer = smallBuffer;
						break;
					case "emptyBuffer":
						baos = new ByteArrayOutputStream();
						out = baos;
						srcBuffer = emptyBuffer;
						break;
					case "file":
						outFile = File.createTempFile("data",".tmp", tempDir);
						out = new FileOutputStream(outFile);
						break;
					default:
						throw new IllegalArgumentException("非法streamId：" + streamId);
				}

				TestCallback callback = new TestCallback(out);
				client.stream(streamId, callback);
				waitForCompletion(callback);

				if (srcBuffer == null) {
					assertTrue("File Stream did not match", Files.equal(testFile, outFile));
				} else {
					ByteBuffer base;
					synchronized (srcBuffer) {
						base = srcBuffer.duplicate();
					}
					byte[] result = baos.toByteArray();
					byte[] expected = new byte[base.remaining()];
					base.get(expected);
					assertEquals(expected.length, result.length);
					assertTrue("buffer don't match", Arrays.equals(expected, result));
				}
			} catch (Exception e) {
				cause = e;
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (outFile != null) {
					outFile.delete();
				}
			}
		}

		private void check() throws Throwable {
			if (cause != null) {
				throw cause;
			}
		}

		private void waitForCompletion(TestCallback callback) throws Exception {
			long now = System.currentTimeMillis();
			long deadline = now + timeoutMS;

			synchronized (callback) {
				while (!callback.completed && now < deadline) {
					callback.wait(deadline - now);
					now = System.currentTimeMillis();
				}
			}

			assertTrue("等待Stream响应超时", callback.completed);
			assertNull(callback.cause);
		}
	}

	private static class TestCallback implements StreamCallback {

		private final OutputStream out;
		private volatile boolean  completed;
		private volatile  Throwable cause;

		public TestCallback(OutputStream out) {
			this.out = out;
		}

		@Override
		public void onData(String streamId, ByteBuffer buffer) throws IOException {
			byte[] bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
			out.write(bytes);
		}

		@Override
		public void onComplete(String streamId) throws IOException {
			out.close();
			synchronized (this) {
				completed =  true;
				notifyAll();
			}
		}

		@Override
		public void onFailure(String streamId, Throwable cause) throws IOException {
			this.cause = cause;
			synchronized (this) {
				completed = true;
				notifyAll();
			}
		}
	}
}
