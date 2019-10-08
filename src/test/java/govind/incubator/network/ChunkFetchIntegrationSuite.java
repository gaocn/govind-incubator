package govind.incubator.network;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import govind.incubator.network.buffer.FileSegmentManagedBuffer;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.TransportContext;
import io.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-8
 */
public class ChunkFetchIntegrationSuite {
	static final long STREAM_ID = 1;
	static final int BUFFER_CHUNK_INDEX = 0;
	static final int FILE_CHUNK_INDEX = 1;

	static TransportServer server;
	static TransportClientFactory clientFactory;
	static StreamManager streamManager;
	static File testFile;

	static ManagedBuffer bufferChunk;
	static ManagedBuffer fileChunk;

	private TransportConf transportConf;

	@BeforeClass
	public static void setup() throws IOException {
		int bufSize = 100000;
		final ByteBuffer buf = ByteBuffer.allocate(bufSize);
		for (int i = 0; i < bufSize; i++) {
			buf.put((byte) i);
		}
		buf.flip();
		bufferChunk = new NioManagedBuffer(buf);

		testFile = File.createTempFile("shuffle-test-file", "txt");
		testFile.deleteOnExit();

		RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
		boolean shouldSuppressIOException = true;

		try {
			byte[] fileContent = new byte[1024];
			new Random().nextBytes(fileContent);
			raf.write(fileContent);
			shouldSuppressIOException = false;
		} finally {
			Closeables.close(raf, shouldSuppressIOException);
		}

		final TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		fileChunk = new FileSegmentManagedBuffer(testFile, 10, testFile.length() - 25);

		streamManager = new StreamManager() {
			@Override
			public ManagedBuffer getChunk(long streamId, int chunkIdx) {
				assertEquals(STREAM_ID, streamId);
				if (chunkIdx == BUFFER_CHUNK_INDEX) {
					return new NioManagedBuffer(buf);
				} else  if (chunkIdx == FILE_CHUNK_INDEX) {
					return new FileSegmentManagedBuffer(testFile, 10, testFile.length() -  25);
				} else {
					throw new IllegalArgumentException("不合法的chunk index: " + chunkIdx);
				}
			}

			@Override
			public void connectionTerminated(Channel channel) {
			}
		};

		RpcHandler rpcHandler = new RpcHandler() {
			@Override
			public StreamManager getStreamManager() {
				return streamManager;
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
	public static void tearDown() throws IOException {
		bufferChunk.release();
		server.close();
		clientFactory.close();
		testFile.delete();
	}

	class FetchResult {
		public Set<Integer> successChunks;
		public Set<Integer> failedChunks;
		public List<ManagedBuffer> buffers;

		public void releaseBuffers() {
			for (ManagedBuffer buffer : buffers) {
				buffer.release();
			}
		}
	}

	private FetchResult fetchChunks(List<Integer> chunkIndices) throws IOException, InterruptedException {
		TransportClient client = clientFactory.createClient(getLocalHost(), server.getPort());
		Semaphore semaphore = new Semaphore(0);

		FetchResult fetchResult = new FetchResult();
		fetchResult.successChunks = Collections.synchronizedSet(new HashSet<>());
		fetchResult.failedChunks = Collections.synchronizedSet(new HashSet<>());
		fetchResult.buffers = Collections.synchronizedList(new LinkedList<>());

		ChunkReceivedCallback callback = new ChunkReceivedCallback() {
			@Override
			public void onSuccess(int chunkIdx, ManagedBuffer buffer) {
				buffer.retain();
				fetchResult.successChunks.add(chunkIdx);
				fetchResult.buffers.add(buffer);
				semaphore.release();
			}

			@Override
			public void onFailure(int chunkIdx, Throwable cause) {
				fetchResult.failedChunks.add(chunkIdx);
				semaphore.release();
			}
		};

		for (Integer chunkIndex : chunkIndices) {
			client.fetchChunk(STREAM_ID, chunkIndex, callback);
		}

		if (!semaphore.tryAcquire(chunkIndices.size(), 5, TimeUnit.SECONDS)) {
			fail("Timeout getting response from the server");
		}

		client.close();
		return fetchResult;
	}

	@Test
	public void testFetchBufferChunk() throws IOException, InterruptedException {
		FetchResult res = fetchChunks(Lists.newArrayList(BUFFER_CHUNK_INDEX));
		assertEquals(res.successChunks, Sets.newHashSet(BUFFER_CHUNK_INDEX));
		assertTrue(res.failedChunks.isEmpty());
		assertBufferListsEquals(res.buffers, Lists.newArrayList(bufferChunk));
	}











	private void assertBufferListsEquals(List<ManagedBuffer> list0, List<ManagedBuffer> list1) throws IOException {
		assertEquals(list0.size(),  list1.size());
		for (int i = 0; i < list0.size(); i++) {
			assertBufferEquals(list0.get(i),  list1.get(i));
		}
	}

	private void assertBufferEquals(ManagedBuffer buffer0, ManagedBuffer buffer1) throws IOException {
		ByteBuffer nio0 = buffer0.nioByteBuffer();
		ByteBuffer nio1 = buffer1.nioByteBuffer();

		assertEquals(nio0.remaining(), nio1.remaining());
		for (int i = 0; i < nio0.remaining(); i++) {
			assertEquals(nio0.get(), nio1.get());
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
