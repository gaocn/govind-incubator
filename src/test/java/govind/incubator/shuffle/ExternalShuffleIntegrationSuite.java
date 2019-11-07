package govind.incubator.shuffle;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcraft.jsch.HASH;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.server.TransportServer;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.network.util.TransportContext;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-5
 */
public class ExternalShuffleIntegrationSuite {
	static String APP_ID = "app-id";
	static String SORT_MANAGER = "org.apache.spark.shuffle.sort.SortShuffleManager";
	static String HASH_MANAGER = "org.apache.spark.shuffle.hash.HashShuffleManager";

	//Executor 0 is sort-based
	static TestShuffleDataContext dataContext0;
	//Executor 1 is hash-based
	static TestShuffleDataContext dataContext1;

	static ExternalShuffleBlockHandler handler;
	static TransportServer server;
	static TransportConf conf;

	static byte[][] exec0Blocks = new byte[][] {
			new byte[123],
			new byte[12345],
			new byte[1234567]
	};

	static byte[][] exec1Blocks = new byte[][]{
			new byte[321],
			new byte[54321]
	};

	@BeforeClass
	public static void beforeAll() throws IOException {
		Random rand = new Random();

		for (byte[] block : exec0Blocks) {
			rand.nextBytes(block);
		}

		for (byte[] block : exec1Blocks) {
			rand.nextBytes(block);
		}

		dataContext0 = new TestShuffleDataContext(2, 5);
		dataContext0.create();
		dataContext0.insertSortBasedShuffleData(0, 0, exec0Blocks);

		dataContext1 = new TestShuffleDataContext(6, 2);
		dataContext1.create();
		dataContext1.insertHashBasedShuffleData(1, 0, exec1Blocks);

		conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		handler = new ExternalShuffleBlockHandler(conf, null);

		TransportContext context = new TransportContext(conf, handler);
		server = context.createServer();
	}

	@AfterClass
	public static void afterAll() throws IOException {
		dataContext0.cleanup();
		dataContext1.cleanup();
		server.close();
	}


	@After
	public void afterEach() {
		handler.applicationRemoved(APP_ID, false);
	}

	class FetchResult {
		public Set<String> successBlocks;
		public Set<String> failedBlocks;
		public List<ManagedBuffer> buffers;

		public void releaseBuffers() {
			for (ManagedBuffer buffer : buffers) {
				buffer.release();
			}
		}
	}

	private FetchResult fetchBlocks(String execId, String[] blockIds) throws Exception {
		return fetchBlocks(execId, blockIds,  server.getPort());
	}

	private FetchResult fetchBlocks(String execId, String[] blockIds, int port) throws Exception {
		final FetchResult result = new FetchResult();

		result.successBlocks = Collections.synchronizedSet(new HashSet<>());
		result.failedBlocks = Collections.synchronizedSet(new HashSet<>());
		result.buffers = Collections.synchronizedList(new LinkedList<>());

		final Semaphore requestRemaining = new Semaphore(0);

		ExternalShuffleClient client = new ExternalShuffleClient(conf, null, false, false);

		client.init(APP_ID);
		client.fetchBlocks(NettyUtil.getLocalHost(), port, execId, blockIds, new BlockFetchingListener() {
			@Override
			public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
				synchronized (this) {
					if (!result.successBlocks.contains(blockId)
							&& !result.failedBlocks.contains(blockId)) {
						data.retain();
						result.successBlocks.add(blockId);
						result.buffers.add(data);
						requestRemaining.release();
					}
				}
			}

			@Override
			public void onBlockFetchFailure(String blockId, Throwable cause) {
				synchronized (this) {
					if (!result.successBlocks.contains(blockId)
							&& !result.failedBlocks.contains(blockId)) {
						result.failedBlocks.add(blockId);
						requestRemaining.release();
					}
				}
			}
		});

		if (!requestRemaining.tryAcquire(blockIds.length, 5, TimeUnit.SECONDS)) {
			fail("从服务器获取结果超时");
		}

		client.close();
		return result;
	}

	@Test
	public void testFetchOneSort() throws Exception {
		registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
		FetchResult res = fetchBlocks("exec-0", new String[]{"shuffle_0_0_0"});

		assertEquals(Sets.newHashSet("shuffle_0_0_0"), res.successBlocks);
		assertTrue(res.failedBlocks.isEmpty());
		assertBufferListEquals(res.buffers, Lists.newArrayList(exec0Blocks[0]));
		res.releaseBuffers();
	}

	@Test
	public void testFetchThreeSort() throws Exception {
		registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
		String[] blockIds = {"shuffle_0_0_0", "shuffle_0_0_1", "shuffle_0_0_2"};
		FetchResult res = fetchBlocks("exec-0", blockIds);

		assertEquals(Sets.newHashSet("shuffle_0_0_0", "shuffle_0_0_1", "shuffle_0_0_2"), res.successBlocks);
		assertTrue(res.failedBlocks.isEmpty());

		assertBufferListEquals(res.buffers, Lists.newArrayList(exec0Blocks[0],exec0Blocks[1],exec0Blocks[2]));
		res.releaseBuffers();
	}

	@Test
	public void testFetchHash() throws Exception {
		registerExecutor("exec-1", dataContext1.createExecutorInfo(HASH_MANAGER));
		FetchResult res = fetchBlocks("exec-1", new String[]{"shuffle_1_0_0", "shuffle_1_0_1"});

		assertEquals(Sets.newHashSet("shuffle_1_0_0", "shuffle_1_0_1"), res.successBlocks);
		assertTrue(res.failedBlocks.isEmpty());
		assertBufferListEquals(res.buffers, Lists.newArrayList(exec1Blocks[0], exec1Blocks[1]));
		res.releaseBuffers();
	}

	@Test
	public void  testFetchWrongShuffle() throws Exception {
		registerExecutor("exec-0", dataContext1.createExecutorInfo(SORT_MANAGER/*错误的Shuffle Manager*/));

		FetchResult res = fetchBlocks("exec-0", new String[]{"shuffle_1_0_0", "shuffle_1_0_1"});

		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("shuffle_1_0_0", "shuffle_1_0_1"), res.failedBlocks);
	}

	@Test
	public void testFetchInvalidShuffle() throws Exception {
		registerExecutor("exec-0", dataContext1.createExecutorInfo("unknown sort manager"));
		FetchResult res = fetchBlocks("exec-0", new String[]{"shuffle_1_0_0"});
		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("shuffle_1_0_0"), res.failedBlocks);
	}

	@Test
	public void testFetchWrongBlockId() throws Exception {
		registerExecutor("exec-1", dataContext1.createExecutorInfo(SORT_MANAGER /*wrong shuffle manager*/));

		FetchResult res = fetchBlocks("exec-1", new String[]{"rdd_1_0_0"});

		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("rdd_1_0_0"), res.failedBlocks);
	}

	@Test
	public void testFetchNonexistent() throws Exception {
		registerExecutor("exec0", dataContext0.createExecutorInfo(SORT_MANAGER));

		FetchResult res = fetchBlocks("exec0", new String[]{"shuffle_2_0_0"});
		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("shuffle_2_0_0"), res.failedBlocks);
	}

	@Test
	public void testFetchWrongExecutor() throws Exception {
		registerExecutor("exec1", dataContext0.createExecutorInfo(SORT_MANAGER));

		FetchResult res = fetchBlocks("exec1", new String[]{"shuffle_0_0_0"/*right*/,"shuffle_1_0_0"/*wrong*/ });

		//有一个非法blockId会导致所有获取失败，因为是按照整体获取
		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("shuffle_0_0_0", "shuffle_1_0_0"), res.failedBlocks);
	}

	@Test
	public void testFetchUnregisteredExecutor() throws Exception {
		registerExecutor("exec0", dataContext0.createExecutorInfo(SORT_MANAGER));

		FetchResult res = fetchBlocks("exec2", new String[]{"shuffle_0_0_0"/*right*/, "shuffle_1_0_0"/*wrong*/});

		assertTrue(res.successBlocks.isEmpty());
		assertEquals(Sets.newHashSet("shuffle_0_0_0","shuffle_1_0_0"), res.failedBlocks);
	}

	@Test
	public void testFetchNoRetry() throws Exception {
		System.setProperty("govind.network.shuffle.io.maxRetries", "0");

		try {
			registerExecutor("exec-0", dataContext0.createExecutorInfo(SORT_MANAGER));
			FetchResult res = fetchBlocks("exec-0", new String[]{"shuffle_1_0_0", "shuffle_1_0_1"});

			assertTrue(res.successBlocks.isEmpty());
			assertEquals(Sets.newHashSet("shuffle_1_0_0","shuffle_1_0_1"), res.failedBlocks);
		} finally {
			System.clearProperty("govind.network.shuffle.io.maxRetries");
		}
	}

	private void registerExecutor(String execId, ExecutorShuffleInfo shuffleInfo) throws IOException {
		ExternalShuffleClient client = new ExternalShuffleClient(conf, null, false, false);
		client.init(APP_ID);
		client.registerWithShuffleServer(NettyUtil.getLocalHost(), server.getPort(),execId, shuffleInfo);
	}

	private void assertBufferListEquals(List<ManagedBuffer> list0, List<byte[]> list1) throws IOException {
		assertEquals(list0.size(), list1.size());

		for (int i = 0; i < list0.size(); i++) {
			assertBufferEquals(list0.get(i), new NioManagedBuffer(ByteBuffer.wrap(list1.get(i))));
		}
	}

	private void assertBufferEquals(ManagedBuffer buffer0, ManagedBuffer buffer1) throws IOException {
		ByteBuffer nioBuffer0 = buffer0.nioByteBuffer();
		ByteBuffer nioBuffer1 = buffer1.nioByteBuffer();

		assertEquals(nioBuffer0.remaining(), nioBuffer1.remaining());
		int len = nioBuffer0.remaining();
		for (int i = 0; i < len; i++) {
			assertEquals(nioBuffer0.get(), nioBuffer1.get());
		}
	}

}
