package govind.incubator.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.shuffle.ExternalShuffleBlockResolver.AppExecId;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-5
 */
public class ExternalShuffleBlockResolverSuite {
	static final String sortBlock0 = "hello!";
	static final String sortBlock1 = "world!";
	static final String hashBlock0 = "Elementary";
	static final String hashBlock1 = "Taular";

	static TestShuffleDataContext dataContext;
	static TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shufle");

	@BeforeClass
	public static void beforeAll() throws IOException {
		dataContext = new TestShuffleDataContext(3, 5);
		dataContext.create();

		dataContext.insertHashBasedShuffleData(0, 0, new byte[][]{
				hashBlock0.getBytes(), hashBlock1.getBytes()
		});

		dataContext.insertSortBasedShuffleData(1, 0, new byte[][]{
				sortBlock0.getBytes(), sortBlock1.getBytes()
		});
	}

	@AfterClass
	public static void afterAll() {
		dataContext.cleanup();
	}

	@Test
	public void testHashShuffleBlock() throws IOException {
		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(conf, null);

		blockResolver.registerExecutor("app0", "exec0", dataContext.createExecutorInfo("org.apache.spark.shuffle.hash.HashShuffleManager"));

		InputStream block0Stream = blockResolver
				.getBlockData("app0", "exec0", "shuffle_0_0_0")
				.createInputStream();

		String block0 = CharStreams.toString(new InputStreamReader(block0Stream));
		assertEquals(hashBlock0, block0);

		InputStream block1Stream = blockResolver
				.getBlockData("app0", "exec0", "shuffle_0_0_1")
				.createInputStream();
		String block1 = CharStreams.toString(new InputStreamReader(block1Stream));
		assertEquals(hashBlock1, block1);
	}

	@Test
	public void testSortShuffleBlock() throws IOException {
		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(conf, null);
		blockResolver.registerExecutor("app0", "exec0", dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));

		InputStream block0Stream = blockResolver
				.getBlockData("app0", "exec0", "shuffle_1_0_0")
				.createInputStream();
		String block0 = CharStreams.toString(new InputStreamReader(block0Stream));

		InputStream block1Stream = blockResolver
				.getBlockData("app0", "exec0", "shuffle_1_0_1")
				.createInputStream();
		String block1 = CharStreams.toString(new InputStreamReader(block1Stream));

		assertEquals(sortBlock0, block0);
		assertEquals(sortBlock1, block1);
	}

	@Test
	public void testJsonSerializationOfExecutorRegistration() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		AppExecId appExecId = new AppExecId("app-1", "exec-2");
		String appExecIdJson = mapper.writeValueAsString(appExecId);

		AppExecId appExecId1 = mapper.readValue(appExecIdJson, AppExecId.class);
		assertEquals(appExecId, appExecId1);

		ExecutorShuffleInfo shuffleInfo = new ExecutorShuffleInfo(
				new String[]{"/bippy","/flippy"}, 7, "hash");

		String shuffleInfoJson = mapper.writeValueAsString(shuffleInfo);
		ExecutorShuffleInfo shuffleInfo1 = mapper.readValue(shuffleInfoJson, ExecutorShuffleInfo.class);
		assertEquals(shuffleInfo, shuffleInfo1);

		String legacyAppIdJson = "{\"appId\":\"app-1\", \"execId\":\"exec-2\"}";
		assertEquals(appExecId, mapper.readValue(legacyAppIdJson, AppExecId.class));
		String legacyShuffleJson = "{\"localDirs\": [\"/bippy\", \"/flippy\"], " +
				"\"subDirsPerLocalDir\": 7, \"shuffleManager\": \"hash\"}";
		assertEquals(shuffleInfo, mapper.readValue(legacyShuffleJson, ExecutorShuffleInfo.class));
	}

	@Test
	public void testBadRequests() throws IOException {
		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(conf, null);

		//unregister executor
		try {
			blockResolver.getBlockData("app0", "exec1","shuffle_1_1_0");
			fail("应该会抛出异常");
		} catch (RuntimeException e) {
			assertTrue("bad error message: " + e, e.getMessage().contains("没有找到Executor元数据信息"));
		}

		//Invalid shuffle manager
		blockResolver.registerExecutor("app0", "exec1", dataContext.createExecutorInfo("InvalidManager"));
		try {
			blockResolver.getBlockData("app0", "exec1", "shuffle_0_1_0");
			fail("应该会抛出异常");
		} catch (UnsupportedOperationException e) {
			//pass
		}

		//Nonexist Shuffle block
		blockResolver.registerExecutor("app1","exec3",dataContext.createExecutorInfo("org.apache.spark.shuffle.sort.SortShuffleManager"));

		try {
			blockResolver.getBlockData("app1", "exec3", "shuffle_2_5_0");
			fail("应该会抛出异常");
		} catch (Exception e) {
			//pass
		}
	}
}
