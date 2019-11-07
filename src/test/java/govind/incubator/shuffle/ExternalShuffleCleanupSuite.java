package govind.incubator.shuffle;

import com.google.common.util.concurrent.MoreExecutors;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-5
 */
public class ExternalShuffleCleanupSuite {
	//确保清理操作在测试线程中同步进行
	Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
	TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");

	@Test
	public void noCleanupAndCleanup() throws IOException {
		TestShuffleDataContext dataContext = createSomeData();

		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(sameThreadExecutor, conf, null);

		blockResolver.registerExecutor("app", "exec0", dataContext.createExecutorInfo("shuffleMgr"));

		blockResolver.applicationRemoved("app", false);
		assertStillThere(dataContext);

		blockResolver.registerExecutor("app1", "exec0", dataContext.createExecutorInfo("shuffleMgr"));
		blockResolver.applicationRemoved("app1", true);
		assertCleanup(dataContext);
	}

	@Test
	public void cleanupUseExecutor() throws IOException {
		TestShuffleDataContext dataContext = createSomeData();

		//确认是否使用了自定义的Executor
		final AtomicBoolean cleanupEnabled = new AtomicBoolean(false);
		Executor noThreadExecutor = new Executor() {
			@Override
			public void execute(Runnable command) {
				cleanupEnabled.set(true);
			}
		};

		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(noThreadExecutor, conf, null);
		blockResolver.registerExecutor("app","exec1",dataContext.createExecutorInfo("shuffleMgr"));
		blockResolver.applicationRemoved("app", true);

		assertTrue(cleanupEnabled.get());
		assertStillThere(dataContext);

		dataContext.cleanup();
		assertCleanup(dataContext);

	}

	@Test
	public void cleanupMultipleExecutors() throws IOException {
		TestShuffleDataContext dataContext0 = createSomeData();
		TestShuffleDataContext dataContext1 = createSomeData();

		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(sameThreadExecutor, conf, null);
		blockResolver.registerExecutor("app1","exec1", dataContext0.createExecutorInfo("shufflemgr"));
		blockResolver.registerExecutor("app1", "exec2", dataContext1.createExecutorInfo("shufflemgr"));
		blockResolver.applicationRemoved("app1", true);

		assertCleanup(dataContext0);
		assertCleanup(dataContext1);

	}

	@Test
	public void cleanOnlyRemovedApp() throws IOException {
		TestShuffleDataContext someData0 = createSomeData();
		TestShuffleDataContext someData1 = createSomeData();

		ExternalShuffleBlockResolver blockResolver = new ExternalShuffleBlockResolver(sameThreadExecutor, conf, null);

		blockResolver.registerExecutor("app","exec", someData0.createExecutorInfo("shuffleMgr"));
		blockResolver.registerExecutor("app1", "exec1", someData1.createExecutorInfo("shuffleMgr"));

		blockResolver.applicationRemoved("app-nonexist", true);
		assertStillThere(someData0);
		assertStillThere(someData1);

		blockResolver.applicationRemoved("app", true);
		assertCleanup(someData0);
		assertStillThere(someData1);

		blockResolver.applicationRemoved("app1", true);
		assertCleanup(someData0);
		assertCleanup(someData1);

		//确保删除不存在的文件时不会报错
		blockResolver.applicationRemoved("app1", true);
		assertCleanup(someData0);
		assertCleanup(someData1);
	}

	private void assertStillThere(TestShuffleDataContext dataContext) {
		for (String dir : dataContext.localDirs) {
			assertTrue(dir + "被删除了", new File(dir).exists());
		}
	}

	private void assertCleanup(TestShuffleDataContext dataContext) {
		for (String localDir : dataContext.localDirs) {
			assertFalse(localDir + "未被删除", new File(localDir).exists());
		}
	}

	private TestShuffleDataContext createSomeData() throws IOException {
		Random random = new Random(123);
		TestShuffleDataContext dataContext = new TestShuffleDataContext(10, 5);

		dataContext.create();
		dataContext.insertHashBasedShuffleData(random.nextInt(1000), random.nextInt(1000) + 1000, new byte[][]{
				"GHI".getBytes(), "JKLMNOPQRSTUVWXYZ".getBytes()
		});

		dataContext.insertSortBasedShuffleData(random.nextInt(1000), random.nextInt(1000), new  byte[][]{
				"ABC".getBytes(), "DEF".getBytes()
		});
		return dataContext;
	}
}
