package govind.incubator.shuffle;

import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.conf.SystemPropertyConfigProvider;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.shuffle.RetryingBlockFetcher.BlockFetcherStarter;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import jersey.repackaged.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-6
 *
 * 测试重试逻辑能够在遇到IOExxception时重新获取丢失的blocks
 *
 */
public class RetryingBlockFetcherSuite {

	ManagedBuffer block0 = new NioManagedBuffer(ByteBuffer.wrap(new byte[13]));
	ManagedBuffer block1 = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
	ManagedBuffer block2 = new NioManagedBuffer(ByteBuffer.wrap(new byte[145]));


	@Before
	public void beforeEach() {
		System.setProperty("govind.network.shuffle.io.maxRetries", "3");
		System.setProperty("govind.network.shuffle.io.retryWait", "1");
	}

	@After
	public void afterEach() {
		System.clearProperty("govind.network.shuffle.io.maxRetries");
		System.clearProperty("govind.network.shuffle.io.retryWait");
	}

	@Test
	public void testNoFailure() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions, listener);

		verify(listener).onBlockFetchSuccess("b0", block0);
		verify(listener).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}

	@Test
	public void testUnrecoverableFailure() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", new RuntimeException("Wrong"))
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions,  listener);

		verify(listener).onBlockFetchFailure(eq("b0"), any());
		verify(listener).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}

	@Test
	public void testSingleIOExceptionOnFirst() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", new IOException("连接超时"))
						.put("b1", block1)
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions,  listener);

		verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
		verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}


	@Test
	public void testSingleIOExceptionOnSecond() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions,  listener);

		verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
		verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}

	@Test
	public void testTwoIOException() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", new IOException("连接超时"))
						.put("b1", block1)
						//第一获取失败时，会重新获取，剩余的其他block同样会重新尝试获取
						//.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions,  listener);

		verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
		verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}

	@Test
	public void testThreeIOException() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", new IOException("连接超时"))
						.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b1", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b1", block1)
						.build()
		);

		performInteraction(interactions,  listener);

		verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
		verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
		verifyNoMoreInteractions(listener);
	}

	@Test
	public void testRetryAndUnrecoverable() throws IOException {
		BlockFetchingListener listener = mock(BlockFetchingListener.class);

		List<? extends Map<String, Object>> interactions = Arrays.asList(
				ImmutableMap.<String, Object>builder()
						.put("b0", new IOException("连接超时"))
						.put("b1", new RuntimeException("SomeError"))
						.put("b2", block2)
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b0", block0)
						.put("b1", new RuntimeException("SomeError"))
						.put("b2", new IOException("连接超时"))
						.build(),
				ImmutableMap.<String, Object>builder()
						.put("b2", block2)
						.build()
		);

		performInteraction(interactions, listener);

		verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
		verify(listener, timeout(5000)).onBlockFetchFailure(eq("b1"), any());
		verify(listener, timeout(5000)).onBlockFetchSuccess("b2", block2);
		verifyNoMoreInteractions(listener);
	}


	/**
	 * 针对每个block请求，根据提供的{@code interactions}决定是返回ManagedBuffer
	 * 还是IOException。
	 *
	 * 每个interaction中支持包含多个请求，是<blockId, ManagedBuffer/IOException>的集合
	 *
	 */
	@SuppressWarnings("unchecked")
	private static void performInteraction(
			List<? extends Map<String, Object>> interactions,
			BlockFetchingListener listener) throws IOException {

		TransportConf conf = new TransportConf(new SystemPropertyConfigProvider(), "shuffle");
		BlockFetcherStarter fetcherStarter = mock(BlockFetcherStarter.class);

		Stubber stubber = null;

		final LinkedHashSet<String> blockIds = Sets.newLinkedHashSet();

		for (Map<String, Object> interaction : interactions) {
			blockIds.addAll(interaction.keySet());

			Answer<Void> answer = (Answer<Void>) invocation -> {
				try {
					String[] requestBlockIds = (String[]) invocation.getArguments()[0];
					String[] desiredBlockIds = interaction.keySet().toArray(new String[interaction.size()]);
					assertArrayEquals(desiredBlockIds, requestBlockIds);

					BlockFetchingListener retryListener = (BlockFetchingListener) invocation.getArguments()[1];
					interaction.entrySet().forEach(entry -> {
						String blockId = entry.getKey();
						Object blockValue = entry.getValue();

						if (blockValue instanceof ManagedBuffer) {
							retryListener.onBlockFetchSuccess(blockId, (ManagedBuffer) blockValue);
						} else if (blockValue instanceof Exception) {
							retryListener.onBlockFetchFailure(blockId, (Throwable) blockValue);
						} else {
							fail("只能处理ManagedBuffer、IOException类型");
						}
					});
					return null;
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			};

			if (stubber == null) {
				stubber = doAnswer(answer);
			} else {
				stubber.doAnswer(answer);
			}
		}

		assert stubber != null;
		stubber.when(fetcherStarter).createAndStart(any(), any());
		String[] blockIdArray = blockIds.toArray(new String[blockIds.size()]);
		new RetryingBlockFetcher(conf, fetcherStarter, blockIdArray, listener).start();
	}
}
