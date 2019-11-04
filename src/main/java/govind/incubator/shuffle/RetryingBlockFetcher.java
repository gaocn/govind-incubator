package govind.incubator.shuffle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.conf.TransportConf;
import jersey.repackaged.com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 当因网络原因从远程获取block失败时，可以自动重试获取。
 *
 */
@Slf4j
public class RetryingBlockFetcher {
	/** 在等待或尝试重试是使用 */
	private static final ExecutorService executorService = Executors.newCachedThreadPool(
			new ThreadFactoryBuilder().setNameFormat("Block Fetch Retry").build()
	);

	/** 尝试重试次数 */
	private final int maxRetries;

	/** 每次重试等待时间，单位ms */
	private final int retryWaitTime;

	/** 已经重试次数 注意：所有non-final字段在访问和修改是需要加锁！ */
	private int retryCount;

	/** 尚未成功获取的blockIds，获取block的顺序按照用户请求的顺序进行重试 */
	private final LinkedHashSet<String> outstandingBlockIds;

	/** 父监听器，当某个block最终获取失败或成功时被调用 */
	private final BlockFetchingListener listener;

	/** 用于初始化BlockFetcher，以便进行重试尝试获取为成功获取的blocks */
	private final BlockFetcherStarter fetchStarter;

	/** 当前活跃的监听器，每次重试获取时都会新建一个监听器替代当前的监听器，
	 * 表示忽略前一个监听器获取的结果 */
	private RetryingBlockFetchListener currentListener;


	public RetryingBlockFetcher(
			TransportConf conf,
			BlockFetcherStarter fetchStarter,
			String[] blockIds,
			BlockFetchingListener listener) {
		this.listener = listener;
		this.fetchStarter = fetchStarter;
		this.maxRetries = conf.maxIORetries();
		this.retryWaitTime = conf.ioRetryWaitTimeMS();
		this.currentListener = new RetryingBlockFetchListener();
		this.outstandingBlockIds = Sets.newLinkedHashSet();
		this.outstandingBlockIds.addAll(Arrays.asList(blockIds));
	}

	/**
	 * 首次尝试获取blocks，并根据获取结果会尝试重试获取尚未成功获得blocks。
	 */
	public void start() {
		fetchAllOutstanding();
	}

	/**
	 * 尝试获取所有尚未被获取到的blocks
	 */
	private void fetchAllOutstanding() {
		String[] blockIdsToFetch;
		int numReties;
		RetryingBlockFetchListener myListener;

		synchronized (this) {
			blockIdsToFetch  = outstandingBlockIds.toArray(new String[outstandingBlockIds.size()]);
			numReties = retryCount;
			myListener = currentListener;
		}

		try {
			fetchStarter.createAndStart(blockIdsToFetch, myListener);
		} catch (Exception e) {
			log.error("{}尝试获取{}blocks失败：{}",
					numReties>0?"在重试" + numReties + "后":""
					,outstandingBlockIds.size(), e.getMessage());

			if (shouldRetry(e)) {
				initiateRetry();
			} else {
				for (String blockId : blockIdsToFetch) {
					listener.onBlockFetchFailure(blockId, e);
				}
			}
		}
	}

	/**
	 * 通过重启一个线程开始进行重试，重试操作会在等待一段时间开始运行。
	 */
	private synchronized void initiateRetry() {
		retryCount++;
		currentListener = new RetryingBlockFetchListener();

		log.info("在{}ms后，开始尝试重试({}/{}){}个未成功获取的blocks", retryWaitTime, retryCount, maxRetries, outstandingBlockIds.size());

		executorService.submit(() -> {
			Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
			fetchAllOutstanding();
		});
	}

	/**
	 * 当遇到IOException并且重试次数未超过最大次数时才会重试。
	 */
	private synchronized boolean shouldRetry(Throwable cause) {
		boolean isIOException = cause instanceof IOException
				|| (cause.getCause() != null && cause.getCause() instanceof IOException);
		boolean hasRemainingRetries = retryCount < maxRetries;
		return isIOException && hasRemainingRetries;
	}

	/** 用于第一次获取所有blocks，以及重试获取剩余未成功获取的blocks */
	public interface BlockFetcherStarter {
		/**
		 * 创建一个BlockFetcher实例并尝试获取给定blockIds的blocks，获取过程
		 * 是异步进行的，BlockFetcher最终会调用监听器反馈结果，每个blockId会
		 * 调用一次。
		 *
		 * 为了避免使用同一个连接产生的问题，该方法每次都会从TransportClientFactory
		 * 中尝试获取一个新的TransportClient实例。
		 *
		 */
		void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException;
	}

	/**
	 * 重试监听器根据获取block结果决定是否重试，并同时父监听器获取结果。
	 *
	 * 注意：在重试期间，会立即将{@code currentListener}成员变量替换，表示任
	 * 何来自非当前{@code currentListener}的响应都会被忽略。
	 *
	 */
	public class RetryingBlockFetchListener implements BlockFetchingListener {

		@Override
		public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
			//若当前block请求是重试后获取成功的，且当前监听器为活跃的监听器，
			// 则将成功获取block的结果报告给父监听器。
			boolean shouldForwardSuccess = false;
			synchronized (RetryingBlockFetcher.this) {
				if (this == currentListener && outstandingBlockIds.contains(blockId)) {
					outstandingBlockIds.remove(blockId);
					shouldForwardSuccess = true;
				}
			}

			if (shouldForwardSuccess) {
				listener.onBlockFetchSuccess(blockId, data);
			}
		}

		@Override
		public void onBlockFetchFailure(String blockId, Throwable cause) {
			//若当前block请求是重试后获取失败的，且当前监听器为活跃的监听器，
			// 同时没法继续重试时，则将成功获取block的结果报告给父监听器。
			boolean shouldForwardFailure = false;
			synchronized (RetryingBlockFetcher.this) {
				if (currentListener == this && outstandingBlockIds.contains(blockId)) {
					if (shouldRetry(cause)) {
						initiateRetry();
					} else {
						log.error("尝试获取block【{}】失败，超过重试次数【{}】，停止重试。失败原因：{}。", blockId, retryCount,cause.getMessage());
						outstandingBlockIds.remove(blockId);
						shouldForwardFailure = true;
					}
				}
			}

			if (shouldForwardFailure) {
				listener.onBlockFetchFailure(blockId, cause);
			}
		}
	}
}
