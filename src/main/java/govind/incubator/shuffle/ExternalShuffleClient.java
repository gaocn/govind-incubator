package govind.incubator.shuffle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.client.TransportClientBootstrap;
import govind.incubator.network.client.TransportClientFactory;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.NoOpRpcHandler;
import govind.incubator.network.sasl.SaslClientBootstrap;
import govind.incubator.network.sasl.SecretKeyHolder;
import govind.incubator.network.util.TransportContext;
import govind.incubator.shuffle.RetryingBlockFetcher.BlockFetcherStarter;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;
import govind.incubator.shuffle.protocol.RegisterExecutor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 *
 * 从外部服务端获取shuffle block数据的客户端，而不是从其他Executor中读取
 * shuffle blocks（BlockTransferService，缺点是若丢失Executors时会丢失数据）。
 *
 */
@Slf4j
public class ExternalShuffleClient extends ShuffleClient {

	private final TransportConf conf;
	private final boolean saslEnabled;
	private final boolean saslEncryptionEnabled;
	private final SecretKeyHolder secretKeyHolder;

	protected TransportClientFactory clientFactory;
	protected String appId;

	public ExternalShuffleClient(TransportConf conf, SecretKeyHolder secretKeyHolder, boolean saslEnabled, boolean saslEncryptionEnabled) {
		Preconditions.checkArgument(
				!saslEncryptionEnabled || saslEnabled,
				"加密只有在saslEnabled认证开启时才能启动");
		this.conf = conf;
		this.saslEnabled = saslEnabled;
		this.saslEncryptionEnabled = saslEncryptionEnabled;
		this.secretKeyHolder = secretKeyHolder;
	}

	@Override
	public void init(String appId) {
		this.appId = appId;
		TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true);
		ArrayList<TransportClientBootstrap> bootstraps = Lists.newArrayList();
		if (saslEnabled) {
			bootstraps.add(new SaslClientBootstrap(saslEncryptionEnabled, appId, conf, secretKeyHolder));
		}
		clientFactory = context.createClientFactory(bootstraps);
	}

	/**
	 * 从指定远端获取shuffle blocks
	 */
	@Override
	public void fetchBlocks(String host, int port, String execId, String[] blockIds, BlockFetchingListener listener) {
		checkInit();
		log.debug("准备从{}:{}(executor id={})获取shuffle数据", host, port, execId);

		try {
			BlockFetcherStarter fetcherStarter = new BlockFetcherStarter() {
				@Override
				public void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException {
					TransportClient client = clientFactory.createClient(host, port);
					new  OneForOneBlockFetcher(client, appId, execId, blockIds, listener)
					.start();
				}
			};

			int maxRetries = conf.maxIORetries();
			if (maxRetries > 0) {
				new RetryingBlockFetcher(conf, fetcherStarter, blockIds,  listener).start();
			} else {
				fetcherStarter.createAndStart(blockIds, listener);
			}
		} catch (Exception e) {
			log.error("获取blocks时失败", e);
			for (String blockId : blockIds) {
				listener.onBlockFetchFailure(blockId, e);
			}
		}
	}

	private void checkInit() {
		assert appId != null : "应该在init方法之后调用";
	}

	/**
	 * 将当前Executor元数据信息注册给指定的Shuffle Server，元数据包括当前
	 * Executor将shuffle文件存放在哪里以及以何种方式存储。
	 *
	 * @param host host of shuffle server
	 * @param port port of shuffle server
	 * @param execId this executor's id
	 * @param shuffleInfo 包含所有能够定位shuffle文件的元数据
	 */
	public void registerWithShuffleServer(String host, int port, String execId, ExecutorShuffleInfo shuffleInfo) throws IOException {
		checkInit();
		TransportClient client = clientFactory.createClient(host, port);

		try {
			RegisterExecutor msg = new RegisterExecutor(appId, execId, shuffleInfo);
			client.sendRpcSync(msg.toByteBuffer(), 5000);
		} finally {
			client.close();
		}
	}

	@Override
	public void close() throws IOException {
		Closeables.closeQuietly(clientFactory);
	}
}
