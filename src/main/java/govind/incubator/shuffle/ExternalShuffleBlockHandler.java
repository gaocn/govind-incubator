package govind.incubator.shuffle;

import com.google.common.collect.Lists;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.OneForOneStreamManager;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.handler.StreamManager;
import govind.incubator.shuffle.protocol.BlockTransferMessage;
import govind.incubator.shuffle.protocol.BlockTransferMessage.Decoder;
import govind.incubator.shuffle.protocol.OpenBlock;
import govind.incubator.shuffle.protocol.RegisterExecutor;
import govind.incubator.shuffle.protocol.StreamHandle;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.Op;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 为了在Executor外部进程的server端能够提供shuffle data获取服务而实现的RpcHandler实例。
 *
 * 1、支持Executor注册，及存放Executor文件的元数据信息。
 * 2、支持获取Shuffle数据，shuffle block是按照"one-for-one"策略注册，即Transport-Layer
 * 的chunk与Spark-Leve shuffle block是一一对应。
 *
 */
@Slf4j
public class ExternalShuffleBlockHandler extends RpcHandler implements Closeable {

	final ExternalShuffleBlockResolver blockManger;
	private final OneForOneStreamManager streamManager;

	public ExternalShuffleBlockHandler(TransportConf conf, File registeredExecutorFile) throws IOException {
		this(new ExternalShuffleBlockResolver(conf, registeredExecutorFile),
				new OneForOneStreamManager());
	}

	public ExternalShuffleBlockHandler(ExternalShuffleBlockResolver blockManger, OneForOneStreamManager streamManager) {
		this.blockManger = blockManger;
		this.streamManager = streamManager;
	}

	@Override
	public StreamManager getStreamManager() {
		return streamManager;
	}

	@Override
	public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
		BlockTransferMessage msgObj = Decoder.fromByteByffer(msg);
		handleMessage(msgObj, client, callback);
	}

	/**
	 * 当应用程序结束时，移除已用程序，同时可以指定是否删除与该应用关联的配置文
	 * 件所在的目录及文件（在但单独的线程中完成清理操作）。
	 */
	public void applicationRemoved(String appId, boolean cleanLocalDirs) {
		blockManger.applicationRemoved(appId, cleanLocalDirs);
	}

	private void handleMessage(BlockTransferMessage msg, TransportClient client, RpcCallback callback) {

		if (msg instanceof OpenBlock) {
			handleOpenBlock(client, callback, (OpenBlock)msg);
		} else if (msg instanceof RegisterExecutor) {
			handleRegisterExecutor(client,callback, (RegisterExecutor)msg);
		} else {
			throw new UnsupportedOperationException("不支持的消息类型：" + msg);
		}
	}

	private void handleOpenBlock(TransportClient client, RpcCallback callback, OpenBlock msg) {
		checkAuth(client, msg.appId);

		ArrayList<ManagedBuffer> blocks = Lists.newArrayList();
		for (String blockId : msg.blockIds) {
			blocks.add(blockManger.getBlockData(msg.appId, msg.execId, blockId));
		}
		long streamId = streamManager.registerStream(client.getClientId(), blocks.iterator());
		log.debug("为streamId[{}]注册了[{}]个buffers", streamId, blocks.size());
		callback.onSuccess(new StreamHandle(streamId, msg.blockIds.length).toByteBuffer());
	}

	private void handleRegisterExecutor(TransportClient client, RpcCallback callback, RegisterExecutor msg) {
		checkAuth(client, msg.appId);
		blockManger.registerExecutor(msg.appId, msg.execId, msg.executorShuffleInfo);
		callback.onSuccess(ByteBuffer.wrap(new byte[0]));
	}

	private void checkAuth(TransportClient client, String appId) {
		if (client.getClientId() != null && !client.getClientId().equals(appId)) {
			throw new SecurityException(String.format(
					"客户端[%s]未授权访问应用[%s]", client.getClientId(), appId
			));
		}
	}

	@Override
	public void close() throws IOException {
		if (blockManger != null) {
			blockManger.close();
		}
	}
}
