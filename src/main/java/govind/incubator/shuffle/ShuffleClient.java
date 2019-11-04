package govind.incubator.shuffle;

import java.io.Closeable;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 提供从Executor或外部服务读取文件的接口
 *
 */
public abstract class ShuffleClient implements Closeable {

	/**
	 * 初始化ShuffleClient，需要指定Executor所在的appId
	 */
	public void init(String appId) {}


	/**
	 * 异步从远程获取指定序列的blocks
	 *
	 * 注意：该API可以传入多个blockId进行批量请求，用户可以通过onBlockFetchSuccess
	 * 方法知道某个block成功获取，而不是等待所有blocks都获取成功后在返回。
	 *
	 */
	public abstract void fetchBlocks(
			String host,
			int port,
			String execId,
			String[] blockIds,
			BlockFetchingListener listener);

}
