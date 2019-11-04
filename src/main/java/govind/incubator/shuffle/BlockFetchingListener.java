package govind.incubator.shuffle;

import govind.incubator.network.buffer.ManagedBuffer;

import java.util.EventListener;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 */
public interface BlockFetchingListener extends EventListener {
	/**
	 * 每次成功获取block后被调用，当该方法返回时{@code data}会被自动释放，若{@code data}被
	 * 传递给其他线程，需要接收者自己调用retain()和release()，或者将{@code data}拷贝到一个
	 * 新的buffer中。
	 */
	void onBlockFetchSuccess(String blockId, ManagedBuffer data);

	/**
	 * 每次获取block失败时被调用
	 */
	void onBlockFetchFailure(String blockId, Throwable cause);

}
