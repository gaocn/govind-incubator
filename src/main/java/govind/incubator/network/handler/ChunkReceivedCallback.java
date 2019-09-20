package govind.incubator.network.handler;

import govind.incubator.network.buffer.ManagedBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 当客户端接收到chunk数据时被调用，对于接收到的chunk数据，该框架确保
 * 回调函数被同一个线程按照请求顺序处理chunk数据。
 *
 * PS：若一个chunk stream传输失败，所有已知的chunk requests可能都
 * 会失败。
 *
 */
public interface ChunkReceivedCallback {

	/**
	 * 当客户端接收到chunk数据时被调用
	 *
	 * PS：初始时`buffer`的引用计数为1，当`onSuccess`返回时就会被
	 * 释放，因此如需要在方法返回时继续使用，需要调用`retain`方法或
	 * 者`拷贝`缓存中的内容。
	 *
	 * @param chunkIdx
	 * @param buffer
	 */
	void onSuccess(int chunkIdx, ManagedBuffer buffer);


	/**
	 * 当客户端获取某个chunk失败时被调用。
	 *
	 * PS:
	 * 1、该方法可能会在获取prior chunk失败时被调用；
	 * 2、当该方法被调用时，此时stream可能失效，也可能没有失效，因此
	 * 客户端不能假定服务端的stream已经关闭！
	 *
	 * @param chunkIdx
	 * @param cause
	 */
	void onFailure(int chunkIdx, Throwable cause);
}
