package govind.incubator.network.handler;

import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 客户端接收到Rpc响应后的回调函数，其中的两个方法会被调用一次
 */
public interface RpcCallback {
	/**
	 * 客户端成功接收到服务端发送过来的序列化后的数据
	 *
	 * 注意：当`onSuccess`返回时，`response`的内存就会被回收，其内
	 * 容也就不可用，因此如果程序在onSuccess返回后仍然使用该内容，该
	 * 当`拷贝`该内容 ！
	 *
	 * @param response
	 */
	void onSuccess(ByteBuffer response);

	/**
	 * 数据从服务端发送过程中、客户端自身出现错误时被调用
	 * @param cause
	 */
	void onFailure(Throwable cause);
}
