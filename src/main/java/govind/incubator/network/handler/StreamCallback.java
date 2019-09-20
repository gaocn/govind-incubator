package govind.incubator.network.handler;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 客户端接收到流请求后的回调函数，当数据到达时调用onData(...)方法，当数
 * 据接收完毕时调用onComplete(...)方法
 */
public interface StreamCallback {

	/**
	 * 当流数据到达客户端时被调用
	 * @param streamId
	 * @param buffer
	 * @throws IOException
	 */
	void onData(String streamId, ByteBuffer buffer) throws IOException;

	/**
	 * 当数据接收完毕时被调用
	 * @param streamId
	 * @throws IOException
	 */
	void onComplete(String streamId) throws IOException;

	/**
	 * 数据从服务端发送过程中、客户端自身出现错误时被调用
	 *
	 * @param streamId
	 * @param cause
	 * @throws IOException
	 */
	void onFailure(String streamId, Throwable cause) throws IOException;
}
