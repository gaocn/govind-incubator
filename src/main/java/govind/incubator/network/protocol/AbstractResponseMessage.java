package govind.incubator.network.protocol;

import govind.incubator.network.buffer.ManagedBuffer;

/**
 * 服务端响应消息父类
 */
public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage{
	public AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
		super(body, isBodyInFrame);
	}

	/**
	 * 服务端处理出现异常，将异常消息 返回
	 * @param msg
	 * @return
	 */
	public abstract ResponseMessage createFailureResponse(String msg);
}
