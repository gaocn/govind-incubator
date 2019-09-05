package govind.incubator.network.protocol.codec;

import govind.incubator.network.protocol.AbstractResponseMessage;
import govind.incubator.network.protocol.Message;
import govind.incubator.network.protocol.MessageWithHeader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Sharable
@Slf4j
public class MessageEncoder extends MessageToMessageEncoder<Message> {
	/**
	 * 对消息进行编码后传输
	 * 1、对于no-body的消息会把：帧长度、消息类型和消息本身放在ByteBuf中，然后添加到"out"中。
	 * 2、对于{@link govind.incubator.network.protocol.ChunkFetchSuccess}消息其中会
	 * 有ManagedBuffer，因此会将其单独添加到"out"中，以便利用零拷贝进行数据传输。
	 *
	 * @param ctx
	 * @param msg
	 * @param out
	 * @throws Exception
	 */
	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
		Object body = null;
		long bodyLength = 0L;
		boolean isBodyInFrame = false;

		//若body中有消息，则尝试利用Zero-Copy进行传输
		if (msg.body() != null) {
			try {
				bodyLength = msg.body().size();
				body = msg.body().nettyByteBuf();
				isBodyInFrame = msg.isBodyInFrame();
			} catch (IOException e) {
				//若是响应报文，则将异常消息返回给客户端
				if (msg instanceof AbstractResponseMessage) {
					AbstractResponseMessage resp =  (AbstractResponseMessage) msg;
					String error = e.getMessage() != null ? e.getMessage() : "null";
					log.error("处理来自{}的消息{}异常：{}", ctx.channel().remoteAddress(), msg, e.getMessage());
					//将错误消息编码后返回
					encode(ctx, resp.createFailureResponse(error), out);
				} else {
					throw e;
				}
			}
		}

		/**
		 * 帧格式：
		 *  ---------------------------------
		 *  | Frame Len(8 bytes) | ByteBuf  |
		 *  ---------------------------------
		 *  					/           |
		 *  			     /	            |
		 *  			  /	                |
		 *  		   /                    |
		 *  		/                       |
		 * 	        -------------------------
		 * 	        | Header  | BodyInFrame |
		 * 	        -------------------------
		 * 	        \          \
		 * 			 -------------------------------
		 * 			 | Type(1 byte) | MsgInHeader  |
		 * 			 -------------------------------
		 */
		Message.Type type = msg.type();
		int headerLen = 8 + type.encodedLength() + msg.encodedLength();

		long frameLen = headerLen + (isBodyInFrame ? bodyLength : 0);

		ByteBuf header = ctx.alloc().heapBuffer(headerLen);
		header.writeLong(frameLen);
		type.encode(header);
		msg.encode(header);

		assert header.writableBytes() == 0 : "可写字节数应该为0";

		if (body != null && bodyLength > 0) {
			out.add(new MessageWithHeader(header, body, headerLen, bodyLength));
		} else {
			out.add(header);
		}

	}
}
