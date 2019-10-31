package govind.incubator.network.protocol.codec;

import govind.incubator.network.protocol.*;
import govind.incubator.network.protocol.Message.Type;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 客户端使用用于解码server-to-client的响应消息，为无状态的，因此线程安
 * 全可以被多个线程共享。
 */
@Slf4j
@Sharable
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
		Type type = Type.decode(buf);
		Message decodedMsg = decode(buf, type);

		log.info("接收{}类型的消息：{}", type, decodedMsg);
		out.add(decodedMsg);
	}

	private Message decode(ByteBuf buf, Type type) {
		switch (type) {
			case ChunkFetchRequest:
				return ChunkFetchRequest.decode(buf);
			case ChunkFetchSuccess:
				return ChunkFetchSuccess.decode(buf);
			case ChunkFetchFailure:
				return ChunkFetchFailure.decode(buf);
			case RpcRequest:
				return RpcRequest.decode(buf);
			case RpcResponse:
				return RpcResponse.decode(buf);
			case RpcFailure:
				return RpcFailure.decode(buf);
			case StreamRequest:
				return StreamRequest.decode(buf);
			case StreamResponse:
				return StreamResponse.decode(buf);
			case StreamFailure:
				return StreamFailure.decode(buf);
			case OneWayMessage:
				return OneWayMessage.decode(buf);
			default:
				throw new IllegalArgumentException("不支持的消息类型：" + type);
		}
	}

}
