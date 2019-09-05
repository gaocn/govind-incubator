package govind.incubator.network.protocol;


import govind.incubator.network.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

public interface Message extends Encodable{

	/**
	 * 消息类型
	 * @return
	 */
	Type type();


	/**
	 * Optional Body
	 * 当{@link #isBodyInFrame()}为true时，  表示需要进行大量数据传输，
	 * 则将消息序列化到{@link ManagedBuffer}中，以方便进行传输。
	 * @return
	 */
	ManagedBuffer body();

	/**
	 * true表示将消息内容和body放在同一个帧中进行传输。
	 *
	 * PS：针对需要进行大量数据传输的情况，将Body单独存放以便使用Zero-Copy技术
	 * 实现快速数据传输。
	 *
	 * @return
	 */
	boolean isBodyInFrame();

	/**
	 * 消息类型
	 */
	enum Type implements Encodable {
		ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
		RpcRequest(3), RpcResponse(4), RpcFailure(5),
		StreamRequest(6), StreamResponse(7), StreamFailure(8),
		OneWayMessage(9);

		/**
		 * 支持128个消息类型
		 */
		private final byte id;

		Type(int id) {
			assert id < 128 : "不支持的消息类型";
			this.id = (byte) id;
		}

		@Override
		public int encodedLength() {
			return 1;
		}

		@Override
		public void encode(ByteBuf buf) {
			buf.writeByte(this.id);
		}

		public static Type decode(ByteBuf buf) {
			byte id = buf.readByte();
			switch (id) {
				case 0: return ChunkFetchRequest;
				case 1: return ChunkFetchSuccess;
				case 2: return ChunkFetchFailure;
				case 3: return RpcRequest;
				case 4: return RpcResponse;
				case 5: return RpcFailure;
				case 6: return StreamRequest;
				case 7: return StreamResponse;
				case 8: return StreamFailure;
				case 9: return OneWayMessage;
				default: throw new IllegalArgumentException("非法消息类型");
			}
		}
	}

}
