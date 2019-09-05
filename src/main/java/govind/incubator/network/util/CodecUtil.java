package govind.incubator.network.util;

import io.netty.buffer.ByteBuf;
import org.apache.commons.io.Charsets;

import java.nio.charset.Charset;

/**
 * 实现常用元素类型的编解码
 */
public class CodecUtil {
	private final static Charset DEFAULT_CHARSET = Charsets.UTF_8;

	public static class Strings {
		public static int encodedLength(String s) {
			return 4 + s.getBytes(DEFAULT_CHARSET).length;
		}

		public static void encode(ByteBuf buf, String s) {
			byte[] bytes = s.getBytes(DEFAULT_CHARSET);
			buf.writeInt(bytes.length);
			buf.writeBytes(bytes);
		}

		public static String decode(ByteBuf buf) {
			int length = buf.readInt();
			byte[] bytes = new byte[length];
			buf.readBytes(bytes);
			return new String(bytes, DEFAULT_CHARSET);
		}

	}

	public static class ByteArray {
		public static int encodedLength(byte[] bytes) {
			return 4 + bytes.length;
		}

		public static void encode(ByteBuf buf, byte[] bytes) {
			buf.writeInt(bytes.length);
			buf.writeBytes(bytes);
		}

		public static byte[] decode(ByteBuf buf) {
			int length = buf.readInt();
			byte[] bytes = new byte[length];
			buf.readBytes(bytes);
			return bytes;
		}
	}

	public static class StringArray {
		public static int encodedLength(String[] strs) {
			int encodedLen = 4;

			for (String s : strs) {
				encodedLen += Strings.encodedLength(s);
			}
			return encodedLen;
		}

		public static void encode(ByteBuf buf, String[] strs) {
			buf.writeInt(strs.length);
			for (String s : strs) {
				Strings.encode(buf, s);
			}
		}

		public static String[] decode(ByteBuf buf) {
			int length =  buf.readInt();
			String[] res = new String[length];
			for (int i = 0; i < res.length; i++) {
				res[i] = Strings.decode(buf);
			}
			return res;
		}
	}

}
