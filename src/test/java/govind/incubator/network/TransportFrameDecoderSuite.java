package govind.incubator.network;

import govind.incubator.network.protocol.codec.Inteceptor;
import govind.incubator.network.protocol.codec.TransportFrameDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-8
 */
@Slf4j
public class TransportFrameDecoderSuite {
	private static Random RND = new Random();

	@AfterClass
	public static void cleanup() {
		RND = null;
	}

	@Test
	public void testFrameDecoding() throws Exception {
		TransportFrameDecoder decoder = new TransportFrameDecoder();
		ChannelHandlerContext ctx = mockChannelHandlerContext();
		ByteBuf data = createAndFeedFrame(100, decoder, ctx);
		verifyAndCloseDecoder(decoder, ctx, data);
	}

	@Test
	public void testInterception() throws Exception {
		final int interceptedReads = 3;
		TransportFrameDecoder decoder = new TransportFrameDecoder();
		MockInteceptor interceptor = spy(new MockInteceptor(interceptedReads));
		ChannelHandlerContext ctx = mockChannelHandlerContext();

		byte[] data = new byte[8];
		ByteBuf len = Unpooled.copyLong(8 + data.length);
		ByteBuf dataBuf = Unpooled.wrappedBuffer(data);

		try {
			decoder.setInteceptor(interceptor);
			for (int i = 0; i < interceptedReads; i++) {
				decoder.channelRead(ctx, dataBuf);
				assertEquals(0,  dataBuf.refCnt());
				dataBuf = Unpooled.wrappedBuffer(data);
			}

			decoder.channelRead(ctx, len);
			decoder.channelRead(ctx, dataBuf);

			verify(interceptor, times(interceptedReads)).handle(any(ByteBuf.class));
			verify(ctx).fireChannelRead(any(ByteBuf.class));
			assertEquals(0, len.refCnt());
			assertEquals(0, dataBuf.refCnt());
		} finally {
			release(len);
			release(dataBuf);
		}
	}

	@Test
	public void testRetainedFrame() throws Exception {
		TransportFrameDecoder decoder = new TransportFrameDecoder();
		final AtomicInteger count = new AtomicInteger();
		final List<ByteBuf> retained = new ArrayList<>();

		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		when(ctx.fireChannelRead(any())).thenAnswer(((Answer<Void>) in -> {
			ByteBuf buf = (ByteBuf) in.getArguments()[0];
			if (count.incrementAndGet() % 2 == 0) {
				retained.add(buf);
			} else {
				buf.release();
			}
			return null;
		}));

		ByteBuf data = createAndFeedFrame(100, decoder, ctx);

		try {
			/* 验证是否所有retained buf可读 */
			for (ByteBuf b : retained) {
				byte[] bytes = new byte[b.readableBytes()];
				b.readBytes(bytes);
				b.release();
			}
			verifyAndCloseDecoder(decoder, ctx,  data);
		} finally {
			for (ByteBuf buf : retained) {
				release(buf);
			}
		}
	}

	@Test
	public void testSplitLengthField() throws Exception {
		byte[] frame = new byte[1024 * (RND.nextInt(31) + 1)];
		ByteBuf buf = Unpooled.buffer(frame.length + 8);
		buf.writeLong(frame.length + 8);
		buf.writeBytes(frame);

		TransportFrameDecoder decoder = new TransportFrameDecoder();
		ChannelHandlerContext ctx = mockChannelHandlerContext();

		try {
			decoder.channelRead(ctx, buf.readSlice(RND.nextInt(7)).retain());
			verify(ctx, never()).fireChannelRead(any(ByteBuf.class));

			decoder.channelRead(ctx, buf);
			verify(ctx,  times(1)).fireChannelRead(any(ByteBuf.class));

			assertEquals(0, buf.refCnt());

		} finally {
			decoder.channelInactive(ctx);
			release(buf);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNegativeFrameSize() throws Exception {
		testInvalidFrame(-1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyFrameSize() throws Exception {
		testInvalidFrame(8);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testLargeFrameSize() throws Exception {
		//测试时，需要向其中多添加些直接，以便满足测试条件
		testInvalidFrame(Integer.MAX_VALUE + 9L);
	}


	/********************************************************/
	/**
	 * 随机生成不同长度的frames，并将它们feed给decoder，最后验证这些frames是否被成功读取
	 * @param frameCount
	 * @param decoder
	 * @param ctx
	 */
	private ByteBuf createAndFeedFrame(
			int frameCount,
			TransportFrameDecoder decoder,
			ChannelHandlerContext ctx) throws Exception {
		ByteBuf data = Unpooled.buffer();

		for (int i = 0; i < frameCount; i++) {
			byte[] frame = new byte[1024 * (RND.nextInt(31) + 1)];
			data.writeLong(frame.length + 8);
			data.writeBytes(frame);
		}

		try {
			while (data.isReadable()) {
				int size = RND.nextInt(4 *  1_024) + 256;
				decoder.channelRead(
						ctx,
						data.readSlice(Math.min(data.readableBytes(), size)).retain());
			}

			verify(ctx, times(frameCount)).fireChannelRead(any(ByteBuf.class));
		} catch (Exception e) {
			release(data);
			throw e;
		}
		return data;
	}

	private void verifyAndCloseDecoder(
			TransportFrameDecoder decoder,
			ChannelHandlerContext ctx,
			ByteBuf data) throws Exception {
		try {
			decoder.channelInactive(ctx);
			assertTrue("应该不能存在指向data的引用", data.release());
		} finally {
			release(data);
		}
	}

	private void release(ByteBuf data) {
		if (data.refCnt() >  0) {
			data.release(data.refCnt());
		}
	}

	private void testInvalidFrame(long size) throws Exception {
		TransportFrameDecoder frameDecoder = new TransportFrameDecoder();
		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		ByteBuf frame = Unpooled.copyLong(size);
//		ByteBuf frame = Unpooled.buffer();
//		frame.writeLong(size);
//		if (size - 8 > 0) {
//			int times = (int)((size - 8) / Integer.MAX_VALUE + 1);
//			for (int i = 0; i < times; i++) {
//				int capacity = (i == times - 1 ? (int) (size - 8 - i  * Integer.MAX_VALUE): Integer.MAX_VALUE);
//				byte[] tmp = new byte[capacity];
//				frame.writeBytes(tmp);
//			}
//		}

		try {
			frameDecoder.channelRead(ctx, frame);
		}catch (Exception e){
			log.debug(e.getMessage());
			throw e;
		} finally {
			release(frame);
		}
	}

	private ChannelHandlerContext  mockChannelHandlerContext() {
		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

		when(ctx.fireChannelRead(any())).thenAnswer((Answer<Void>) in -> {
			ByteBuf buf = (ByteBuf) in.getArguments()[0];
			buf.release();
			return null;
		});
		return ctx;
	}

	/**
	 * 消费TransportFrameDecoder中的数据
	 */
	private static class MockInteceptor implements Inteceptor {
		private int remainingReads;

		public MockInteceptor(int readCount) {
			this.remainingReads = readCount;
		}

		@Override
		public boolean handle(ByteBuf buf) throws Exception {
			buf.readerIndex(buf.readerIndex() + buf.readableBytes());
			assertFalse(buf.isReadable());
			remainingReads -= 1;
			return remainingReads != 0;
		}

		@Override
		public void channelInactive() throws Exception {

		}

		@Override
		public void exceptionCaught(Throwable cause) throws Exception {

		}
	}
}


