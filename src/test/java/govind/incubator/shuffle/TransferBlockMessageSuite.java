package govind.incubator.shuffle;

import govind.incubator.shuffle.protocol.*;
import govind.incubator.shuffle.protocol.BlockTransferMessage.Decoder;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 */
public class TransferBlockMessageSuite {

	@Test
	public void serializeOpenShuffleBlock() {
		checkSerializeDeserialize(new OpenBlock("app-1","exec-1",new String[]{"b1","b2"}));
		checkSerializeDeserialize(new RegisterExecutor("app-2","exec-2", new ExecutorShuffleInfo(new String[]{"/local1","/local2"}, 32, "testShuffleManager")));

		checkSerializeDeserialize(new UploadBlock("app-3","exec-3","block-1", new byte[]{1,2,3},new  byte[]{4,5,6,7}));
		checkSerializeDeserialize(new StreamHandle(12345L, 16));
	}

	private void checkSerializeDeserialize(BlockTransferMessage msg) {
		BlockTransferMessage toMsg = Decoder.fromByteByffer(msg.toByteBuffer());
		assertEquals(msg, toMsg);
		assertEquals(msg.hashCode(), toMsg.hashCode());
		assertEquals(msg.toString(), toMsg.toString());
	}
}
