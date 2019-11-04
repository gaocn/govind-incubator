package govind.incubator.shuffle.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.protocol.Encodable;
import govind.incubator.network.util.CodecUtil;
import govind.incubator.network.util.CodecUtil.StringArray;
import govind.incubator.network.util.CodecUtil.Strings;
import io.netty.buffer.ByteBuf;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Arrays;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 包含所有某个Executor写到Shuffle Server文件的定位信息
 *
 */
public class ExecutorShuffleInfo implements Encodable {
	/** Executors将其shuffle文件存放在Shuffle Server中的位置集合 */
	public final String[] localDirs;

	/** 每个localDir中创建的子目录个数 */
	public final int subDirsPerLocalDir;

	/** Executor使用的ShuffleManger(HashShuffleManager or SortShuffleManager) */
	public final String shuffleManager;

	@JsonCreator
	public ExecutorShuffleInfo(
			@JsonProperty("localDirs") String[] localDirs,
			@JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
			@JsonProperty("shuffleManager") String shuffleManager) {
		this.localDirs = localDirs;
		this.subDirsPerLocalDir = subDirsPerLocalDir;
		this.shuffleManager = shuffleManager;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.StringArray.encodedLength(localDirs)
				+ CodecUtil.Strings.encodedLength(shuffleManager)
				+ 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.StringArray.encode(buf, localDirs);
		buf.writeInt(subDirsPerLocalDir);
		CodecUtil.Strings.encode(buf, shuffleManager);
	}

	public static ExecutorShuffleInfo decode(ByteBuf buf) {
		String[] localDirs = StringArray.decode(buf);
		int subDirsPerLocalDir = buf.readInt();
		String shuffleManager = Strings.decode(buf);
		return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);

	}

	@Override
	public int hashCode() {
		return Objects.hashCode(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof ExecutorShuffleInfo) {
			ExecutorShuffleInfo esi =  (ExecutorShuffleInfo) obj;
			return Objects.equal(subDirsPerLocalDir, esi.subDirsPerLocalDir) &&
					Objects.equal(shuffleManager, esi.shuffleManager) &&
					Arrays.equals(localDirs, esi.localDirs);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("subDirsPerLocalDir", subDirsPerLocalDir)
				.add("subDirsPerLocalDir", shuffleManager)
				.add("localDirs", Arrays.toString(localDirs))
				.toString();
	}
}
