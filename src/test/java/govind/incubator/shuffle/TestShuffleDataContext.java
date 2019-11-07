package govind.incubator.shuffle;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import govind.incubator.shuffle.ExternalShuffleBlockResolver;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;

import java.io.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 *
 * 用于创建hash-based、sort-based shuffle数据同时清理这些生成的临时数据，
 * 以便能够使用{@link govind.incubator.shuffle.ExternalShuffleBlockResolver}
 * 读取这些数据。
 *
 */
public class TestShuffleDataContext {

	public final String[] localDirs;
	public final int subDirPerLocalDir;

	public TestShuffleDataContext(int numLocalDirs, int subDirPerLocalDir) {
		this.localDirs = new String[numLocalDirs];
		this.subDirPerLocalDir = subDirPerLocalDir;
	}


	public void create() {
		for (int i = 0; i < localDirs.length; i++) {
			localDirs[i] = Files.createTempDir().getAbsolutePath();
			for (int i1 = 0; i1 < subDirPerLocalDir; i1++) {
				new File(localDirs[i], String.format("%02x", i1)).mkdir();
			}
		}
	}

	public void cleanup() {
		for (String dir : localDirs) {
			deleteRecurively(new File(dir));
		}
	}

	private void deleteRecurively(File file) {
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			if (files != null) {
				for (File child : files) {
					deleteRecurively(child);
				}
			}
		}
		file.delete();
	}

	public ExecutorShuffleInfo createExecutorInfo(String shuffleManager) {
		return new ExecutorShuffleInfo(localDirs, subDirPerLocalDir, shuffleManager);
	}

	public void insertSortBasedShuffleData(int shuffleId, int mapId, byte[][] blocks) throws IOException {
		String blockId = "shuffle_" + shuffleId + "_" + mapId + "_0";

		OutputStream dataStream = null;
		DataOutputStream indexStream = null;
		boolean suppressExceptionDuringClose = true;

		try {
			dataStream = new FileOutputStream(ExternalShuffleBlockResolver.getFile(localDirs,subDirPerLocalDir, blockId + ".data"));
			indexStream = new DataOutputStream(new FileOutputStream(ExternalShuffleBlockResolver.getFile(localDirs, subDirPerLocalDir, blockId + ".index")));

			long offset = 0;
			indexStream.writeLong(offset);
			for (byte[] block : blocks) {
				offset+=block.length;
				dataStream.write(block);
				indexStream.writeLong(offset);
			}
			suppressExceptionDuringClose = false;
		}finally {
			Closeables.close(dataStream, suppressExceptionDuringClose);
			Closeables.close(indexStream, suppressExceptionDuringClose);
		}


	}

	public void insertHashBasedShuffleData(int shuffleId, int mapId, byte[][] blocks) throws IOException {
		for (int i = 0; i < blocks.length; i++) {
			String blockId = "shuffle_" + shuffleId + "_" + mapId + "_" + i;
			Files.write(blocks[i], ExternalShuffleBlockResolver.getFile(localDirs, subDirPerLocalDir, blockId));
		}
	}

}
