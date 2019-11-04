package govind.incubator.shuffle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import govind.incubator.network.buffer.FileSegmentManagedBuffer;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.util.NettyUtil;
import govind.incubator.shuffle.protocol.ExecutorShuffleInfo;
import govind.incubator.shuffle.util.LevelDBProvider;
import govind.incubator.shuffle.util.StoreVersion;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.*;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 *
 * 功能：BlockManger
 *
 * 在Executor外另起一个进程，用于将block存储为一段段的本地物理文件。每个
 * Executor必须必须注册说明其文件的存储位置(local dirs)和如何存储(shuffle manager)。
 *
 * 类似功能可以参考 FileShuffleBlockResolver、IndexShuffleBlockResolver。
 *
 */
@Slf4j
public class ExternalShuffleBlockResolver implements Closeable{
	/**
	 * 每个应用注册时的Key前缀，因为LevelDB支持前缀搜索，这样提高查询速率。
	 */
	private static final String APP_KEY_PREFIX = "AppExecShuffleInfo";
	private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

	private static final ObjectMapper mapper = new ObjectMapper();

	/** 单线程线程池，用于执行比较耗时的文件夹递归删除操作 */
	private  final Executor dirCleaner;
	private final TransportConf conf;

	/** 保存所有已注册的Executor的元数据 */
	final ConcurrentMap<AppExecId, ExecutorShuffleInfo> executors;
	final File registeredExecutorFile;
	final DB db;

	public ExternalShuffleBlockResolver(TransportConf conf, File registeredExecutorFile) throws IOException {
		this(
				Executors.newSingleThreadExecutor(NettyUtil.createThreadFactory("govind-shuffle-directory-cleaner")),
				conf,
				registeredExecutorFile);
	}

	public ExternalShuffleBlockResolver(Executor dirCleaner, TransportConf conf, File registeredExecutorFile) throws IOException {
		this.dirCleaner = dirCleaner;
		this.conf = conf;
		this.registeredExecutorFile = registeredExecutorFile;
		if (registeredExecutorFile != null) {
			db = LevelDBProvider.initLevelDB(registeredExecutorFile,CURRENT_VERSION, mapper);
			executors = reloadRegisteredExecutors(db);
		} else {
			db = null;
			executors = Maps.newConcurrentMap();
		}
	}

	/********************** 公共接口 ************************/

	/** 注册Executor，同时持久化元数据信息，以便找到其shuffle文件 */
	public void registerExecutor(String appId, String execId, ExecutorShuffleInfo shuffleInfo) {
		AppExecId appExecId = new AppExecId(appId, execId);
		log.debug("注册Executor[{}]，元数据信息为：{}",appExecId, shuffleInfo);

		try {
			if (db != null) {
				byte[] key = dbAppExecKey(appExecId);
				byte[] value = mapper.writeValueAsString(shuffleInfo).getBytes(Charsets.UTF_8);
				db.put(key, value);
			}
		} catch (IOException e) {
			log.error("保存注册的Executor元数据失败", e);
		}
		executors.put(appExecId, shuffleInfo);
	}


	/** 删除该应用下注册的所有Executor元数据，同时允许配置是否删除对应的物理文件 */
	public void applicationRemoved(String appId, boolean cleanupDirs) {
		log.info("应用程序移除：{}，是否清理对应的本地文件夹：{}", appId, cleanupDirs);

		Iterator<Entry<AppExecId, ExecutorShuffleInfo>> iter = executors.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<AppExecId, ExecutorShuffleInfo> entry = iter.next();
			AppExecId appExecId = entry.getKey();
			final ExecutorShuffleInfo shuffleInfo = entry.getValue();

			if (appId.equals(appExecId.appId)) {
				iter.remove();
				if (db != null) {
					try {
						db.delete(dbAppExecKey(appExecId));
					} catch (IOException e) {
						log.error("无法删除{}对应的状态", appExecId, e);
					}
				}

				if (cleanupDirs) {
					log.info("删除Executor[{}]状态对应的{}个本地目录", appExecId, shuffleInfo.localDirs.length);
					dirCleaner.execute(()-> deleteExecutorDirs(shuffleInfo.localDirs));
				}
			}
		}
	}

	/**
	 * 根据blockId获取shuffle file对应的FileSegmentMangedBuffer。
	 *
	 * 前提条件：
	 * 1、blockId的格式为： shuffle_ShuffleId_MapId_reduceId
	 * 2、HashShuffle和SortBasedShuffle的存储方式是已知的。
	 */
	public ManagedBuffer getBlockData(String appId, String execId, String blockId) {
		String[] splits = blockId.split("_");

		if (splits.length < 4) {
			throw new IllegalArgumentException("非法的block id：" + blockId);
		} else if (!splits[0].equals("shuffle")) {
			throw new IllegalArgumentException("非法的shuffle blockI id：" + blockId);
		}

		int shuffleId = Integer.parseInt(splits[1]);
		int mapId = Integer.parseInt(splits[2]);
		int reduceId = Integer.parseInt(splits[3]);

		ExecutorShuffleInfo shuffleInfo = executors.get(new AppExecId(appId, execId));
		if (shuffleInfo == null){
			throw new RuntimeException(String.format(
					"没有找到Executor元数据信息，确定Executor[{}]是否注册？", appId, execId
			));
		}

		if ("org.apache.spark.shuffle.hash.HashShuffleManager".equals(shuffleInfo.shuffleManager)) {
			return getHashBasedShuffleBlockData(shuffleInfo, blockId);
		} else if ("org.apache.spark.shuffle.sort.SortShuffleManager".equals(shuffleInfo.shuffleManager)
		|| "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager".equals(shuffleInfo.shuffleManager)) {
			return getSortBasedShuffleBlockData(shuffleInfo, shuffleId, mapId, reduceId);
		}  else {
			throw new UnsupportedOperationException("不支持的ShuffleManger：" + shuffleInfo.shuffleManager);
		}
	}

	/** 将文件名映射为对应的本地路径下的物理文件 */
	public static File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
		int hasCode = NettyUtil.nonnagetiveHash(filename);
		String dir = localDirs[hasCode % localDirs.length];
		int subDirId = (hasCode / localDirs.length ) % subDirsPerLocalDir;
		return new File(new File(dir, String.format("%02x", subDirId)), filename);
	}

	@Override
	public void close() {
		if (db != null) {
			try {
				db.close();
			} catch (IOException e) {
				log.error("关闭数据库失败：{}", e);
			}
		}
	}

	/*******************************************************/

	/** 同步删除文件夹，每次删除一个，每个Executor中的文件夹在单独的线程中进行，删除操作会比较耗时 */
	private void deleteExecutorDirs(String[] dirs) {
		for (String dir : dirs) {
			try {
				NettyUtil.deleteRecursively(new File(dir));
				log.info("成功删除文件夹：{}", dir);
			} catch (Exception e) {
				log.error("删除文件夹{}失败：{}", dir, e);
			}
		}
	}

	/**
	 * hash-based shuffle data存储方式是每个block对应一个文件，
	 * 文件名：shuffle_ShuffleId_MapId_reduceId
	 *
	 * 可以参考：FileShuffleBlockResolver
	 */
	private ManagedBuffer getHashBasedShuffleBlockData(ExecutorShuffleInfo excutor, String blockId) {
		File file = getFile(excutor.localDirs, excutor.subDirsPerLocalDir, blockId);
		return new FileSegmentManagedBuffer(file, 0, file.length());
	}

	/**
	 * sort-based shuffle data对应两个文件：
	 * 	1、索引文件：shuffle_ShuffleId_MapId_0.index
	 * 	2、数据文件：shuffle_ShuffleId_MapId_0.data
	 */
	private ManagedBuffer getSortBasedShuffleBlockData(ExecutorShuffleInfo excutor, int shuffleId, int mapId, int reduceId) {

		String indexFileName =  "shuffle_" + shuffleId + "_" + mapId + "_0.index";
		String dataFileName =  "shuffle_" + shuffleId + "_" + mapId + "_0.data";
		File indexFile = getFile(excutor.localDirs, excutor.subDirsPerLocalDir, indexFileName);
		File dataFie = getFile(excutor.localDirs, excutor.subDirsPerLocalDir, dataFileName);

		DataInputStream in = null;

		try {
			in = new DataInputStream(new FileInputStream(indexFile));
			in.skipBytes(reduceId * 8);
			long offset = in.readLong();
			long nextOffset = in.readLong();
			return new FileSegmentManagedBuffer(dataFie, offset, nextOffset - offset);
		} catch (IOException e) {
			throw new RuntimeException("打开文件" + indexFile + "失败", e);
		} finally {
			Closeables.closeQuietly(in);
		}
	}

	/** 从数据库加载Executor保存文件位置信息等的元数据 */
	private ConcurrentMap<AppExecId, ExecutorShuffleInfo> reloadRegisteredExecutors(DB db) throws IOException {
		ConcurrentMap<AppExecId, ExecutorShuffleInfo>  regiesteredExecutors = Maps.newConcurrentMap();
		if (db != null) {
			DBIterator iter = db.iterator();
			iter.seek(APP_KEY_PREFIX.getBytes(Charsets.UTF_8));
			while (iter.hasNext()) {
				Entry<byte[], byte[]> entry = iter.next();
				String key = new String(entry.getKey(), Charsets.UTF_8);
				if (!key.startsWith(APP_KEY_PREFIX)) {
					break;
				}

				AppExecId appExecId = parseDbAppExecKey(key);
				ExecutorShuffleInfo shuffleInfo = mapper.readValue(entry.getValue(), ExecutorShuffleInfo.class);
				regiesteredExecutors.put(appExecId, shuffleInfo);
			}
		}
		return regiesteredExecutors;
	}

	private static AppExecId parseDbAppExecKey(String key) throws IOException {
		if (!key.startsWith(APP_KEY_PREFIX)) {
			throw new IllegalArgumentException("要解析的字符串前缀应该为：" + APP_KEY_PREFIX);
		}

		String appExecJson = key.substring(APP_KEY_PREFIX.length() + 1);
		AppExecId appExecId = mapper.readValue(appExecJson, AppExecId.class);
		return appExecId;
	}

	private static byte[] dbAppExecKey(AppExecId appExecId) throws IOException {
		String appExecJson = mapper.writeValueAsString(appExecId);
		String key = APP_KEY_PREFIX + ";" + appExecJson;
		return key.getBytes(Charsets.UTF_8);
	}

	/** 封装Executor的唯一标识信息：appId + execId */
	public static class AppExecId {
		public final String appId;
		public final String execId;

		@JsonCreator
		public AppExecId(@JsonProperty("appId") String appId, @JsonProperty("execId") String execId) {
			this.appId = appId;
			this.execId = execId;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(appId, execId);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof AppExecId) {
				AppExecId aei = (AppExecId) obj;
				return Objects.equal(appId, aei.appId)
						&&  Objects.equal(execId, aei.execId);
			}
			return false;
		}
	}
}
