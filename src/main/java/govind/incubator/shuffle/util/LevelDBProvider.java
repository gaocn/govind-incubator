package govind.incubator.shuffle.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 *
 * LevelDB工具类
 *
 */
@Slf4j
public class LevelDBProvider {
	public static DB initLevelDB(File dbFile, StoreVersion version, ObjectMapper mapper) throws IOException {
		DB tmpDB = null;

		if (dbFile != null) {
			Options options = new Options();
			options.createIfMissing(false);
			options.logger(new LevelDBLogger());

			try {
				tmpDB = JniDBFactory.factory.open(dbFile, options);
			} catch (NativeDB.DBException e) {
				if (e.isNotFound() || e.getMessage().contains("does not exist")) {
					log.info("创建state database: {}", dbFile);
					options.createIfMissing(true);
					try {
						tmpDB = JniDBFactory.factory.open(dbFile, options);
					} catch (IOException e1) {
						throw new IOException("无法创建state store", e1);
					}
				} else {
					//可能由于未知原因导致数据库崩溃，尝试删除数据库并重新创建一个
					if (dbFile.isDirectory()) {
						for (File file : dbFile.listFiles()) {
							if (!file.delete()) {
								log.warn("无法删除文件：{}",  file.getPath());
							}
						}
					}

					if (!dbFile.delete()) {
						log.warn("无法删除文件：{}",  dbFile.getPath());
					}

					options.createIfMissing(true);
					try {
						tmpDB = JniDBFactory.factory.open(dbFile, options);
					} catch (IOException e1) {
						throw new IOException("无法创建state store", e1);
					}

				}
			}

			//若版本不匹配，则抛出异常，表示服务不可用
			checkVersion(tmpDB, version, mapper);
		}
		return tmpDB;
	}

	/**
	 * 版本格式：major.minor
	 *
	 * major不同表示有不兼容的变化，minor不同是允许的也就是说我们可以minor
	 * 前后不同版本中的内容。
	 */
	public static void checkVersion(DB db, StoreVersion newVersion, ObjectMapper mapper) throws IOException {
		byte[] version = db.get(StoreVersion.KEY);
		if (version == null) {
			storeVersion(db, newVersion, mapper);
		} else {
			StoreVersion storeVersion = mapper.readValue(version, StoreVersion.class);
			if (storeVersion.major != newVersion.major) {
				throw new IOException("无法从DB版本为" + storeVersion
						+ "的数据库中读取状态，因为与当前版本" + newVersion
						+ "不兼容");
			}
			storeVersion(db, newVersion, mapper);
		}
	}

	public static void storeVersion(DB db, StoreVersion newVersion, ObjectMapper mapper) throws IOException {
		db.put(StoreVersion.KEY, mapper.writeValueAsBytes(newVersion));
	}
}
