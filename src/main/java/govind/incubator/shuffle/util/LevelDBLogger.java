package govind.incubator.shuffle.util;

import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.Logger;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 */
@Slf4j
public class LevelDBLogger implements Logger {
	@Override
	public void log(String message) {
		log.info(message);
	}
}
