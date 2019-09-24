package govind.incubator.network.conf;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-24
 *
 * 用于管理所有暴露给用户的配置项
 *
 */
public class TransportConf {
	/**
	 * 配置器
	 */
	private final ConfigProvider conf;

	/**
	 * 模块名称
	 */
	private final String module;

	private final String NETWORK_IO_MODE_KEY;
	private final String NETWORK_IO_PREFERDIRECTBUFS_KEY;
	private final String NETWORK_IO_CONNECTIONTIMEOUT_KEY;
	private final String NETWORK_IO_BACKLOG_KEY;
	private final String NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
	private final String NETWORK_IO_SERVERTHREADS_KEY;
	private final String NETWORK_IO_CLIENTTHREADS_KEY;
	private final String NETWORK_IO_RECEIVEBUFFER_KEY;
	private final String NETWORK_IO_SENDBUFFER_KEY;
	private final String NETWORK_SASL_TIMEOUT_KEY;
	private final String NETWORK_IO_MAXRETRIES_KEY;
	private final String NETWORK_IO_RETRYWAIT_KEY;
	private final String NETWORK_IO_LAZYFD_KEY;

	public TransportConf(ConfigProvider conf, String module) {
		this.conf = conf;
		this.module = module;
		NETWORK_IO_MODE_KEY = getConfKey("io.mode");
		NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs");
		NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout");
		NETWORK_IO_BACKLOG_KEY = getConfKey("io.backlog");
		NETWORK_IO_NUMCONNECTIONSPERPEER_KEY = getConfKey("io.numConnectionsPerPeer");
		NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads");
		NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads");
		NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer");
		NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer");
		NETWORK_SASL_TIMEOUT_KEY = getConfKey("sasl.timeout");
		NETWORK_IO_MAXRETRIES_KEY = getConfKey("io.maxRetries");
		NETWORK_IO_RETRYWAIT_KEY = getConfKey("io.retryWait");
		NETWORK_IO_LAZYFD_KEY = getConfKey("io.lazyFD");

	}

	private String getConfKey(String suffix) {
		return "govind.network." + module + "." + suffix;
	}

	public String ioMode() {
		return conf.get(NETWORK_IO_MODE_KEY, "NIO").toUpperCase();
	}

	public boolean preferDirectBufs() {
		return conf.getBoolean(NETWORK_IO_PREFERDIRECTBUFS_KEY, true);
	}

	public int connectionTimeoutMS() {
		return conf.getInt(NETWORK_IO_CONNECTIONTIMEOUT_KEY, 120) * 100;
	}

	public int backlog() {
		return conf.getInt(NETWORK_IO_BACKLOG_KEY, -1);
	}

	public int numConnectionsPerPeer() {
		return conf.getInt(NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
	}

	/**
	 * 0 表示默认创建 2 * #CORES个线程
	 * @return
	 */
	public int serverThreads() {
		return conf.getInt(NETWORK_IO_SERVERTHREADS_KEY, 0);
	}

	/**
	 * 0 表示默认创建 2 * #CORES个线程
	 * @return
	 */
	public int clientThreads() {
		return conf.getInt(NETWORK_IO_CLIENTTHREADS_KEY, 0);
	}

	/**
	 * 接收缓冲区最优值应为：rrt_latency * band_width
	 * @return
	 */
	public int receiveBuffer() {
		return conf.getInt(NETWORK_IO_RECEIVEBUFFER_KEY, -1);
	}

	/**
	 *发送缓冲区最优值应为：rrt_latency * band_width
	 * @return
	 */
	public int sendBuffer() {
		return conf.getInt(NETWORK_IO_SENDBUFFER_KEY,  -1);
	}

	public int maxIORetries() {
		return conf.getInt(NETWORK_IO_MAXRETRIES_KEY, 3);
	}

	public int ioRetryWaitTimeMS() {
		return conf.getInt(NETWORK_IO_RETRYWAIT_KEY, 5) * 1000;
	}

	public boolean lazyFileDescription() {
		return conf.getBoolean(NETWORK_IO_LAZYFD_KEY, true);
	}

	/**
	 * 使用内存映射进行IO操作时，直接内存的大小必须大于2M，否则使用普通的IO操作。
	 * @return
	 */
	public int memoryMapBytes() {
		return conf.getInt("storage.memoryMapThreshold", 2) * 1024 * 1024;
	}
}
