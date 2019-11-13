package govind.incubator.launcher.util;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-7
 *
 * launcher中的常量
 *
 */
public class LauncherConsts {

	/** configuration key for master */
	public static final String GOVIND_MASTER = "govind.master";

	/** configuration key for  dirver  */
	public static final String DRIVER_MEMOPRY = "govind.driver.memory";
	public static final String DRIVER_EXTRA_CLASSPATH = "govind.driver.extraClassPath";
	public static final String DRIVER_EXTRA_OPTIONS = "govind.driver.extraJavaOptions";
	//native library path
	public static final String DRIVER_EXTRA_LIBRARY_PATH = "govind.driver.extraLibraryPath";

	/** configuration keys for executor*/
	public static final String EXECUTOR_MEMORY = "govind.executor,memory";
	public static final String EXECUTOR_EXTRA_CLASSPATH = "govind.executor.extraClassPath";
	public static final String EXECUTOR_EXTRA_OPTIONS = "govind.executor.extraJavaOptions";
	public static final String EXECUTOR_EXTRA_LIBRARY_PATH = "govind.executor.extraLibraryPath";
	public static final String EXECUTOR_CORES = "govind.executor.cores";

	/** 当启动一个子进程时，配置打印日志的Logger名称对应的key */
	public static final String CHILD_PROCESS_LOGGER_NAME = "govind.launcher.childProcessLoggerName";
	/** 当子进程启动时，连接Launcher Server的超时时间对应的key，单位ms */
	public static final String CHILD_CONNECTION_TIMEOUT = "govind.launcher.childConnectionTimeout";

	public static final String DEFAULT_MEM = "1g";
	public static final String DEFAULT_PROPERTIES_FILE = "govind-defaults.conf";
	public static final String ENV_SPARK_HOME = "SPARK_HOME";
	public static final String ENV_SPARK_ASSEMBLY = "ENV_SPARK_ASSEMBLY";
	/**
	 * Name of the app resource used to identify the PySpark shell. The command line parser expects
	 * the resource name to be the very first argument to spark-submit in this case.
	 *
	 * NOTE: this cannot be "pyspark-shell" since that identifies the PySpark shell to SparkSubmit
	 * (see java_gateway.py), and can cause this code to enter into an infinite loop.
	 */
	static final String PYSPARK_SHELL = "pyspark-shell-main";

	/**
	 * This is the actual resource name that identifies the PySpark shell to SparkSubmit.
	 */
	static final String PYSPARK_SHELL_RESOURCE = "pyspark-shell";

	/**
	 * Name of the app resource used to identify the SparkR shell. The command line parser expects
	 * the resource name to be the very first argument to spark-submit in this case.
	 *
	 * NOTE: this cannot be "sparkr-shell" since that identifies the SparkR shell to SparkSubmit
	 * (see sparkR.R), and can cause this code to enter into an infinite loop.
	 */
	static final String SPARKR_SHELL = "sparkr-shell-main";

	/**
	 * This is the actual resource name that identifies the SparkR shell to SparkSubmit.
	 */
	static final String SPARKR_SHELL_RESOURCE = "sparkr-shell";
}
