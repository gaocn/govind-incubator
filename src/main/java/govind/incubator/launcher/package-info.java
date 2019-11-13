/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-7
 *
 * 用于启动Spark应用程序的工具类库。
 *
 * 该类库使得采用程序方式启动Spark应用程序成为可能，并且该类库的唯一入口为：
 * {@link govind.incubator.launcher.GovindLauncher}。
 *
 * （1）采用{@link govind.incubator.launcher.GovindLauncher#startApplication(
 * govind.incubator.launcher.GovindAppHandle.Listener...)}方法启动应用程序
 * 同时可以指定一个监听器用于控制应用程序的状态。
 *
 * <pre>
 *  {@code
 *  	public class MyLauncher {
 *		  public static void main(String[] args) throws Exception {
 *		    SparkAppHandle handle = new SparkLauncher()
 *		      .setAppResource("/my/app.jar")
 *		      .setMainClass("my.spark.app.Main")
 *		      .setMaster("local")
 *		      .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *		      .startApplication();
 *		      // Use handle API to monitor / control application.
 *		  }
 *		}
 *  }
 * </pre>
 *
 *（2）若要启动一个子进程，可以通过{@link govind.incubator.launcher.GovindLauncher#launch()}
 * 方法实现。
 *
 * <pre>
 *  {@code
 *  	public class MyLauncher {
 *		  public static void main(String[] args) throws Exception {
 *		    Process process = new SparkLauncher()
 *		      .setAppResource("/my/app.jar")
 *		      .setMainClass("my.spark.app.Main")
 *		      .setMaster("local")
 *		      .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
 *		      .launch();
 *		      // Use handle API to monitor / control application.
 *		      process.waitFor();
 *		  }
 *		}
 *  }
 * </pre>
 *
 * 注意：采用上述方式需要调用者自己维护该子进程，包括处理器输出以避免产生死锁。
 *
 */
package govind.incubator.launcher;