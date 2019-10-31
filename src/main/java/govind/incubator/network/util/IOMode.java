package govind.incubator.network.util;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 */
public enum IOMode {
	NIO("NIO"), EPOLL("EPOLL");

	private String name;

	IOMode(String name) {
		this.name = name;
	}


}
