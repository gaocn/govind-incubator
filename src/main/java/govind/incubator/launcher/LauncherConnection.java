package govind.incubator.launcher;

import govind.incubator.launcher.LauncherProtocol.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;

import static govind.incubator.launcher.util.CommandBuilderUtils.checkState;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-8
 *
 * Encapsulates a connection between a launcher server and client. This
 * takes care of the communication (sending and receiving messages), while
 * processing of messages is left for the implementations.
 *
 */
@Slf4j
abstract public class LauncherConnection implements Closeable, Runnable{
	final Socket socket;
	final ObjectOutputStream out;
	volatile boolean closed;

	public LauncherConnection(Socket socket) throws IOException {
		this.socket = socket;
		this.out = new ObjectOutputStream(socket.getOutputStream());
		this.closed = false;
	}

	protected abstract void handle(Message msg) throws IOException;

	protected synchronized void send(Message msg) throws IOException {
		try {
			checkState(!closed,"disconnect");
			out.writeObject(msg);
			out.flush();
		} catch (Exception e) {
			log.error("发送消息时异常：{}", e);
			try {
				close();
			} catch (Exception unused) {
				//NOP
			}
			throw e;
		}
	}

	@Override
	public void run() {
		try {
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			while (!closed) {
				Message msg = (Message) ois.readObject();
				handle(msg);
			}
		} catch (EOFException e) {
			//远端关闭连接
			try {
				close();
			} catch (Exception unused) {
				//NOP
			}
		} catch (Exception e) {
			if (!closed) {
				log.error("处理接收消息时异常：{}", e);
				//远端关闭连接
				try {
					close();
				} catch (Exception unused) {
					//NOP
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			synchronized (this) {
				if (!closed) {
					closed = true;
					socket.close();
				}
			}
		}
	}
}
