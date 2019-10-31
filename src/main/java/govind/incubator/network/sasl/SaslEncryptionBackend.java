package govind.incubator.network.sasl;

import javax.security.sasl.SaslException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 */
public interface SaslEncryptionBackend {
	/**
	 * disposes of resources used by the backend
	 */
	void  dispose();

	/**
	 * 加密数据
	 * @param data
	 * @param offset
	 * @param len
	 * @return
	 * @throws SaslException
	 */
	byte[] wrap(byte[] data, int offset, int len)  throws SaslException;

	/**
	 * 解密数据
	 * @param data
	 * @param offset
	 * @param len
	 * @return
	 * @throws SaslException
	 */
	byte[] unwrap(byte[] data, int offset, int len) throws SaslException;
}
