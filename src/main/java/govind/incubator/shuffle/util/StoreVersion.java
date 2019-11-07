package govind.incubator.shuffle.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 */
public class StoreVersion {
	static final byte[] KEY = "StoreVersion".getBytes(Charsets.UTF_8);

	public final int major;
	public final int minor;

	@JsonCreator
	public StoreVersion(@JsonProperty("major") int major, @JsonProperty("minor") int minor) {
		this.major = major;
		this.minor = minor;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(major, minor);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StoreVersion) {
			StoreVersion sv = (StoreVersion) obj;
			return Objects.equal(major, sv.major)
					&& Objects.equal(minor, sv.minor);
		}
		return false;
	}
}
