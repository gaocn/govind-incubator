package govind.incubator.network.protocol;

import govind.incubator.network.buffer.ManagedBuffer;

import java.util.Objects;

public abstract class AbstractMessage implements Message {
	private final ManagedBuffer body;
	private final boolean isBodyInFrame;

	public AbstractMessage() {
		this(null, false);
	}

	public AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
		this.body = body;
		this.isBodyInFrame = isBodyInFrame;
	}

	@Override
	public ManagedBuffer body() {
		return body;
	}

	@Override
	public boolean isBodyInFrame() {
		return isBodyInFrame;
	}

	public boolean equals(AbstractMessage o) {
		return this.isBodyInFrame == o.isBodyInFrame && Objects.equals(body, o.body);
	}

}
