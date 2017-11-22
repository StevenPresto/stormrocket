package storm.rocketmq.common;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public class MessageStat implements Serializable {
	private static final long serialVersionUID = 1277714452693486955L;

	private AtomicInteger     failureTimes     = new AtomicInteger(0);
	private long              elapsedTime      = System.currentTimeMillis();

	public MessageStat() {
		super();
	}

	public MessageStat(int failureTimes) {
		this.failureTimes = new AtomicInteger(failureTimes);
	}

	public void setElapsedTime() {
		this.elapsedTime = System.currentTimeMillis();
	}

	public AtomicInteger getFailureTimes() {
		return failureTimes;
	}

	public long getElapsedTime() {
		return elapsedTime;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
