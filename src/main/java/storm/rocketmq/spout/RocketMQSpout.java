package storm.rocketmq.spout;

import com.google.common.collect.MapMaker;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.rocketmq.common.MessagePushConsumer;
import storm.rocketmq.common.MessageStat;
import storm.rocketmq.config.RocketMQConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public class RocketMQSpout implements IRichSpout, MessageListenerOrderly {

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSpout.class);

	private SpoutOutputCollector collector;
	private TopologyContext context;

	private MessagePushConsumer consumer;
	private RocketMQConfig config;

	private BlockingQueue<Pair<MessageExt, MessageStat>> failureQueue = new LinkedBlockingQueue<Pair<MessageExt, MessageStat>>();
	private Map<String, Pair<MessageExt, MessageStat>> failureMsgs;

	public void setConfig(RocketMQConfig config) {
		this.config = config;
	}

	public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
		try {
			for (MessageExt msg : msgs) {
				MessageStat msgStat = new MessageStat();
				collector.emit(new Values(msg, msgStat), msg.getMsgId());
			}
		} catch (Exception e) {
			LOG.error("Failed to emit message {} in context {},caused by {} !", new Object[] { msgs,
					this.context.getThisTaskId(), e.getCause() });
			return ConsumeOrderlyStatus.ROLLBACK;
		}
		return ConsumeOrderlyStatus.SUCCESS;
	}

//	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//	                                                ConsumeConcurrentlyContext context) {
//		try {
//			for (MessageExt msg : msgs) {
//				MessageStat msgStat = new MessageStat();
//				collector.emit(new Values(msg, msgStat), msg.getMsgId());
//			}
//		} catch (Exception e) {
//			LOG.error("Failed to emit message {} in context {},caused by {} !", new Object[] { msgs,
//					this.context.getThisTaskId(), e.getCause() });
//			return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//		}
//		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//	}

	public void open(@SuppressWarnings("rawtypes") Map conf,
	                 TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
		this.context = context;
		this.failureMsgs = new MapMaker().makeMap();

		if (consumer == null) {
			try {
				config.setInstanceName(String.valueOf(context.getThisTaskId()));

				consumer = new MessagePushConsumer(config);
				consumer.start(this);
			} catch (Exception e) {
				LOG.error("Failed to init consumer!", e);
				e.printStackTrace();
			}
		}

	}

	public void close() {
		if (!failureMsgs.isEmpty()) {
			for (Map.Entry<String, Pair<MessageExt, MessageStat>> entry : failureMsgs.entrySet()) {
				Pair<MessageExt, MessageStat> pair = entry.getValue();
				LOG.warn("Failed to handle message {},message statics {} !",
						new Object[] { pair.getObject1(), pair.getObject2() });
			}
		}

		if (consumer != null) {
			consumer.shutdown();
		}
	}

	public void activate() {
		consumer.resume();
	}

	public void deactivate() {
		consumer.suspend();
	}

	public void nextTuple() {
		Pair<MessageExt, MessageStat> pair = null;
		try {
			pair = failureQueue.take();
		} catch (Exception e) {
			return;
		}
		if (pair == null) {
			return;
		}

		pair.getObject2().setElapsedTime();
		collector.emit(new Values(pair.getObject1(), pair.getObject2()), pair.getObject1()
				.getMsgId());

	}

	public void ack(Object msgId) {
		String mId = (String) msgId;
		failureMsgs.remove(mId);
	}

	public void fail(Object msgId) {
		handleFailure((String) msgId);
	}

	private void handleFailure(String msgId) {
		Pair<MessageExt, MessageStat> pair = failureMsgs.get(msgId);
		if (pair == null) {
			MessageExt msg;
			try {
				msg = consumer.getConsumer().viewMessage(msgId);
			} catch (Exception e) {
				LOG.error("Failed to get message {} from broker !", new Object[] { msgId }, e);
				return;
			}

			MessageStat stat = new MessageStat();
			pair = new Pair<MessageExt, MessageStat>(msg, stat);
			failureMsgs.put(msgId, pair);
			failureQueue.offer(pair);
		} else {
			int failureTime = pair.getObject2().getFailureTimes().incrementAndGet();
			if (config.getMaxFailTimes() < 0 || failureTime < config.getMaxFailTimes()) {
				failureQueue.offer(pair);
				return;
			} else {
				LOG.info("Failure too many times, skip message {} !", pair.getObject1());
				ack(msgId);
				return;
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("MessageExt", "MessageStat");
		declarer.declare(fields);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer.getConsumer();
	}
}
