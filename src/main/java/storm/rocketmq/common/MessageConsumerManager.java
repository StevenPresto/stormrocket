package storm.rocketmq.common;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.rocketmq.config.RocketMQConfig;
import storm.rocketmq.utils.FastBeanUtils;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public class MessageConsumerManager {

	private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerManager.class);
	private static DefaultMQPushConsumer pushConsumer;
	private static DefaultMQPullConsumer pullConsumer;

	MessageConsumerManager() {
	}

	public static MQConsumer getConsumerInstance(RocketMQConfig config, MessageListener listener,
	                                             Boolean isPushlet) throws MQClientException {
		LOG.info("Begin to init consumer,instanceName->{},configuration->{}",
				new Object[] { config.getInstanceName(), config });

		if (BooleanUtils.isTrue(isPushlet)) {
			pushConsumer = (DefaultMQPushConsumer) FastBeanUtils.copyProperties(config,
					DefaultMQPushConsumer.class);
			pushConsumer.setConsumerGroup(config.getGroupId());
			pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

			pushConsumer.subscribe(config.getTopic(), config.getTopicTag());
			if (listener instanceof MessageListenerConcurrently) {
				pushConsumer.registerMessageListener((MessageListenerConcurrently) listener);
			}
			if (listener instanceof MessageListenerOrderly) {
				pushConsumer.registerMessageListener((MessageListenerOrderly) listener);
			}
			return pushConsumer;
		} else {
			pullConsumer = (DefaultMQPullConsumer) FastBeanUtils.copyProperties(config,
					DefaultMQPullConsumer.class);
			pullConsumer.setConsumerGroup(config.getGroupId());
			return pullConsumer;
		}
	}
}
