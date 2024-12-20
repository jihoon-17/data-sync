package cn.com.xx.writer.config;

import cn.com.xx.common.constant.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerConfig {

    public static Properties getProperties(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER_LIST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 关闭自动提交
        /**
         * 1.如果存在已经提交的offest时,不管设置为earliest 或者latest 都会从已经提交的offest处开始消费
         * 如果不存在已经提交的offest时,earliest 表示从头开始消费,latest 表示从最新的数据消费,也就是新产生的数据.
         * none topic各分区都存在已提交的offset时，从提交的offest处开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         *
         * 2.topic中已有分组消费数据，新建其他分组ID的消费者时，之前分组提交的offset对新建的分组消费不起作用。
         *
         * 3.不更改group.id，只是添加了config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");，
         * consumer不会从最开始的位置消费 ，只要不更改消费组，只会从上次消费结束的地方继续消费；
         * 同理不更改group.id，latest也不会只消费最新的数据，只要不更改消费组，只会从上次消费结束的地方继续消费

         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的偏移量开始消费
        return props;
    }
}
