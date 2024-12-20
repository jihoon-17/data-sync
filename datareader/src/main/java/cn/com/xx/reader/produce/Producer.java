package cn.com.xx.reader.produce;

import cn.com.xx.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Kafka生产者
 */
@Slf4j
public class Producer {

    /**
     * 发送消息
     * @param topic
     * @param msg
     */
    public static void send(String topic, String msg) {

        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.BROKER_LIST); //这里的localhost可以改成机器名或ip
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "104857600"); // 设置最大请求大小为 100MB
        props.put("acks", "1");
        props.put("retries", 1);

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<>(props);

        try{

            producer.send(new ProducerRecord<String, String>(topic,msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        log.error("send message unsuccess:",e);
                    } else {
                        //发送消息
                        log.info("topicPartition offset:{}",metadata.toString());

                    }
                }
            });


        }catch (Exception e){

            log.error("send message error:",e);
        }finally {
            producer.close();
        }
    }

}
