package cn.com.xx.writer.consume;

import cn.com.xx.common.constant.Constants;
import cn.com.xx.writer.config.ThreadPoolConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * kafka消费者入口
 * @author zhixun
 */
@Slf4j
@Service
public class KafkaConsume {

    @Resource
    MacData2RomaConsume macData2RomaConsume;

    @Resource
    MacData2HanaConsume macData2HanaConsume;

    @Resource
    MacData2MysqlConsume macData2MysqlConsume;

    @Autowired
    private ThreadPoolConfig threadPoolConfig;

    @PostConstruct
    public void consume_init(){

        threadPoolConfig.taskExecutor(Constants.STARROCKS_CONSUMER_GROUP).execute(() -> {

            macData2RomaConsume.consume();
        });

        threadPoolConfig.taskExecutor(Constants.HANA_CONSUMER_GROUP).execute(() -> {

            macData2HanaConsume.consume();

        });

        threadPoolConfig.taskExecutor(Constants.MYSQL_CONSUMER_GROUP).execute(() -> {

            macData2MysqlConsume.consume();

        });


    }


}
