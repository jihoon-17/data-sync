package cn.com.daikin.writer.consume;

import cn.com.daikin.common.constant.Constants;
import cn.com.daikin.common.kafka.DataVo;
import cn.com.daikin.common.util.DatabaseConfig;
import cn.com.daikin.common.util.DateUtil;
import cn.com.daikin.writer.config.KafkaConsumerConfig;
import cn.com.daikin.writer.util.JdbcUtil;
import cn.com.daikin.writer.util.StringUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.*;

/**
 * 消费kafka数据写入mysql
 *
 * @author jihoon
 */
@Slf4j
@Component
public class MacData2MysqlConsume implements consume {

    @Override
    public void consume() {

        log.info("mysql consume start...");
        Properties props = KafkaConsumerConfig.getProperties(Constants.MYSQL_CONSUMER_GROUP);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(Constants.MYSQL_TOPIC));

        try {
            // 连接池
            DataSource dataSource = DatabaseConfig.getDataSource(Constants.MYSQL_JDBCURL, Constants.MYSQL_USERNAME, Constants.MYSQL_PASSWORD);
            log.info("mysql JdbcUtil.getConnection....................");

            try (Connection conn = dataSource.getConnection()) {

                // 禁用自动提交
                conn.setAutoCommit(false);
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {

                        //获取业务表数据
                        DataVo dataVo = JSONUtil.toBean(record.value(), DataVo.class);
                        JSONArray jsonArray = JSONUtil.parseArray(dataVo.getMsg());

                        //判空
                        if (jsonArray.isEmpty()) {
                            continue;
                        }

                        //处理表字段 select+id+,+isdeleted+from+address__c
                        String columns = dataVo.getColumns();
                        String tablename = dataVo.getTargettablename();

                        List<String> fields = StringUtil.extractFields(columns);

                        try (PreparedStatement preparedStatement = conn.prepareStatement(JdbcUtil.buildReplaceSql(tablename, fields))) {

                            for (int i = 0; i < jsonArray.size(); i++) {

                                JSONObject jsonObject = jsonArray.getJSONObject(i);

                                //判断是否是删除数据
                                String isdeleted = jsonObject.getStr("IsDeleted");
                                if (isdeleted.equals(Constants.TRUE)) {
                                    log.info("MacData2MysqlConsume isdelete is true,id:{}", jsonObject.getStr("Id"));
                                    continue;
                                }

                                for (int j = 0; j < fields.size(); j++) {

                                    //非空处理
                                    Object obj = jsonObject.get(fields.get(j));
                                    String value = Objects.isNull(obj) ? Constants.NULL : obj.toString().replace(Constants.t, Constants.EMPTY_STR);

                                    if (Objects.nonNull(value)) {
                                        //对value做去\n和\t处理
                                        value = value.replaceAll(Constants.n, Constants.EMPTY_STR).replaceAll(Constants.t, Constants.EMPTY_STR)
                                                .replaceAll(Constants.r, Constants.EMPTY_STR).replaceAll(Constants.SPACE_STR, Constants.EMPTY_STR);

                                        //2024-10-21T06:11:43.000+0000 若是时间字段需要转换成北京时间
                                        value = StringUtil.isUtcTime(value) ? DateUtil.substractHour(value.replace("T", " ").replace(".000+0000", ""), Constants.eight) : value;
                                    }

                                    preparedStatement.setObject(j + Constants.one, value);

                                }
                                preparedStatement.getMetaData();
                                preparedStatement.addBatch();

                            }

                            preparedStatement.executeBatch();

                        } catch (Exception e) {
                            log.error("MacData2MysqlConsume PreparedStatement error:", e);
                        }

                        conn.commit();

                        log.info("table data upsert mysql success,data size:{}", jsonArray.size());

                    }

                }


            } catch (Exception e) {

                log.error("MacData2MysqlConsume insert error:", e);
            }


        } catch (Exception e) {
            log.error("MacData2MysqlConsume error:", e);
        } finally {
            consumer.close();
        }


    }


    public static void main(String[] args) {


    }


}
