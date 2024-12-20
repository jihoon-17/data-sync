package cn.com.xx.writer.consume;

import cn.com.xx.common.constant.Constants;
import cn.com.xx.common.kafka.DataVo;
import cn.com.xx.common.util.DateUtil;
import cn.com.xx.writer.config.KafkaConsumerConfig;
import cn.com.xx.writer.util.JdbcUtil;
import cn.com.xx.writer.util.StringUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.*;

/**
 * 消费kafka数据写入hana
 * @author zhixun
 */
@Slf4j
@Component
public class MacData2HanaConsume implements consume{

    @Override
    public void consume() {


        log.info("hana consume start...");

        Properties props = KafkaConsumerConfig.getProperties(Constants.HANA_CONSUMER_GROUP);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(Constants.HANA_TOPIC));

        Connection conn = null;
        PreparedStatement preparedStatement = null;

        try {

            conn = JdbcUtil.getConnection(Constants.HANA_JDBCURL, Constants.HANA_USERNAME, Constants.HANA_PASSWORD);
            // 禁用自动提交
            conn.setAutoCommit(false);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {

                    //获取业务表数据
                    DataVo dataVo = JSONUtil.toBean(record.value(), DataVo.class);
                    JSONArray jsonArray = JSONUtil.parseArray(dataVo.getMsg());

                    //判空
                    if(jsonArray.isEmpty()){
                        continue;
                    }

                    //处理表字段 select+id+,+isdeleted+from+address__c
                    String columns = dataVo.getColumns();
                    String tablename = dataVo.getTargettablename();

                    List<String> fields = StringUtil.extractFields(columns);

                    preparedStatement = conn.prepareStatement(JdbcUtil.buildUpsertSql(tablename,fields));

                    log.info("upsert sql:{}",JdbcUtil.buildUpsertSql(tablename,fields));

                    for (int i = 0; i < jsonArray.size(); i++) {

                        JSONObject jsonObject = jsonArray.getJSONObject(i);
                        for (int j=0; j < fields.size(); j++){

                            //非空处理
                            Object obj = jsonObject.get(fields.get(j));
                            String value  = Objects.isNull(obj) ? Constants.NULL :obj.toString().replace(Constants.t,Constants.EMPTY_STR);

                            if(Objects.nonNull(value)){
                                //对value做去\n和\t处理
                                value = value.replaceAll(Constants.n,Constants.EMPTY_STR).replaceAll(Constants.t,Constants.EMPTY_STR)
                                        .replaceAll(Constants.r,Constants.EMPTY_STR).replaceAll(Constants.SPACE_STR,Constants.EMPTY_STR);

                                //2024-10-21T06:11:43.000+0000 若是时间字段需要转换成北京时间
                                value = StringUtil.isUtcTime(value) ? DateUtil.substractHour(value.replace("T"," ").replace(".000+0000",""),Constants.eight) : value;
                            }

//                            log.info("upsert field:{},value:{}",fields.get(j),value);
                            preparedStatement.setObject(j + Constants.one, value);

                        }
                        preparedStatement.getMetaData();
                        preparedStatement.addBatch();

                    }

                    preparedStatement.executeBatch();
                    // 提交偏移量
                    consumer.commitSync();
                    conn.commit();

                    log.info("table data upsert hana success,data size:{}",jsonArray.size());

                }
            }
        } catch (Exception e) {
            log.error("MacData2HanaConsume error:",e);
        } finally {

            consumer.close();
            JdbcUtil.close(preparedStatement, conn);
            log.info("HanaConsume close,need restart app!!!");
        }


    }


    public static void main(String[] args) {

        String jsonStr = "{\"msg\":\"[{\\\"BillingAddressFlag__c\\\":false,\\\"LastModifiedDate\\\":\\\"2024-10-21T06:11:46.000+0000\\\",\\\"ShiptoPhone__c\\\":\\\"***********\\\",\\\"IsActive__c\\\":false,\\\"ShippingAddressFlag__c\\\":false,\\\"Name\\\":\\\"仓库\\\",\\\"Region__c\\\":\\\"a092800000OF1erAAD\\\",\\\"CreatedById\\\":\\\"0050K00000BbQTzQAN\\\",\\\"ShiptoName__c\\\":\\\"黄**\\\",\\\"LocationEncrypted__c\\\":null,\\\"Description__c\\\":null,\\\"DeliveryLimited__c\\\":null,\\\"Location__c\\\":\\\"五桂山长命水大街44号威斯达工业园厂房b栋3楼\\\",\\\"Shiptoadd1Encrypted__c\\\":\\\"7Fj7l6lNXCZg56AK2acFocX5Vcp5be28T1mP6HQl56U=\\\",\\\"AliDataId__c\\\":\\\"1297925287960817665\\\",\\\"ShiptoNameEncrypted__c\\\":\\\"rjIp0K5WVhKS1VSumchfnQ==\\\",\\\"IsDeleted\\\":false,\\\"IsDefault__c\\\":false,\\\"Partner__c\\\":\\\"0010K00002YcaZWQAZ\\\",\\\"SystemModstamp\\\":\\\"2024-10-21T06:11:46.000+0000\\\",\\\"PIPLMigrationStatus__c\\\":\\\"To Do\\\",\\\"Shiptoaddress1__c\\\":\\\"**********\\\",\\\"ShiptoPhoneEncrypted__c\\\":\\\"kVATekM2Cz0ACDl4GLBtFA==\\\",\\\"CreatedDate\\\":\\\"2024-10-21T06:11:43.000+0000\\\",\\\"LastActivityDate\\\":null,\\\"attributes\\\":{\\\"type\\\":\\\"Address__c\\\",\\\"url\\\":\\\"/services/data/v32.0/sobjects/Address__c/a0SJ300000CYosnMAD\\\"},\\\"ExternalId__c\\\":null,\\\"Id\\\":\\\"a0SJ300000CYosnMAD\\\",\\\"LastModifiedById\\\":\\\"0050K00000BbQTzQAN\\\"},{\\\"BillingAddressFlag__c\\\":false,\\\"LastModifiedDate\\\":\\\"2024-10-26T08:58:23.000+0000\\\",\\\"ShiptoPhone__c\\\":null,\\\"IsActive__c\\\":false,\\\"ShippingAddressFlag__c\\\":false,\\\"Name\\\":\\\"浙江中建材料设备工程有限公司\\\",\\\"Region__c\\\":\\\"a092800000OF1ctAAD\\\",\\\"CreatedById\\\":\\\"00528000005opwtAAA\\\",\\\"ShiptoName__c\\\":null,\\\"LocationEncrypted__c\\\":null,\\\"Description__c\\\":null,\\\"DeliveryLimited__c\\\":null,\\\"Location__c\\\":\\\"拱墅区德胜路289号松泰文创园1号楼\\\",\\\"Shiptoadd1Encrypted__c\\\":null,\\\"AliDataId__c\\\":\\\"1044285380670521344\\\",\\\"ShiptoNameEncrypted__c\\\":null,\\\"IsDeleted\\\":false,\\\"IsDefault__c\\\":true,\\\"Partner__c\\\":\\\"00128000016mYK0AAM\\\",\\\"SystemModstamp\\\":\\\"2024-10-26T08:58:23.000+0000\\\",\\\"PIPLMigrationStatus__c\\\":\\\"Phase3 Completed\\\",\\\"Shiptoaddress1__c\\\":null,\\\"ShiptoPhoneEncrypted__c\\\":null,\\\"CreatedDate\\\":\\\"2020-08-06T09:13:49.000+0000\\\",\\\"LastActivityDate\\\":null,\\\"attributes\\\":{\\\"type\\\":\\\"Address__c\\\",\\\"url\\\":\\\"/services/data/v32.0/sobjects/Address__c/a0S0K00000Suo5dUAB\\\"},\\\"ExternalId__c\\\":null,\\\"Id\\\":\\\"a0S0K00000Suo5dUAB\\\",\\\"LastModifiedById\\\":\\\"00528000005opwtAAA\\\"},{\\\"BillingAddressFlag__c\\\":false,\\\"LastModifiedDate\\\":\\\"2024-11-07T07:35:12.000+0000\\\",\\\"ShiptoPhone__c\\\":\\\"***********\\\",\\\"IsActive__c\\\":false,\\\"ShippingAddressFlag__c\\\":true,\\\"Name\\\":\\\"收货地址\\\",\\\"Region__c\\\":\\\"a092800000OF1dOAAT\\\",\\\"CreatedById\\\":\\\"0050K000008lNSbQAM\\\",\\\"ShiptoName__c\\\":\\\"陈**\\\",\\\"LocationEncrypted__c\\\":null,\\\"Description__c\\\":null,\\\"DeliveryLimited__c\\\":\\\"350521197910020539\\\",\\\"Location__c\\\":\\\"福建省泉州市丰泽区洛江区河市镇大华蓄电池园区货管家仓储有限公\\\",\\\"Shiptoadd1Encrypted__c\\\":\\\"ysPZ4NV7vl0JEKUclP7ak0QzCX2tSNC9ZeKSb/spvqE=\\\",\\\"AliDataId__c\\\":\\\"1044285380909596673\\\",\\\"ShiptoNameEncrypted__c\\\":\\\"KOf4+HCNXlydKXQshXXnXA==\\\",\\\"IsDeleted\\\":false,\\\"IsDefault__c\\\":false,\\\"Partner__c\\\":\\\"0010K00001rQL3uQAG\\\",\\\"SystemModstamp\\\":\\\"2024-11-07T07:35:12.000+0000\\\",\\\"PIPLMigrationStatus__c\\\":\\\"Phase3 Completed\\\",\\\"Shiptoaddress1__c\\\":\\\"**********\\\",\\\"ShiptoPhoneEncrypted__c\\\":\\\"WgfKzw7Q3VtyAGGgfc4N6A==\\\",\\\"CreatedDate\\\":\\\"2018-09-04T10:08:14.000+0000\\\",\\\"LastActivityDate\\\":null,\\\"attributes\\\":{\\\"type\\\":\\\"Address__c\\\",\\\"url\\\":\\\"/services/data/v32.0/sobjects/Address__c/a0S0K00000F4tbEUAR\\\"},\\\"ExternalId__c\\\":null,\\\"Id\\\":\\\"a0S0K00000F4tbEUAR\\\",\\\"LastModifiedById\\\":\\\"0050K000008lNSbQAM\\\"},{\\\"BillingAddressFlag__c\\\":false,\\\"LastModifiedDate\\\":\\\"2024-11-07T07:36:36.000+0000\\\",\\\"ShiptoPhone__c\\\":\\\"***********\\\",\\\"IsActive__c\\\":false,\\\"ShippingAddressFlag__c\\\":true,\\\"Name\\\":\\\"收货地址\\\",\\\"Region__c\\\":\\\"a092800000OF1dOAAT\\\",\\\"CreatedById\\\":\\\"00528000006nvpmAAA\\\",\\\"ShiptoName__c\\\":\\\"陈**\\\",\\\"LocationEncrypted__c\\\":null,\\\"Description__c\\\":null,\\\"DeliveryLimited__c\\\":\\\"350521197910020539\\\",\\\"Location__c\\\":\\\"福建省泉州市丰泽区洛江区河市镇大华蓄电池园区货管家仓储有限公\\\",\\\"Shiptoadd1Encrypted__c\\\":\\\"ysPZ4NV7vl0JEKUclP7ak0QzCX2tSNC9ZeKSb/spvqE=\\\",\\\"AliDataId__c\\\":\\\"1044285380834099206\\\",\\\"ShiptoNameEncrypted__c\\\":\\\"KOf4+HCNXlydKXQshXXnXA==\\\",\\\"IsDeleted\\\":false,\\\"IsDefault__c\\\":false,\\\"Partner__c\\\":\\\"0012800001DzMXMAA3\\\",\\\"SystemModstamp\\\":\\\"2024-11-07T07:36:36.000+0000\\\",\\\"PIPLMigrationStatus__c\\\":\\\"Phase3 Completed\\\",\\\"Shiptoaddress1__c\\\":\\\"**********\\\",\\\"ShiptoPhoneEncrypted__c\\\":\\\"WgfKzw7Q3VtyAGGgfc4N6A==\\\",\\\"CreatedDate\\\":\\\"2019-04-10T06:41:08.000+0000\\\",\\\"LastActivityDate\\\":null,\\\"attributes\\\":{\\\"type\\\":\\\"Address__c\\\",\\\"url\\\":\\\"/services/data/v32.0/sobjects/Address__c/a0S0K00000KOUsAUAX\\\"},\\\"ExternalId__c\\\":null,\\\"Id\\\":\\\"a0S0K00000KOUsAUAX\\\",\\\"LastModifiedById\\\":\\\"00528000006nvpmAAA\\\"}]\",\"columns\":\"select+id+,+isdeleted+,+name+,+createddate+,+createdbyid+,+lastmodifieddate+,+lastmodifiedbyid+,+systemmodstamp+,+lastactivitydate+,+partner__c+,+billingaddressflag__c+,+description__c+,+externalid__c+,+isactive__c+,+isdefault__c+,+location__c+,+region__c+,+shippingaddressflag__c+,+shiptoaddress1__c+,+deliverylimited__c+,+shiptoname__c+,+shiptophone__c+,+alidataid__c+,+locationencrypted__c+,+piplmigrationstatus__c+,+shiptonameencrypted__c+,+shiptophoneencrypted__c+,+shiptoadd1encrypted__c+from+address__c\"}";

        DataVo dataVo = JSONUtil.toBean(jsonStr, DataVo.class);

        String columns = dataVo.getColumns();
        String tablename = columns.split("from+")[0];
        String column[] = columns.split("select+")[1].split("from");

        System.out.println(tablename);
        System.out.println(Arrays.toString(column));
        JSONArray jsonArray = JSONUtil.parseArray(dataVo.getMsg());


    }




}
