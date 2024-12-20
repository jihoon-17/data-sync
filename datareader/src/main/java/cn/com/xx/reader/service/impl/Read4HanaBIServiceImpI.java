package cn.com.xx.reader.service.impl;

import cn.com.xx.common.constant.Constants;
import cn.com.xx.common.kafka.DataVo;
import cn.com.xx.common.util.DateUtil;
import cn.com.xx.reader.constant.Constant;
import cn.com.xx.reader.entity.DataSyncRecords;
import cn.com.xx.reader.entity.MacTableConfig;
import cn.com.xx.reader.mapper.DataSyncRecordsMapper;
import cn.com.xx.reader.mapper.MacTableConfigMapper;
import cn.com.xx.reader.produce.Producer;
import cn.com.xx.reader.service.ReaderService;
import cn.com.xx.reader.util.HttpUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 *读取mac数据类
 * 存到data-for-hana kafka topic中
 * @author zhixun
 */
@Slf4j
@Service("read4HanaBIService")
public class Read4HanaBIServiceImpI implements ReaderService {

    private String baseUri;
    private String querySQL;
    private String maxLastModityDate = "";
    private Header oauthHeader;
    private Boolean done;
    private String nextRecordsUrl;

    SimpleDateFormat sdf_mm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Resource
    DataSyncRecordsMapper dataSyncRecordsMapper;

    @Resource
    MacTableConfigMapper macTableConfigMapper;

    @Override
    public void read(String targetSchema, String tablename) throws Exception {

            boolean isQueryLimitEmpty = false;

            //获取最新一条的执行记录
            DataSyncRecords dataSyncRecords = dataSyncRecordsMapper.findOneBySchemaAndTable(targetSchema, tablename);
            String lastModifiedDate = "";
            if(Objects.nonNull(dataSyncRecords)){
                lastModifiedDate = sdf_mm.format(dataSyncRecords.getLastmodifieddate()).replace("T"," ");
            }

            lastModifiedDate = lastModifiedDate.equals("") ? "" : DateUtil.substractHour(lastModifiedDate, Constants._eight);
            log.info(targetSchema +"-"+ tablename+" maxlastModityDate:{}",lastModifiedDate);

            //登录salesforce
            String getResult = HttpUtil.post();
            JSONObject jsonObject = (JSONObject) new JSONTokener(getResult).nextValue();
            String loginAccessToken = jsonObject.getString("access_token");
            String  loginInstanceUrl = jsonObject.getString("instance_url");
            baseUri = loginInstanceUrl + Constant.REST_ENDPOINT + Constant.API_VERSION ;
            oauthHeader = new BasicHeader("Authorization", "OAuth " + loginAccessToken) ;

            //获取表配置
            MacTableConfig config = macTableConfigMapper.findQuerySqlBySchemaAndTable(targetSchema, tablename);

            if(Objects.isNull(config)){
                log.error("表配置不存在，请检查表名是否正确targetSchema:{},tablename:{}",targetSchema,tablename);
                return;
            }

            querySQL = config.getQuerysql();

            Boolean isFirstRunFlag = true;
            try (CloseableHttpClient httpClient = HttpUtil.createCustomPoolingHttpClient()){

                while (!isQueryLimitEmpty){

                    String querySqlFull = querySQL;
                    //判断是否为初次读取该表
                    if(isFirstRunFlag && (Objects.isNull(lastModifiedDate) || lastModifiedDate.equals(""))){
                        querySqlFull =  querySqlFull + Constant.queryFilter + "1900-01-01T00%3A00%3A00.000%2B0000";
                    }else if(isFirstRunFlag && Objects.nonNull(lastModifiedDate)){

                        lastModifiedDate = lastModifiedDate.replaceAll(" ","T").replaceAll(":","%3A");

                        querySqlFull = querySqlFull + Constant.queryFilter + lastModifiedDate+".000%2B0000";

                    }else{

                        querySqlFull = querySqlFull + Constant.queryFilter + URLEncoder.encode(maxLastModityDate,"utf-8");

                    }


                    querySqlFull = querySqlFull + Constant.queryOrder;
                    querySqlFull = querySqlFull + Constant.queryLimit;

                    log.info("querySqlFull:{}",querySqlFull);
                    //读取源数据
                    String response_string = "";
                    if(Objects.nonNull(done) && !done){

                        response_string = HttpUtil.queryLeads(httpClient,querySqlFull,loginInstanceUrl + nextRecordsUrl, oauthHeader);

                    }else{
                        response_string = HttpUtil.queryLeads(httpClient,querySqlFull,baseUri, oauthHeader);
                    }

                    org.json.JSONObject json = new org.json.JSONObject(response_string);
                    done = json.getBoolean("done");

                    JSONArray j = json.getJSONArray("records");
                    boolean nonEmpty = j.length()>Constants.zero;

                    if(nonEmpty){

                        log.info("Read4HanaBIServiceImpI done:{},records size:{}",done,j.length());
                        if(!done){//循环获取下一页的数据直到done为true,说明已获取到最新的数据
                            nextRecordsUrl = json.getString("nextRecordsUrl");
                            log.info("Read4HanaBIServiceImpI nextRecordsUrl:{}",nextRecordsUrl);

                        }

                        maxLastModityDate = json.getJSONArray("records").getJSONObject(j.length() -1 ).getString("LastModifiedDate");

                    }
                    log.info("Read4HanaBIServiceImpI maxLastModityDate:{},nonEmpty:{}",maxLastModityDate,nonEmpty);
                    //生产者: 数据发送到kafka
                    if(nonEmpty){
                        Producer.send(Constants.HANA_TOPIC, JSONUtil.toJsonStr(new DataVo(j.toString(), querySQL, config.getTargettablename())));
                        log.info("hana send data to kafka success data size:{}",j.length());
                    }

                    isFirstRunFlag = false;
                    //判断是否还有数据
                    isQueryLimitEmpty =  done ? true : false;

                    //更新执行记录
                    if(done && nonEmpty){
                        dataSyncRecordsMapper.insertRecord(new DataSyncRecords(targetSchema,tablename,
                                sdf_mm.parse(DateUtil.substractHour(maxLastModityDate.replace("T"," ").replace(".000+0000",""),Constants.eight)),
                                new Date()));
                    }


                }


            } catch (Exception e) {
                log.error("Read4HanaBIServiceImpI error:",e);
            }

    }
}
