package cn.com.xx.writer.streamload;// Copyright (c) 2021 Beijing Dingshi Zongheng Technology Co., Ltd. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import cn.com.xx.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * This class is a java demo for starrocks stream load
 *
 * The pom.xml dependency:
 *
 *         <dependency>
 *             <groupId>org.apache.httpcomponents</groupId>
 *             <artifactId>httpclient</artifactId>
 *             <version>4.5.3</version>
 *         </dependency>
 *
 * How to use:
 *
 * 1 create a table in starrocks with any mysql client
 *
 * CREATE TABLE `stream_test` (
 *   `id` bigint(20) COMMENT "",
 *   `id2` bigint(20) COMMENT "",
 *   `username` varchar(32) COMMENT ""
 * ) ENGINE=OLAP
 * DUPLICATE KEY(`id`)
 * DISTRIBUTED BY HASH(`id`) BUCKETS 20;
 *
 *
 * 2 change the StarRocks cluster, db, user config in this class
 *
 * 3 run this class, you should see the following output:
 *
 * {
 *     "TxnId": 27,
 *     "Label": "39c25a5c-7000-496e-a98e-348a264c81de",
 *     "Status": "Success",
 *     "Message": "OK",
 *     "NumberTotalRows": 10,
 *     "NumberLoadedRows": 10,
 *     "NumberFilteredRows": 0,
 *     "NumberUnselectedRows": 0,
 *     "LoadBytes": 50,
 *     "LoadTimeMs": 151
 * }
 *
 * Attention:
 *
 * 1 wrong dependency version(such as 4.4) of httpclient may cause shaded.org.apache.http.ProtocolException
 *   Caused by: shaded.org.apache.http.ProtocolException: Content-Length header already present
 *     at shaded.org.apache.http.protocol.RequestContent.process(RequestContent.java:96)
 *     at shaded.org.apache.http.protocol.ImmutableHttpProcessor.process(ImmutableHttpProcessor.java:132)
 *     at shaded.org.apache.http.impl.execchain.ProtocolExec.execute(ProtocolExec.java:182)
 *     at shaded.org.apache.http.impl.execchain.RetryExec.execute(RetryExec.java:88)
 *     at shaded.org.apache.http.impl.execchain.RedirectExec.execute(RedirectExec.java:110)
 *     at shaded.org.apache.http.impl.client.InternalHttpClient.doExecute(InternalHttpClient.java:184)
 *
 *2 run this class more than once, the status code for http response is still ok, and you will see
 *  the following output:
 *
 * {
 *     "TxnId": -1,
 *     "Label": "39c25a5c-7000-496e-a98e-348a264c81de",
 *     "Status": "Label Already Exists",
 *     "ExistingJobStatus": "FINISHED",
 *     "Message": "Label [39c25a5c-7000-496e-a98e-348a264c81de"] has already been used.",
 *     "NumberTotalRows": 0,
 *     "NumberLoadedRows": 0,
 *     "NumberFilteredRows": 0,
 *     "NumberUnselectedRows": 0,
 *     "LoadBytes": 0,
 *     "LoadTimeMs": 0
 * }
 * 3 when the response statusCode is 200, that doesn't mean your stream load is ok, there may be still
 *   some stream problem unless you see the output with 'ok' message
 */

@Slf4j
@Component
public class StringStreamLoad {
    private final static String STARROCKS_HOST = "xxx.xxx.xxx.x";
    private final static String STARROCKS_USER = "xxx";//""root";
    private final static String STARROCKS_PASSWORD = "xxx";//
    private final static int STARROCKS_HTTP_PORT = 8040;

    public static String makeUUID(int len) {
        return UUID.randomUUID().toString().replaceAll("-", "").substring(0, len);
    }

    public String sendData(String content, String table) throws Exception {
        final String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
                STARROCKS_HOST,
                STARROCKS_HTTP_PORT,
                Constants.databaseName,//Constant.DATABASE
                table);


            HttpPut put = new HttpPut(loadUrl);
            StringEntity entity = new StringEntity(content, "UTF-8");
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(STARROCKS_USER, STARROCKS_PASSWORD));
            // the label header is optional, not necessary
            // use label header can ensure at most once semantics
            String value = "39c25a5c-7000-496e-a98e-" +makeUUID(12);
            put.setHeader("label", value);
            put.setEntity(entity);

            try (CloseableHttpResponse response = StreamLoadHttpClientPool.getHttpClient().execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }
                final int statusCode = response.getStatusLine().getStatusCode();
                // statusCode 200 just indicates that starrocks be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed, statusCode=%s load result=%s", statusCode, loadResult));
                }

                if(loadResult.contains("FAILED")){

//                    String msg = HttpUtil.webhookString("数据插入starrocks出错",loadResult,"");
//                    HttpUtil.post(HttpUtil.WEBHOOK_URL,msg);
                }

                log.info("stream load result:"+loadResult);
                return loadResult;
            }catch (Exception e){

                log.error("stream load sendData error:",e);
            }


        return null;
    }


     //连接池
     static class StreamLoadHttpClientPool {
        private static final int MAX_TOTAL_CONNECTIONS = 100; // 总连接数
        private static final int MAX_CONNECTIONS_PER_ROUTE = 20; // 每个路由的最大连接数

        private static final PoolingHttpClientConnectionManager connectionManager;
        private static final CloseableHttpClient httpClient;

        static {
            connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
            connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(10000) // 连接超时时间
                    .setSocketTimeout(10000) // 读取超时时间
                    .build();

            httpClient = HttpClients.custom()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
        }

        public static CloseableHttpClient getHttpClient() {
            return httpClient;
        }
    }


    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception {
        int id1 = 1;
        int id2 = 10;
        String id3 = "Simon";
        int rowNumber = 10;
        String oneRow = id1 + "\t" + id2 + "\t" + id3 + "\n";

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < rowNumber; i++) {
            stringBuilder.append(oneRow);
        }

        stringBuilder.deleteCharAt(stringBuilder.length() - 1);

        String loadData = stringBuilder.toString();
        log.info("loadData:"+loadData);
        StringStreamLoad starrocksStreamLoad = new StringStreamLoad();
//        starrocksStreamLoad.sendData(loadData);
    }
}
