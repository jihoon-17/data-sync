package cn.com.xx.reader.util;

import cn.com.xx.reader.constant.Constant;
import cn.hutool.json.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.*;

@Slf4j
public class HttpUtil {

    public static final String UTF8_ENCODE = "UTF-8";

    /**
     * application/json
     */
    public static final String APPLICATION_JSON = "application/json";

    public static final String WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxx";


    static final String LOGINURL     = "https://login.salesforce.com";
    static final String GRANTSERVICE = "/services/oauth2/token?grant_type=password";

    static final String USERNAME     = "xxx";
    static final String PASSWORD     = "xxx";
    static final String CLIENTID     = "xxx";
    static final String CLIENTSECRET = "xxx";


    private static final int MAX_TOTAL_CONNECTIONS = 20;
    private static final int MAX_CONNECTIONS_PER_ROUTE = 10;
    private static final int SOCKET_TIMEOUT = 15000; // 15秒
    private static final int CONNECT_TIMEOUT = 15000; // 15秒

    /**
     * http链接池
     * @return
     */
    public static CloseableHttpClient createCustomPoolingHttpClient() {
        // 创建连接管理器
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();

        // 设置最大连接数
        connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);

        // 设置每个路由的最大连接数
        connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);

        // 创建请求配置
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(SOCKET_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                .build();

        //设置代理
        HttpHost proxy = new HttpHost("xxx.xx.xx.xxx", 9000);

        // 创建 HttpClient 实例
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .setProxy(proxy)
                .build();

        return httpClient;
    }


    public static String post() throws UnsupportedEncodingException {
        HttpClient httpclient = HttpClientBuilder.create().build();

        // Assemble the login request URL
        String loginURL = LOGINURL +
                GRANTSERVICE +
                "&client_id=" + CLIENTID +
                "&client_secret=" + CLIENTSECRET +
                "&username=" + USERNAME +
                "&password=" + PASSWORD;
        HttpPost httpPost = new HttpPost(loginURL);
        String getResult = null;
        try {
            // Login requests must be POSTs

            HttpResponse response = null;


            // Execute the login POST request
            response = httpclient.execute(httpPost);


            // verify response is HTTP OK
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                System.out.println("Error authenticating to Force.com: "+statusCode);
                return null;
            }

                getResult = EntityUtils.toString(response.getEntity());

            } catch (IOException ioException) {
                ioException.printStackTrace();
            }

            httpPost.releaseConnection();

        return getResult;
    }

    public static String post(String url, String body){
        HttpClient httpclient = HttpClientBuilder.create().build();

        // Login requests must be POSTs
        HttpPost httpPost = new HttpPost(url);

        HttpResponse response = null;
        String getResult = null;
        try {

            StringEntity stringEntity = new StringEntity(body, UTF8_ENCODE);
            stringEntity.setContentType(APPLICATION_JSON);
            stringEntity.setContentEncoding(UTF8_ENCODE);
            httpPost.setEntity(stringEntity);

            // Execute the login POST request
            response = httpclient.execute(httpPost);
        } catch (ClientProtocolException cpException) {
            cpException.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        // verify response is HTTP OK
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            System.out.println("Error authenticating to Force.com: "+statusCode);
            return null;
        }

        try {
            getResult = EntityUtils.toString(response.getEntity());
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        httpPost.releaseConnection();

        return getResult;
    }

    public static String getBody(InputStream inputStream) {
        String result = "";
        try {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(inputStream)
            );
            String inputLine;
            while ( (inputLine = in.readLine() ) != null ) {
                result += inputLine;
                result += "\n";
            }
            in.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return result;
    }


    public static String webhookNotice(String server, String errMsg, String msgId){

        JSONObject errJson = new JSONObject();
        String content = "**标题:服务状态提醒,无需处理。**\n" +
                "服务:<font color=\"comment\">" + server + "</font>\n" +
                "状态:<font color=\"info\">" + errMsg + "</font>\n" +
                "id:<font color=\"comment\">" + msgId + "</font>\n" //+"处理人:<font color=\"comment\"> 阳光彩虹小白马 </font>"
                ;
        JSONObject contentJson = new JSONObject();
        contentJson.putOnce("content", content);
        errJson.putOnce("msgtype", "markdown");
        errJson.putOnce("markdown", contentJson);

        return errJson.toString();

    }

    // Query Leads using REST HttpGet
    public static String queryLeads(CloseableHttpClient httpClient,String QuerySqlFull, String baseUri, Header oauthHeader) throws IOException {
        System.out.println("\n_______________ START QUERY _______________");
        String response_string = "";

        CloseableHttpResponse response = null;

        try {

            String uri = "";
            if(baseUri.contains("-") && (baseUri.contains("00") || baseUri.contains("/query/") )){//走nextrecord url
                uri = baseUri;

            }else{
                uri = baseUri + "/queryAll?q=" + QuerySqlFull;
            }

            System.out.println("Query URL: " + uri);
            HttpGet httpGet = new HttpGet(uri);
            httpGet.addHeader(oauthHeader);
            httpGet.addHeader(Constant.prettyPrintHeader);
            // Make the request.
            response = httpClient.execute(httpGet);

            // Process the result
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                response_string = EntityUtils.toString(response.getEntity());

            } else {
                log.info("Query was unsuccessful. Status code returned is " + statusCode);
                log.info("An error has occured. Http status: " + response.getStatusLine().getStatusCode());
                HttpUtil.post(HttpUtil.WEBHOOK_URL,HttpUtil.getBody(response.getEntity().getContent()));
                log.info(HttpUtil.getBody(response.getEntity().getContent()));
            }

        } catch (Exception e) {
            log.error("get mac data http get error:",e);
        } finally {

            response.close();

        }

        System.out.println("\n_______________ END QUERY _______________");
        return response_string;
    }



    public static void main(String[] args) {

        try (CloseableHttpClient httpClient = createCustomPoolingHttpClient()) {
            // 创建 HTTP GET 请求
            HttpGet httpGet = new HttpGet("http://example.com");

            // 执行请求
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                // 处理响应
                if (response.getStatusLine().getStatusCode() == 200) {
                    String responseContent = EntityUtils.toString(response.getEntity());
                    System.out.println("Response: " + responseContent);
                } else {
                    System.out.println("Failed to get response. Status code: " + response.getStatusLine().getStatusCode());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }



}
