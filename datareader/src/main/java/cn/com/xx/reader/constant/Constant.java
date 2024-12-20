package cn.com.xx.reader.constant;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

public class Constant {


    public static String REST_ENDPOINT = "/services/data" ;
    public static String API_VERSION = "/v45.0" ;
    public static String queryFilter = "+Where+LastModifiedDate%3E%3D";
    public static String queryFilter_ = "+Where+CreatedDate%3E";//"+Where+field%3D'ExpectedShipdate__c'+and+CreatedDate%3E"
    public static String queryOrder = "+Order+By+LastModifiedDate";
    public static String queryOrder_ = "+Order+By+CreatedDate";
    public static String queryLimit = "";//+Limit+1000
    public static Header prettyPrintHeader = new BasicHeader("X-PrettyPrint", "1");

}
