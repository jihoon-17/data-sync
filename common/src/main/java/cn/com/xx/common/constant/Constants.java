package cn.com.xx.common.constant;

public class Constants {

    public static String userName = "xxxx";
    public static String passWord = "xxxx";
    public static String ipAddress = "xxx.xx.xxx.x";
    public static String databaseName = "xxx";
    public static String port = "9030";

    public static int zero = 0;
    public static int one = 1;
    public static int _eight = -8;
    public static int _five = -5;
    public static int eight = 8;

    /**
     * 1：\n:标识换行
     * 2：\t:标识空四个字符，想当于缩进，Tab键
     * 3：\n\t:标识换行，且缩进4个字符
     * 4:\r:标识回车（光标定位到行首位置），因此enter键可以看成“\r\n”
     */
    public static String N = "\\N"; //NULL
    public static String t = "\t";
    public static String n = "\n";
    public static String r = "\r";
    public static String SPACE_STR = " ";

    public static String EMPTY_STR = "";
    public static String NULL = null;

    public static String HANA_JDBCURL = "jdbc:sap://1xx.xx.xx.xx:xx/?currentschema=xx";;
    public static String HANA_USERNAME = "xxxx";
    public static String HANA_PASSWORD = "xxxx";


    public static String MYSQL_JDBCURL = "jdbc:mysql://xxx:3306/datatrans?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf8";
    public static String MYSQL_USERNAME = "xxxx";
    public static String MYSQL_PASSWORD = "xxxx*^r2skE";


    public static String BROKER_LIST = "xxx.xx.xx.xx:9092";

    public static String STARROCKS_TOPIC = "data-for-starrocks";//data-for-starrocks

    public static String HANA_TOPIC = "data-for-hana";

    public static String MYSQL_TOPIC = "data-for-mysql";

    public static String STARROCKS_CONSUMER_GROUP = "consume-for-roma";
    public static String HANA_CONSUMER_GROUP = "consume-for-hana";
    public static String MYSQL_CONSUMER_GROUP = "consume-for-mysql";
}
