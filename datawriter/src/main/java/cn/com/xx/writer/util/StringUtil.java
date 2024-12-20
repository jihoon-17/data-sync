package cn.com.xx.writer.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    /**
     * 提取sqlquery中的字段
     * @param sqlQuery
     * @return
     */
    public static List<String> extractFields(String sqlQuery){

        Pattern pattern = Pattern.compile("\\+(\\w+)\\+");
        Matcher matcher = pattern.matcher(sqlQuery);

        // 存储提取的字段
        List<String> fields = new ArrayList<>();

        // 遍历匹配结果
        while (matcher.find()) {
            fields.add(matcher.group(1));
        }

        return fields;

    }

    /**
     * 检测是否为utc时间
     * 2024-10-21t06:11:43.000+0000
     * @param time
     * @return
     */
    public static boolean isUtcTime(String time) {
        try {
            boolean isUtc = (time.contains("T") && time.contains(".000+0000")) ? true : false;
            return isUtc;
        } catch (Exception e) {
            return false;
        }
    }


    public static void main(String[] args) {
        String sqlQuery = "select+id+,+isdeleted+from+address__c";

        List<String> fields = extractFields(sqlQuery);

        System.out.println(fields);

        String tablename = sqlQuery.split("from\\+")[1];

        System.out.println(tablename);


        String time = "2024-10-21T06:11:43.000+0000";

        System.out.println(isUtcTime(time));

    }

}
