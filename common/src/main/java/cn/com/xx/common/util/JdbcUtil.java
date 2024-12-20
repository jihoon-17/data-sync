package cn.com.xx.common.util;

import cn.com.xx.common.constant.Constants;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
@Slf4j
public class JdbcUtil {

    private final Connection connect;
    private final String userName;
    private final String passWord;
    private final String ipAddress;
    private final String databaseName;
    private final String port;



    //构造方法
    public JdbcUtil(String userName, String passWord, String ipAddress, String databaseName, String port) {
        this.userName = userName;
        this.passWord = passWord;
        this.ipAddress = ipAddress;
        this.databaseName = databaseName;
        this.port = port;
        this.connect = this.Connect();
    }

    //建立链接
    private Connection Connect() {
        Connection c = null;
        try {//
            Class.forName("com.mysql.cj.jdbc.Driver");
            c = DriverManager
                    .getConnection("jdbc:mysql://" + this.ipAddress + ":" + this.port + "/" + this.databaseName,
                            this.userName, this.passWord);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return c;
    }

    //关流操作
    public void close() {
        Connection c = this.connect;
        try {
            c.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    public static String select(String table){

        JdbcUtil pgsqlUtil = new JdbcUtil(Constants.userName, Constants.passWord,  Constants.ipAddress, Constants.databaseName, Constants.port);

        String sql_table = "select lastmodifieddate from "+table+" order by lastmodifieddate desc limit 1";

        List<HashMap<String, Object>> list = pgsqlUtil.Select(sql_table);

        Object modifydate = "";

        try {

            if(Objects.nonNull(list) && list.size() > 0){

                modifydate = list.get(0).get("lastmodifieddate");
                System.out.println(modifydate.toString());
            }


        } catch (Exception throwable) {
            throwable.printStackTrace();
        }finally {
            pgsqlUtil.close();
        }

        return modifydate.toString();


    }


    /**
     * 获取最新的最后更新时间
     * @return
     */
    public static String getMaxLastModifyDate(String table){

        JdbcUtil pgsqlUtil = new JdbcUtil(Constants.userName, Constants.passWord,  Constants.ipAddress, Constants.databaseName, Constants.port);

        String sql_table = "select lastmodifieddate from "+table+" order by lastmodifieddate desc limit 1";

        List<HashMap<String, Object>> list = pgsqlUtil.Select(sql_table);

        Object modifydate = "";

        try {

            if(Objects.nonNull(list) && list.size() > 0){

                modifydate = list.get(0).get("lastmodifieddate");
                System.out.println(modifydate.toString());
            }



        } catch (Exception throwable) {
            throwable.printStackTrace();
        }finally {
            pgsqlUtil.close();
        }

        return modifydate.toString();


    }

    public static void main(String[] args) throws Exception {

//        getMaxLastModifyDate("ods.mac_vrvinfo__c_df");

//        String account_sql = "select lastname,name,personmobilephone,phone from "+ Constants.Account__c+" where id = '0015i00001794hqAAA'";
//        JdbcUtil pgsqlUtil = new JdbcUtil(Constants.userName, Constants.passWord,  Constants.ipAddress, Constants.databaseName, Constants.port);

//
//        List<HashMap<String, Object>> list = pgsqlUtil.Select(account_sql);
//
//        System.out.println(list.get(0).get("name"));



        SimpleDateFormat sdf_mm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = new Date();
        JdbcUtil pgsqlUtil = new JdbcUtil(Constants.userName, Constants.passWord,  Constants.ipAddress, Constants.databaseName, Constants.port);

        String sql_table = "insert into tmp.mac_2_sr_last_etl_time values ('mac_2_sr','"+sdf_mm.format(date)+"')";
        pgsqlUtil.Insert(sql_table);
        pgsqlUtil.close();

    }



    //查询
    public List<HashMap<String, Object>> Select(String sql) {
        //1、与数据库建立链接
        Connection c = this.connect;
        //2、创建操作对象
        Statement stmt = null;
        //3、创建返回最终查询的数据集合
        List<HashMap<String, Object>> list = new ArrayList<>();
        try {
            //2.1、初始化操作对象
            stmt = c.createStatement();
            //4、执行需要执行的sql语句
            ResultSet rs = stmt.executeQuery(sql);
            //3.1开始封装返回的对象
            ResultSetMetaData metaData = rs.getMetaData();//获取全部列名
            int columnCount = metaData.getColumnCount();//列的数量
            //5、读取数据
            while (rs.next()) {
                HashMap<String, Object> map = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    //getColumnName获取列名
                    String name = metaData.getColumnName(i);
                    //获取对应的元素
                    Object object = rs.getObject(i);
                    map.put(name, object);
                }
                list.add(map);
            }
            //6、关流操作
            rs.close();
            stmt.close();
            //c.close();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
        return list;
    }

    //插入操作
    public Boolean Insert(String sql) {
        //1、与数据库建立链接
        Connection connect = this.connect;
        //2、创建操作对象
        Statement stmt = null;
        int count = 0;
        try {
            //2.1、初始化创建对象
            stmt = connect.createStatement();
            //3、添加特殊语句。
            connect.setAutoCommit(false);//之前不用
            //4、执行添加操作
            count = stmt.executeUpdate(sql);

            //5、关流
            stmt.close();
            connect.commit();
            //connect.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return count != 0;
    }

    //删除
    public void Delete(String sql) {
        //1、链接数据库
        Connection c = this.Connect();
        Statement stmt = null;
        try {
            c.setAutoCommit(false);
            stmt = c.createStatement();

            stmt.executeUpdate(sql);
            c.commit();

            stmt.close();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
    }


}
