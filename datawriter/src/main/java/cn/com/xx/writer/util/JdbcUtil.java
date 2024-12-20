package cn.com.xx.writer.util;

import java.sql.*;
import java.util.List;

public class JdbcUtil {


    /**
     * 获取数据库连接
     * @param jdbcUrl
     * @param username
     * @param password
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
        return java.sql.DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 关闭数据库连接
     * @param pstmt
     * @param conn
     */
    public static void close(PreparedStatement pstmt, Connection conn) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  pstmt = conn.prepareStatement(sql);
     *
     *             Record record;
     *             while ((record = lineReceiver.getFromReader()) != null) {
     *                 for (int i = 0; i < columns.size(); i++) {
     *                     StringColumn column = record.getColumn(i).asStringColumn();
     *                     pstmt.setString(i + 1, column.getValue());
     *                 }
     *                 pstmt.addBatch();
     *             }
     *
     *             pstmt.executeBatch();
     */

    /**
     * 构建插入语句
     * @param table
     * @param columns
     * @return
     */
    public static String buildInsertSql(String table, List<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append(columns.get(i));
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append("?");
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    /**
     * 构建Upsert语句
     * @param table
     * @param columns
     * @return
     */
    public static String buildUpsertSql(String table, List<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder("upsert ");
        sqlBuilder.append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append(columns.get(i));
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append("?");
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(") with primary key");
        return sqlBuilder.toString();
    }

    /**
     * 构建MYSQL upsert语句
     * @param table
     * @param columns
     * @return
     */
    public static String buildReplaceSql(String table, List<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder("REPLACE INTO ");
        sqlBuilder.append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append(columns.get(i));
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            sqlBuilder.append("?");
            if (i < columns.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }


    public static void main(String[] args) {

//        String sql = "UPSERT SF.TEST_ROMA (id, name, ORDERLINE__C) VALUES (?, ?, ?) with primary key";

//        Connection connection = null;
//        PreparedStatement preparedStatement = null;

        try {
//             connection = getConnection(Constants.HANA_JDBCURL, Constants.HANA_USERNAME, Constants.HANA_PASSWORD);
//            // 4. 设置自动提交为 false
//            connection.setAutoCommit(false);
//            preparedStatement = connection.prepareStatement(sql);
//
//            for (int i = 1; i <= 2; i++) {
//                preparedStatement.setInt(1, i);
//                preparedStatement.setString(2, "ZHIXUN-TEST" + i);
//                preparedStatement.setInt(3, i * 10);
//                preparedStatement.addBatch();
//            }
//
//            preparedStatement.executeBatch();
//
//            System.out.println("执行成功");
//
//            // 7. 提交事务
//            connection.commit();
            String sql = "select+Id+,+IsDeleted+,+Name+,+CreatedDate+,+CreatedById+,+LastModifiedDate+,+LastModifiedById+,+SystemModstamp+,+LastActivityDate+,+Partner__c+,+BillingAddressFlag__c+,+Description__c+,+ExternalId__c+,+IsActive__c+,+IsDefault__c+,+Location__c+,+Region__c+,+ShippingAddressFlag__c+,+Shiptoaddress1__c+,+DeliveryLimited__c+from+address__c";
            List<String> fields = StringUtil.extractFields(sql);

            System.out.println(buildUpsertSql("address__c",fields));


        } catch (Exception throwables) {
            throwables.printStackTrace();
        }finally {
//            close(preparedStatement, connection);
        }


    }





}
