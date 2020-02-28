package org.wltea.analyzer.ext;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.logging.log4j.Logger;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.sql.*;

/**
 * @Description: 数据库连接配置工具类
 * @Author: LIU.KL
 * @Date: 2020/2/26
 * ...
 */
public class DBConnUtils {

    private static final Logger logger = ESPluginLoggerFactory.getLogger(DBConnUtils.class.getName());

    // TODO: 2020/2/26 读取配置文件
    private static final String driver = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://10.10.16.20:3306/zll_test?serverTimezone=GMT&characterEncoding=utf8";
    private static final String username = "remote";
    private static final String password = "F2CAuq0h89spFlri";

    /**
     * 获取数据库连接 JDBC 连接数据库
     * @return
     */
    public static Connection getDataBaseConnection(){
        Connection connection = null;
        try {
            // 高版本驱动可忽略
            Class.forName("com.mysql.jdbc.Driver");
            connection= DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            logger.error("[>>>>>>>>>] 药渡 get db connection fail", e);
        }
        return connection;
    }

    /**
     * druid 连接池 获取数据库连接
     * @param druidDataSource
     * @return
     */
    public static DruidPooledConnection getDruidDataSourceConnection(DruidDataSource druidDataSource){
        DruidPooledConnection connection = null;
        druidDataSource.setDriverClassName(driver);
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);
        druidDataSource.setInitialSize(5);
        druidDataSource.setMaxActive(50);
        druidDataSource.setMinIdle(5);
        druidDataSource.setMaxWait(60000);
        druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
        druidDataSource.setMinEvictableIdleTimeMillis(300000);
        druidDataSource.setValidationQuery("SELECT 1 FROM DUAL");
        try {
            connection = druidDataSource.getConnection();
        } catch (SQLException e) {
            logger.error("[>>>>>>>>>] 药渡 get db connection fail", e);
        }
        return connection;
    }

    /**
     * 关闭数据库连接
     * @param connection
     */
    public static void closeConnection(Connection connection){
        if(null != connection){
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("[>>>>>>>>>>] 药渡 close db connection fail", e);
            }
        }
    }

    public static void main(String[] args) {
        Connection jdbcConnection = null;
        Connection druidDataSourceConnection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatement1 = null;
        String sql = "select count(1) cc from ES_IK_EXT_WORD";
        try {
            // JDBC 连接数据库 test
            jdbcConnection = getDataBaseConnection();
            System.out.print("执行SQL：" + sql);
            preparedStatement = jdbcConnection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                int cc = resultSet.getInt("cc");
                System.out.println("  》》执行结果：" + cc);
            }

            // druid test
            druidDataSourceConnection = getDruidDataSourceConnection(new DruidDataSource());
            System.out.print("执行SQL：" + sql);
            preparedStatement1 = druidDataSourceConnection.prepareStatement(sql);
            ResultSet resultSet1 = preparedStatement1.executeQuery();
            while (resultSet1.next()){
                int cc = resultSet1.getInt("cc");
                System.out.println("  》》执行结果：" + cc);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection(jdbcConnection);
            closeConnection(druidDataSourceConnection);
        }
    }

}
