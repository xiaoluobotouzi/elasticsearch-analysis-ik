package org.wltea.analyzer.ext;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.logging.log4j.Logger;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    private static final String url = "jdbc:mysql://10.10.16.20:3306/zll_test?characterEncoding=utf8";
    private static final String username = "remote";
    private static final String password = "F2CAuq0h89spFlri";

    /**
     * MysqlDataSource 不确定是是否有集成连接池
     * @param mysqlDataSource
     * @return
     */
    public static Connection getMysqlDataSourceConnection(MysqlDataSource mysqlDataSource){
        Connection connection = null;
        mysqlDataSource.setUrl(url);
        mysqlDataSource.setUser(username);
        mysqlDataSource.setPassword(password);
        try {
            connection = mysqlDataSource.getConnection();
        } catch (SQLException e) {
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
        Connection mysqlDataSourceConnection = null;
        Connection druidDataSourceConnection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement preparedStatement1 = null;
        String sql = "select count(1) cc from ES_IK_EXT_WORD";
        try {
            // mysql-connection-java test
            mysqlDataSourceConnection = getMysqlDataSourceConnection(new MysqlDataSource());
            System.out.print("执行SQL：" + sql);
            preparedStatement = mysqlDataSourceConnection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                int cc = resultSet.getInt("cc");
                System.out.println("  》》执行结果：" + cc);
            }

            // druid test
            druidDataSourceConnection = getDruidDataSourceConnection(new DruidDataSource());
            System.out.println("执行SQL：" + sql);
            preparedStatement1 = druidDataSourceConnection.prepareStatement(sql);
            ResultSet resultSet1 = preparedStatement1.executeQuery();
            while (resultSet1.next()){
                int cc = resultSet1.getInt("cc");
                System.out.println("  》》执行结果：" + cc);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection(mysqlDataSourceConnection);
            closeConnection(druidDataSourceConnection);
        }
    }

}
