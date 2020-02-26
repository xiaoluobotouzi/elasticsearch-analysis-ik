package org.wltea.analyzer.ext;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.logging.log4j.Logger;
import org.wltea.analyzer.dic.Dictionary;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Description: 监听Mysql词库
 * @Author: LIU.KL
 * @Date: 2020/2/26
 * ...
 */
public class MonitorMysql implements Runnable {

    private static final Logger logger = ESPluginLoggerFactory.getLogger(MonitorMysql.class.getName());

    @Override
    public void run() {
        logger.info("[>>>>>>>>>>>] reload hot dict from mysql Start...");
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            this.runUnprivileged();
            return null;
        });
        logger.info("[>>>>>>>>>>>] reload hot dict from mysql End...");
    }

    private int currentVersion;

    /**
     * 监控流程：
     *  ①向数据库发送查询版本请求语句
     *  ②从查询结果获取版本号字段，判断数据库版本和当前版本
     *  ③如果数据库版本和当前版本一致，说明未变化，休眠1min，返回第①步
     * 	④如果数据库版本比当前版本大，说明有新词，重新加载词典
     *  ⑤休眠1min，返回第①步
     */
    public void runUnprivileged() {
        // 获取数据库连接
        DruidPooledConnection druidDataSourceConnection =
                DBConnUtils.getDruidDataSourceConnection(new DruidDataSource());

        // 连接有效
        if(null != druidDataSourceConnection){
            // 查询版本表最大的版本
            String selectVersionSql = "SELECT MAX(VERSION) FROM ES_IK_EXT_WORD_VER";
            try {
                PreparedStatement preparedStatement = druidDataSourceConnection.prepareStatement(selectVersionSql);
                ResultSet resultSet = preparedStatement.executeQuery();
                int newVersion = 0;
                while (resultSet.next()){
                    newVersion = resultSet.getInt("VERSION");
                }
                // 数据库新版本大于当前版本，更新词库
                if(newVersion > currentVersion){
                    // 增量词库，修改版本
                    Dictionary.getSingleton().reLoadHotDictByMysql(newVersion);
                    currentVersion = newVersion;
                }
            } catch (SQLException e) {
                logger.error("[>>>>>>>>>>] 药渡 reLoad Hot Dict Error ", e);
            }
        }

        // 释放连接
        DBConnUtils.closeConnection(druidDataSourceConnection);
    }
}
