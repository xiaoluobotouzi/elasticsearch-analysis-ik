package org.wltea.analyzer.ext;

import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
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
        logger.info("[>>>>>>>>>>>] 药渡 60s load once hot dict from mysql. Start...");
        // java 权限安全检查
        SpecialPermission.check();
        // 访问控制
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            this.runUnprivileged();
            return null;
        });
        logger.info("[>>>>>>>>>>>] 药渡 60s load hot dict from mysql End...");
    }

    public MonitorMysql(int extWordMaxVersion, int stopWordMaxVersion){
        this.currentExtWordVersion = extWordMaxVersion;
        this.currentStopWordVersion = stopWordMaxVersion;
    }

    // 当前扩展词库版本
    private int currentExtWordVersion;
    // 当前扩展停用词库版本
    private int currentStopWordVersion;

    /**
     * 监控流程：
     *  ①向数据库发送查询版本请求语句
     *  ②从查询结果获取版本号字段，判断数据库版本和当前版本
     *  ③如果数据库版本和当前版本一致，说明未变化，休眠1min，返回第①步
     * 	④如果数据库版本比当前版本大，说明有新词，重新加载词典
     *  ⑤休眠1min，返回第①步
     */
    public void runUnprivileged() {
        logger.info("[>>>>>>>>>>] 药渡 get dict version start");
        Dictionary singleton = Dictionary.getSingleton();
        // 获取数据库连接
        DruidPooledConnection DBConnection = singleton.getDruidDataSourceConnection();

        // 连接有效
        if(null != DBConnection){
            // 查询版本控制表，扩展词库版本和停用词库版本
            String selectVersionSql = "SELECT MAX(WORD_VER) WORD_VER, MAX(STOP_WORD_VER) STOP_WORD_VER FROM ES_IK_EXT_WORD_VER";
            try {
                PreparedStatement preparedStatement = DBConnection.prepareStatement(selectVersionSql);
                ResultSet resultSet = preparedStatement.executeQuery();
                int newWordVersion = 0;
                int newStopWordVersion = 0;
                while (resultSet.next()){
                    newWordVersion = resultSet.getInt("WORD_VER");
                    newStopWordVersion = resultSet.getInt("STOP_WORD_VER");
                }

                logger.info("[>>>>>>>>>>] 药渡 get extend word version. mysql version " + newWordVersion + ", current version " + currentExtWordVersion);

                // 数据库版本大于当前版本，更新词库
                if(newWordVersion > currentExtWordVersion){
                    // 增量词库，修改版本
                    boolean b = Dictionary.getSingleton().reLoadHotDictByMysql(currentExtWordVersion);
                    if(b){
                        currentExtWordVersion = newWordVersion;
                    }
                }

                logger.info("[>>>>>>>>>>] 药渡 get stop word version. mysql version " + newStopWordVersion + ", current version " + currentStopWordVersion);

                // 数据库版本大于当前版本，更新停用词库
                if(newStopWordVersion > currentStopWordVersion){
                    // 增量停用词库，修改版本
                    boolean b = Dictionary.getSingleton().reLoadStopWordByMysql(currentStopWordVersion);
                    if(b){
                        currentStopWordVersion = newStopWordVersion;
                    }
                }
            } catch (SQLException e) {
                logger.error("[>>>>>>>>>>] 药渡 load dict Error ", e);
            }
        }
    }
}
