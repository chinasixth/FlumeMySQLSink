package com.qinglianyun.flume.sink.mysql;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 12:03 2019/2/27
 * @ desc  :  将采集的数据按批次写入数据库
 */
public class MySQLSinkByBatch extends AbstractSink implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSinkByBatch.class);

    private static int batchSize = 10;

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    private String hostname;

    private String port;

    private String user;

    private String password;

    private String database;

    private String tableName;

    private String columnName;

    private static String separate;

    private Connection conn;

    private PreparedStatement pstmt;

    public MySQLSinkByBatch() {
        LOGGER.info("MySQL Sink start ...");
    }

    @Override
    public synchronized void start() {
        super.start();

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database;

        try {
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);

            String[] strings = columnName.split(",");

            StringBuffer buffer = new StringBuffer(strings[0]);
            StringBuffer valueBuffer = new StringBuffer("?");
            for (int i = 1; i < strings.length; i++) {
                buffer.append("," + strings[i]);
                valueBuffer.append(",?");
            }

            pstmt = conn.prepareStatement("insert into " + tableName + " (" + buffer.toString() + ") values (" + valueBuffer.toString() + ")");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
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
        LOGGER.info("MySQL Sink stop ......");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        boolean success = false;
        int txnEventCount = 0;

        Event event;
        for (; txnEventCount < batchSize; txnEventCount++) {
            event = channel.take();
            if (event == null) {
                break;
            }

            String content = new String(event.getBody());
            String[] strings = content.split(separate);
            try {
                if (columnName.split(",").length == strings.length) {
                    for (int i = 0; i < strings.length; i++) {
                        pstmt.setObject(i + 1, strings[i]);
                    }
                    pstmt.addBatch();
                    pstmt.executeBatch();
                    conn.commit();
                } else {
                    LOGGER.info("数据和数据库列不匹配。。。。。。");
                }
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }
        try {
            transaction.commit();
            success = true;
            if (txnEventCount < 1) {
                return Status.BACKOFF;
            }
        } catch (Exception e) {
            LOGGER.error("transaction failure ......");
        } finally {
            if (!success) {
                transaction.rollback();
            }
            transaction.close();
        }
        return Status.READY;
    }

    // 从配置文件中读取各种属性，并进行非空检验
    @Override
    public void configure(Context context) {
        hostname = context.getString("hostname");
        Preconditions.checkNotNull(hostname, "hostname must be set");
        port = context.getString("port");
        Preconditions.checkNotNull(port, "port must be set");
        user = context.getString("user");
        Preconditions.checkNotNull(user, "user must be set");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set");
        database = context.getString("database");
        Preconditions.checkNotNull(database, "database must be set");
        columnName = context.getString("columnName");
        Preconditions.checkNotNull(columnName, "columnName must be set");
        // TODO tableName
        String table_name = context.getString("tableName");
        if (table_name.startsWith("%")){
            tableName = table_name.substring(2, table_name.length() - 1);
        } else {
            tableName = table_name;
        }
        Preconditions.checkNotNull(tableName, "tableName must be set");
        // TODO separate
        String sep = context.getString("separate");
        if (sep != null){
            separate = sep;
        }
    }
}
