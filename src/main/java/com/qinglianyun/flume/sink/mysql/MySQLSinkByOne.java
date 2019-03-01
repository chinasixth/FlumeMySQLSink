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
import java.util.Map;

/**
 * @ Author ：liuhao
 * @ Company: qinglianyun
 * @ Date   ：Created in 12:06 2019/2/27
 * @ desc :  将采集的数据逐条写入数据库
 */
public class MySQLSinkByOne extends AbstractSink implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSinkByOne.class);

    private static int batchSize = 10;

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    private String hostname;

    private String port;

    private String user;

    private String password;

    private String database;

    private String tableName;

    private static String tableNameFlag;

    private String columnName;

    private static String separate = "\\^A";

    private Connection conn;

    private PreparedStatement pstmt;

    private StringBuffer buffer;

    private StringBuffer valueBuffer;

    public MySQLSinkByOne() {
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

            buffer = new StringBuffer(strings[0]);
            valueBuffer = new StringBuffer("?");
            for (int i = 1; i < strings.length; i++) {
                buffer.append("," + strings[i]);
                valueBuffer.append(",?");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();

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
    public Sink.Status process() throws EventDeliveryException {
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

            if (tableNameFlag != null) {
                Map<String, String> headers = event.getHeaders();
                for (Map.Entry<String, String> stringStringEntry : headers.entrySet()) {
                    if (stringStringEntry.getKey().equals(tableNameFlag)) {
                        tableName = stringStringEntry.getValue();
                        break;
                    }
                }
            }

            String content = new String(event.getBody());
            String[] strings = content.split(separate);
            try {
                pstmt = conn.prepareStatement("insert into " + tableName + " (" + buffer.toString() + ") values (" + valueBuffer.toString() + ")");
                if (columnName.split(",").length == strings.length) {
                    for (int i = 0; i < strings.length; i++) {
                        pstmt.setObject(i + 1, strings[i]);
                    }
                    pstmt.execute();
                    conn.commit();

                    if (pstmt != null) {
                        try {
                            pstmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    LOGGER.info("数据和数据库列不匹配。。。。。。");
                }
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    LOGGER.error(e + "_conn.rollback failure ......");
                }
                LOGGER.error(e + "_insert operate failure ......");
            }
        }
        try {
            transaction.commit();
            success = true;
            if (txnEventCount < 1) {
                return Sink.Status.BACKOFF;
            }
        } catch (Exception e) {
            LOGGER.error(e + "_transaction failure ......");
        } finally {
            if (!success) {
                transaction.rollback();
            }
            transaction.close();
        }
        return Sink.Status.READY;
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
        if (table_name.startsWith("%")) {
            tableNameFlag = table_name.substring(2, table_name.length() - 1);
            Preconditions.checkNotNull(tableNameFlag, "tableName must be set");
        } else {
            tableName = table_name;
            Preconditions.checkNotNull(tableName, "tableName must be set");
        }
        // TODO separate
        String sep = context.getString("separate");
        if (sep != null) {
            separate = sep;
        }
    }
}
