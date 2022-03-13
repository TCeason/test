import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.cli.*;
import org.apache.commons.cli.BasicParser;

@Slf4j
public class demo {
    private static ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("thread-pool-%s").build();
    static final String JdbcDriver = "com.mysql.cj.jdbc.Driver";
    public static void multiThreadImport(final int threadNum,String url, String user, String password, String tr, String br) {
        final CountDownLatch cdl = new CountDownLatch(threadNum);
        long starttime = System.currentTimeMillis();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, 15, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(50000), threadFactory);
        for (int k = 1; k <= threadNum; k++) {
            executor.execute(() -> {
                Connection conn = null;
                try{
                    //注册 JDBC 驱动

                    Class.forName(JdbcDriver);

                    // 打开链接
                    System.out.println(System.currentTimeMillis() + " 连接数据库...");
                    conn = DriverManager.getConnection(url,user,password);

                    // 执行查询
                    System.out.println(System.currentTimeMillis() + " 准备执行query...");
                    String sql = "insert into db.t (id1, id2, id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,str1,str2,str3,dt) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                    PreparedStatement ps_stmt = conn.prepareStatement(sql);

                    Random r = new Random();
                    int batchs_time = 0;
                    for(int i=1; i<Integer.valueOf(tr); i++){
                        ps_stmt.setInt(1, i);
                        ps_stmt.setInt(2, r.nextInt(10));
                        ps_stmt.setInt(3, r.nextInt(100));
                        ps_stmt.setInt(4, r.nextInt(1000));
                        ps_stmt.setInt(5, r.nextInt(20));
                        ps_stmt.setInt(6, r.nextInt(200));
                        ps_stmt.setInt(7, r.nextInt(2000));
                        ps_stmt.setInt(8, r.nextInt(30));
                        ps_stmt.setInt(9, r.nextInt(300));
                        ps_stmt.setInt(10,r.nextInt(3000));
                        ps_stmt.setInt(11,r.nextInt(40));
                        ps_stmt.setInt(12,r.nextInt(400));
                        ps_stmt.setInt(13,r.nextInt(4000));
                        ps_stmt.setInt(14,r.nextInt(50));
                        ps_stmt.setInt(15,r.nextInt(500));
                        ps_stmt.setInt(16,r.nextInt(5000));
                        //ps_stmt.setString(17, RandomStringUtils.randomAlphanumeric(20));
                        ps_stmt.setString(17, RandomStringUtils.randomAlphanumeric(20));
                        ps_stmt.setString(18, RandomStringUtils.randomAlphanumeric(20));
                        ps_stmt.setString(19, RandomStringUtils.randomAlphanumeric(40));
                        ps_stmt.setString(20, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis() - r.nextLong(100000000))));
                        ps_stmt.addBatch();
                        // 1w条记录插入一次
                        if (i % Integer.valueOf(br) == 0){
                            Long start = System.currentTimeMillis();
                            ps_stmt.executeBatch();
                            Long end = System.currentTimeMillis();
                            batchs_time += end - start;
                            System.out.println(System.currentTimeMillis() + " 一批次 query 执行完毕");
                        }
                    }
                    // 最后插入不足1w条的数据
                    ps_stmt.executeBatch();
                    ps_stmt.close();
                    conn.close();
                    System.out.println("insert cost time = " + batchs_time + "ms");
                }catch(SQLException se){
                    // 处理 JDBC 错误
                    se.printStackTrace();
                }catch(Exception e){
                    // 处理 Class.forName 错误
                    e.printStackTrace();
                }finally{
                    // 关闭资源
                    try{
                        if(conn!=null) conn.close();
                    }catch(SQLException se){
                        se.printStackTrace();
                    }
                }

                System.out.println("\n执行成功！");
            }
            );
        }
        try {
            cdl.await();
            long spendtime = System.currentTimeMillis() - starttime;
            System.out.println(threadNum + "个线程花费时间:" + spendtime/1000+"S");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdown();
    }

    public static void main(String[] args) {
        Options options = new Options( );
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("br", "batch-rows", true, "the batch rows" );
        options.addOption("tr", "total-rows", true, "the total rows");
        options.addOption("u", "user", true, "user name");
        options.addOption("p", "password", true, "password");
        options.addOption("P", "port", true, "port");
        options.addOption("H", "host", true, "host");
        options.addOption("tn", "thread-nums", true, "thread nums");


        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            // 尝试解析命令行参数
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            // 打印解析异常
            System.err.println(e.getMessage());
            // 打印帮助信息
            // 退出程序，退出码为 1
            System.exit(1);
        }

        if (commandLine.hasOption('h')) {
            System.out.println( "Help Message");
            System.out.println("./bench8028 --thread-nums=2 --host=127.0.0.1 --user=root --password=root --port=3307 --batch-rows=1000 --total-rows=60000");
            System.exit(0);
        }
        String user = null;
        String password = null;
        String host = null;
        String port = null;
        String url = null;
        String br = null;
        String tr = null;
        String threadNum = null;

        if (commandLine.hasOption("u")) {
            user = commandLine.getOptionValue("--user");
        }
        if (commandLine.hasOption('p')) {
            password = commandLine.getOptionValue('p');
        }
        if (commandLine.hasOption('H')) {
            host = commandLine.getOptionValue('H');
        }
        if (commandLine.hasOption('P')) {
            port = commandLine.getOptionValue('P');
        }
        if (commandLine.hasOption("br")) {
            br = commandLine.getOptionValue("br");
        }
        if (commandLine.hasOption("tr")) {
            tr = commandLine.getOptionValue("tr");
        }
        if (host != null && port != null) {
            url = "jdbc:mysql://"+ host +":" + port + "/db?useSSL=false";
        }
        if (commandLine.hasOption("tn")) {
            threadNum = commandLine.getOptionValue("tn");
        }
        multiThreadImport(Integer.valueOf(threadNum),url, user, password, tr, br);
    }
}

