package org.nutz.mongo.performance;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.lang.Stopwatch;
import org.nutz.lang.random.R;
import org.nutz.mongo.MongoConnector;
import org.nutz.mongo.MongoDao;
import org.nutz.mongo.performance.pojo.Student;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * 多线程增、删、改、查性能测试
 * 
 * @author qinerg@gmail.com
 */
public class MultiThreadTest {

    // 运行总次数
    private static final int RUN_COUNT = 5000;
    // 启动的线程数
    private static final int THREAD_COUNT = 10;
    private static final String DB_NAME = "nutz_mongo_performance";

    private static MongoConnector connector;
    private static MongoDao dao;
    private static DBCollection coll;

    @BeforeClass
    public static void before() throws UnknownHostException, MongoException {
        connector = new MongoConnector("localhost", 27017);
        dao = connector.getDao(DB_NAME);
        DB db = connector.getMongo().getDB(DB_NAME);
        coll = db.getCollection("student");

        dao.create(Student.class, true);
        dao.save(new Student());

        System.out.println("预热完成开始测试...");
    }

    @Test
    public void test1() throws InterruptedException {
        test1ByNutz();
        test1ByOriginal();
    }

    @AfterClass
    public static void after() {
        dao.cleanCursors();
        connector.close();
    }
    
    public void test1ByOriginal() throws InterruptedException {
        final CountDownLatch beginLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; ++i) {
            Thread thread = new Thread() {
                public void run() {
                    try {
                        beginLatch.await();
                        for (int i = 0; i < RUN_COUNT; ++i) {
                            DBObject dbo = new BasicDBObject();
                            dbo.put("sid", i);
                            dbo.put("name", "学员" + i);
                            dbo.put("age", R.random(10, 16));
                            dbo.put("birthday", new Date());
                            dbo.put("address", "abcdefg");
                            coll.insert(dbo);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        endLatch.countDown();
                    }
                }
            };
            thread.start();
        }
        Stopwatch sw = Stopwatch.begin();
        beginLatch.countDown();
        endLatch.await();
        sw.stop();
        System.out.printf("原生驱动\t多线程插入 %d * %d 条,耗时%8dms,平均%8d条/秒\n", THREAD_COUNT, RUN_COUNT, sw.getDuration(), 
                          THREAD_COUNT * RUN_COUNT * 1000/sw.getDuration());
    }

    public void test1ByNutz() throws InterruptedException {
        final CountDownLatch beginLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; ++i) {
            Thread thread = new Thread() {
                public void run() {
                    try {
                        beginLatch.await();
                        for (int i = 0; i < RUN_COUNT; ++i) {
                            Student s = new Student();
                            s.setSid(i);
                            s.setName("学员" + i);
                            s.setAge(R.random(10, 16));
                            s.setBirthday(new Date());
                            s.setAddress("abcdefg");
                            dao.save(s);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        endLatch.countDown();
                    }
                }
            };
            thread.start();
        }
        Stopwatch sw = Stopwatch.begin();
        beginLatch.countDown();
        endLatch.await();
        sw.stop();
        System.out.printf("Nutz\t多线程插入 %d * %d 条,耗时%8dms,平均%8d条/秒\n", THREAD_COUNT, RUN_COUNT, sw.getDuration(), 
                          THREAD_COUNT * RUN_COUNT * 1000/sw.getDuration());
    }
}
