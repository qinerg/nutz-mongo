package org.nutz.mongo.performance;

import java.net.UnknownHostException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nutz.lang.Stopwatch;
import org.nutz.lang.random.R;
import org.nutz.mongo.MongoConnector;
import org.nutz.mongo.MongoDao;
import org.nutz.mongo.performance.pojo.Student;
import org.nutz.mongo.util.Moo;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * 单线程增、删、改、查性能测试
 * @author qinerg@gmail.com
 */
public class SingleThreadTest {
    //运行总次数
    private static final int RUN_COUNT = 5000;
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
    public void a_insert() {
        long nutz = insertByNutz();
        long orig = insertByOriginal();
        System.out.printf("时间比为 %f\n", 1.0 * nutz/orig);
    }
    
    @Test
    public void b_select() {
        long nutz = selectByNutz();
        long orig = selectByOriginal();
        System.out.printf("时间比为 %f\n", 1.0 * nutz/orig);
    }
    
    @Test
    public void c_update() {
        long nutz = updateByNutz();
        long orig = updateByOriginal();
        System.out.printf("时间比为 %f\n", 1.0 * nutz/orig);
    }
    
    @Test
    public void d_delete() {
        long nutz = deleteByNutz();
        insertByOriginal();
        long orig = deleteByOriginal();
        System.out.printf("时间比为 %f\n", 1.0 * nutz/orig);
    }

    @AfterClass
    public static void after() {
        dao.cleanCursors();
        connector.close();
    }
    
    private long insertByNutz() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            Student s = new Student();
            s.setSid(i);
            s.setName("学员" + i);
            s.setAge(R.random(10, 16));
            s.setBirthday(new Date());
            s.setAddress("abcdefg");
            dao.save(s);
        }
        sw.stop();
        System.out.printf("Nutz\t批量插入 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }
    
    private long insertByOriginal() {
        coll.drop();
        coll.ensureIndex("sid");
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            DBObject dbo = new BasicDBObject();
            dbo.put("sid", i);
            dbo.put("name", "学员" + i);
            dbo.put("age", R.random(10, 16));
            dbo.put("birthday", new Date());
            dbo.put("address", "abcdefg");
            coll.insert(dbo);
        }
        sw.stop();
        System.out.printf("原生驱动\t批量插入 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }
    
    private long selectByOriginal() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            coll.findOne(BasicDBObjectBuilder.start("sid", i).get());
        }
        sw.stop();
        System.out.printf("原生驱动\t批量查询 %d次,耗时%8dms,平均%8d次/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }

    private long selectByNutz() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            dao.findOne(Student.class, Moo.NEW().eq("sid", i));
        }
        sw.stop(); 
        System.out.printf("Nutz\t批量查询 %d次,耗时%8dms,平均%8d次/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }
    
    private long updateByOriginal() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            coll.update(BasicDBObjectBuilder.start("sid", i).get(), BasicDBObjectBuilder.start("name", i + "学员").get());
        }
        sw.stop();
        System.out.printf("原生驱动\t批量修改 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }

    private long updateByNutz() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            dao.update(Student.class, Moo.NEW().eq("sid", i), Moo.NEW().append("name", i + "学员"));
        }
        sw.stop();
        System.out.printf("Nutz\t批量修改 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }
    
    private long deleteByOriginal() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            coll.remove(BasicDBObjectBuilder.start("sid", i).get());
            dao.remove(Student.class, Moo.NEW("sid", i));
        }
        sw.stop();
        System.out.printf("原生驱动\t批量删除 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }

    private long deleteByNutz() {
        Stopwatch sw = Stopwatch.begin();
        for (int i = 0; i < RUN_COUNT; i++) {
            dao.remove(Student.class, Moo.NEW("sid", i));
        }
        sw.stop();
        System.out.printf("Nutz\t批量删除 %d条,耗时%8dms,平均%8d条/秒\n", RUN_COUNT, sw.getDuration(), RUN_COUNT * 1000/sw.getDuration());
        return sw.getDuration();
    }
}
