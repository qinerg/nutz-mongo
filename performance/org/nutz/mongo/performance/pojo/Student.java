package org.nutz.mongo.performance.pojo;

import java.util.Date;

import org.nutz.mongo.annotation.Co;
import org.nutz.mongo.annotation.CoField;
import org.nutz.mongo.annotation.CoIndexes;

@Co
@CoIndexes("+sid")
public class Student {
    @CoField
    private long sid;
    @CoField
    private String name;
    @CoField
    private int age;
    @CoField
    private String address;
    @CoField
    private Date birthday;

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return String.format("%6d:%s(%s %tc) [%s]", sid, name, age, birthday, address);
    }

}
