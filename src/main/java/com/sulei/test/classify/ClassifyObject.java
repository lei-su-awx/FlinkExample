package com.sulei.test.classify;

/**
 * @author sulei
 * @date 2019/10/31
 * @e-mail 776531804@qq.com
 */

public class ClassifyObject {

    private String id; //交易流水号
    private String type; //事件类型，成功，失败，等待，取消
    private double volume; //交易额

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public ClassifyObject() {
    }

    public ClassifyObject(String id, String type, double volume) {
        this.id = id;
        this.type = type;
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "type=" + type + ", volume=" + volume + ", id=" + id;
    }
}
