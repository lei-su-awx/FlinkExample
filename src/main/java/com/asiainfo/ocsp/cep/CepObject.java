package com.asiainfo.ocsp.cep;

import java.util.Objects;

/**
 * @author sulei
 * @date 2019/10/31
 * @e-mail 776531804@qq.com
 */

public class CepObject {

    private String id; //用户id
    private String type; //事件类型，成功，失败
    private double volume; //交易额
    private long timestamp; //时间戳

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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public CepObject() {
    }

    public CepObject(String id, String type, double volume, long timestamp) {
        this.id = id;
        this.type = type;
        this.volume = volume;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CepObject{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", volume=" + volume +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, volume);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CepObject) {
            CepObject other = (CepObject) obj;
            return this.id.equals(other.getId()) && this.type.equals(other.getType()) && this.volume == other.getVolume();
        }
        return false;
    }
}
