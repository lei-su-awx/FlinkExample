package com.sulei.test.cep;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author sulei
 * @date 2019/10/31
 * @e-mail 776531804@qq.com
 */

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<CepObject> {

    private final long maxOutOfOrderness = 5000;
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(CepObject element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
