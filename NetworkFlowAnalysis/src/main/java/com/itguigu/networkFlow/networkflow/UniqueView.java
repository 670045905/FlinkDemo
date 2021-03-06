package com.itguigu.networkFlow.networkflow;

import com.itguigu.networkFlow.beans.PageViewCount;
import com.itguigu.networkFlow.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;

/**
 * 独立用户访问数
 */

public class UniqueView {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，并转换为pojo
        URL resource = UniqueView.class.getResource("/userbehaviro.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        // 3.转换为POJO类，并分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        // 开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        uvStream.print();


        env.execute("unique view");
    }

    private static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> userBehaviors, Collector<PageViewCount> collector) throws Exception {
            // 定义一个set结构，保存窗口中的所有userId,自动去重
            HashSet<Long> uidSet = new HashSet<>();
            for(UserBehavior ub:userBehaviors){
                uidSet.add(ub.getUserId());
            }
            collector.collect(new PageViewCount("uv",timeWindow.getEnd(),(long)uidSet.size()));
        }
    }
}
