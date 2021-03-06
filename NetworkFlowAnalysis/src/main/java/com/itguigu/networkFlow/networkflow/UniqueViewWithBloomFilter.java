package com.itguigu.networkFlow.networkflow;

import com.itguigu.networkFlow.beans.PageViewCount;
import com.itguigu.networkFlow.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.HashSet;

/**
 * 独立用户访问数
 */

public class UniqueViewWithBloomFilter {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，并转换为pojo
        URL resource = UniqueViewWithBloomFilter.class.getResource("/userbehaviro.csv");
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
                .trigger(new MyTrigger())
                .process(new UVCountResultWithBloomFilter());

        uvStream.print();


        env.execute("unique view with UniqueViewWithBloomFilter");
    }


    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFilter {
        // 定义位图的大小,一般需要定义成2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个哈希函数
        public Long hashcode(String value, Integer seed) {
            Long result = 0L;
            for (int i=0;i<value.length();i++){
                result = result * seed + value.charAt(i);
            }
            return result & (cap -1);
        }
    }

    // 实现自定义的函数
    private static class UVCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior,PageViewCount,TimeWindow> {

        // 定义Jedis连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost",6379);
            myBloomFilter = new MyBloomFilter(1<<29);  // 要处理1亿个数据，用64MB大小
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口的count值全部存入redis，用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一张哈希表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的userId
            Long userId = iterable.iterator().next().getUserId();
            // 2. 计算位图中的偏移量
            Long offset  = myBloomFilter.hashcode(userId.toString(),43);

            // 3. 用redis的getbit命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey,offset);

            if(!isExist){
                // 如果不存在，对应位图位置置1
                jedis.setbit(bitmapKey,offset,true);
                // 更新redis保存的count值
                Long uvCount = 0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)){
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName,countKey, String.valueOf(uvCount + 1));

                collector.collect(new PageViewCount("uv",windowEnd,uvCount + 1));
            }


        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}
