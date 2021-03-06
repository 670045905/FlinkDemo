package com.itguigu.networkFlow.networkflow;

import com.itguigu.networkFlow.beans.PageViewCount;
import com.itguigu.networkFlow.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.akka.org.jboss.netty.channel.ExceptionEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * 网站总浏览量
 */

public class PageView {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，并转换为pojo
        URL resource = HotPage.class.getResource("/userbehaviro.csv");
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

        // 4. 过滤，分组，开窗，聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤pv行为
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0) // 根据商品id进行排序
                .timeWindow(Time.hours(1)) // 开一小时滚动统计
                .sum(1);

        // 并行任务改进，设计随机key，改进数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                }).keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将各分区的数据汇总起来
        DataStream<PageViewCount> resultStream = pvStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());


        pvResultStream.print();

        env.execute("page view job");
    }

    private static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    private static class PvCountResult implements WindowFunction<Long,PageViewCount,Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(integer.toString(),timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    // 实现自定义处理函数，把相同窗口分组统计的count累加
    private static class TotalPvCount  extends KeyedProcessFunction<Long,PageViewCount,PageViewCount> {

        // 定义状态，保存当前总count值
        ValueState<Long> total;

        @Override
        public void open(Configuration parameters) throws Exception {
            total = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total",Long.class, 0L));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<PageViewCount> collector) throws Exception {
            total.update(total.value() + pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 定时器触发，所有分组count值到齐，直接输出当前的count数量
            Long totalCount = total.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),totalCount));

            total.clear();
        }
    }
}
