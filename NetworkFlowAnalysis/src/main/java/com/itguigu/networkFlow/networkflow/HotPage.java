package com.itguigu.networkFlow.networkflow;

import com.itguigu.networkFlow.beans.ApacheLogEvent;
import com.itguigu.networkFlow.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.regex.Pattern;

/**
 * todo 乱序数据改进
 */

public class HotPage {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取文件，并转换为pojo
//        URL resource = HotPage.class.getResource("/apache.log");
//        DataStream<String> inputStream = env.readTextFile(resource.getPath());
        DataStream<String> inputStream = env.socketTextStream("localhost",7777);

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:MM:ss");
            Long timestamp = sdf.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });

        // 定义一个测输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};


        // 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod())) // 过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex,data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)   // 根据URL分组
                .timeWindow(Time.minutes(10), Time.seconds(5))  // 开滑窗
                .allowedLateness(Time.minutes(1))   // 允许迟到数据
                    .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集统一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPage(3));


        resultStream.print();

        env.execute("hot page ananysis");
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1 + aLong;
        }
    }

    private static class PageCountResult implements WindowFunction<Long,PageViewCount,String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> input, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url,timeWindow.getEnd(),input.iterator().next()));
        }
    }

    private static class TopNHotPage extends KeyedProcessFunction<Long,PageViewCount,String> {

        private Integer size;
        public TopNHotPage(int i) {
            this.size = i;
        }

        // 定义状态，保存当前所有PageViewCount到list中
        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("pageViewCountListState",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            pageViewCountListState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());

            pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    if (o1.getCount() > o2.getCount())
                        return -1;
                    if (o1.getCount() < o2.getCount())
                        return 1;
                    return 0;
                }
            });

            // 格式化成字符串输出
            // 将排名信息格式化成String，方便打印输出
            StringBuilder result = new StringBuilder();
            result.append("==============================");
            result.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n");

            // 遍历列表，取topN输出
            for (int i=0;i < Math.min(size,pageViewCounts.size()); i++){
                PageViewCount currentPageViewCount = pageViewCounts.get(i);
                result.append("NO ").append(i+1).append("：")
                        .append( "页面URL = ").append(currentPageViewCount.getUrl())
                        .append(" 浏览量 = ").append(currentPageViewCount.getCount())
                        .append("\n");
            }

            result.append("==========================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(result.toString());

        }
    }
}
