package com.itguigu.MarketAnalysis.market_analysis;

import com.itguigu.MarketAnalysis.beans.ChannelPromotionCount;
import com.itguigu.MarketAnalysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从自定义数据源中读取数据源
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulateMarkingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTimstamp();
                    }
                });

        // 2. 分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5)) // 定义滑窗
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();

        env.execute("marketing job");



    }

    // 实现自定义的模拟市场用户行为数据源
    private static class SimulateMarkingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

        // 控制是否正常运行的标志位
        Boolean runing = true;

        // 定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (runing) {
                // 随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                sourceContext.collect(new MarketingUserBehavior(id,behavior,channel,timestamp));

                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            this.runing = false;
        }
    }


    private static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }


    private static class MarketingCountResult extends ProcessWindowFunction<Long,ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String timesatmp = new Timestamp(context.window().getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount(channel,behavior,timesatmp,count));
        }
    }
}
