package com.itguigu.MarketAnalysis.market_analysis;

import com.itguigu.MarketAnalysis.beans.AdClickEvent;
import com.itguigu.MarketAnalysis.beans.AdCountViewByProvince;
import com.itguigu.MarketAnalysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;

import java.net.URL;
import java.sql.Timestamp;

public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. 从文件中读取数据
        URL resource = AdStatisticsByProvince.class.getResource("/AdClickLog.csv");
        DataStream<AdClickEvent> dataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(new Long(fields[0]),new Long(fields[1]),fields[2],fields[3],new Long(fields[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent adClickEvent) {
                        return adClickEvent.getTimestamp() * 1000L;
                    }
                });

        // 2. 对同一个用户对同一个广告的点击行为进行检测报警
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = dataStream
                .keyBy("userId", "adId") //基于用户id和广告id做分组
                .process(new FilterBlackListUser(100));

        // 3. 基于省份开窗分组聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = dataStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5))  // 定义滑窗，五分钟滑动一次
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();

        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){}).print("blacklist-user");

        env.execute("AdStatisticsByProvince job");

    }

    private static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
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

    private static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince,String, TimeWindow> {
        @Override
        public void apply(String province, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new AdCountViewByProvince(province,windowEnd,count));

        }
    }


    // 实现自定义处理函数
    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent> {

        // 定义属性，点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(int countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        // 定义标志状态，保存当前用户是否被发送到黑名单中
        ValueState<Boolean> isSendState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countState",Long.class,0L));
            isSendState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isSendState",Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，就count+1正常输出，如果达到上限，就直接过滤掉，并侧输出流到黑名单
            // 首先获取当前的count值
            Long curCount = countState.value();


            // 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                Long ts = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - (8 * 60 * 60 * 1000);
                context.timerService().registerEventTimeTimer(ts);
            }

            // 2. 判断是否告警
            if (curCount >= countUpperBound) {
                // 判断是否输出到黑名单过，如果没有，就输出到侧输出流
                if (!isSendState.value()) {
                    isSendState.update(true);
                    context.output(new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(adClickEvent.getUserId(),adClickEvent.getAdId(),"click over" + countUpperBound + "times"));
                }

                return ; // 不再执行下面操作
            }

            // 如果没有返回，点击次数加一，更新状态，正常输出当前数据到主流
            countState.update(curCount + 1);
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            // 清空所有状态
            countState.clear();
            isSendState.clear();
        }
    }
}
