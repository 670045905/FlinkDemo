package com.atguigu.otitem_analysis.hotitems;

import com.atguigu.otitem_analysis.beans.ItemViewCount;
import com.atguigu.otitem_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
//        DataStream<String> inputStream = env.readTextFile("");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9200");
        properties.setProperty("group.id","comsumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems",new SimpleStringSchema(),properties));

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
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤pv行为
                .keyBy(UserBehavior::getItemId) // 根据商品id进行排序
                .timeWindow(Time.hours(1), Time.minutes(5)) // 开窗，窗口时间为1小时，5分钟滑动一次
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        // 5. 收集统一窗口的商品count数据，然后排序输出topN
        DataStream<String> resultStream = windowAggStream
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopN(5));

        env.execute("hot items analysis");

    }

    private static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
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

    private static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount,Long, TimeWindow> {
        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(itemId,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    private static class TopN extends KeyedProcessFunction<Long,ItemViewCount,String> {

        Integer size;

        ListState<ItemViewCount> itemViewCountListState;

        public TopN(int i) {
            size = i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，存入list中，并注册定时器
            itemViewCountListState.add(itemViewCount);
            // 注意，这里实际上并不是每来一条数据就注册一个定时器
            // 在flink底层，是根据时间戳来区分定时器的，
            // 而这里的数据的窗口结束时间其实都是一样的，因为之前就已经根据窗口结束时间进行分组了
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    if (o2.getCount() > o1.getCount())
                        return 1;
                    if (o2.getCount() < o1.getCount())
                        return -1;
                    return 0;
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder result = new StringBuilder();
            result.append("==============================");
            result.append("窗口结束时间：").append(new Timestamp(timestamp -1)).append("\n");

            // 遍历列表，取topN输出
            for (int i=0;i < Math.min(size,itemViewCounts.size()); i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                result.append("NO ").append(i+1).append("：")
                        .append( "商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }

            result.append("==========================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(result.toString());

        }
    }
}
