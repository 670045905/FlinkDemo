package com.itguigu.OrderTimeoutDetect;

import com.itguigu.OrderTimeoutDetect.beans.OrderEvent;
import com.itguigu.OrderTimeoutDetect.beans.OrderResult;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderTimeoutDetect {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据并转为pojo类
        URL resource = OrderTimeoutDetect.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> dataStream = env.readTextFile(resource.getPath()).map(line -> {
//            System.out.println(line);
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.getTimestamp() * 1000L;
            }
        });

        // 1. 定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern
                .<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        // 2. 定义侧输出流标签，表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

        // 3. 将pattern应用到数据流上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(dataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 4. 调用select方法，实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultSream = patternStream
                .select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultSream.print();
        resultSream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect");

    }

    // 实现自定义的超时事件处理函数
    private static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult> {

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timestamp) throws Exception {
            // 获取超时订单id
            Long timeoutOrderId= map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId, "timeout "+timestamp);
        }
    }

    // 实现自定义的政策匹配事件处理函数
    private static class OrderPaySelect implements PatternSelectFunction<OrderEvent,OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId,"payed");
        }
    }
}
