package com.itguigu.OrderTimeoutDetect;

import com.itguigu.OrderTimeoutDetect.beans.OrderEvent;
import com.itguigu.OrderTimeoutDetect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;


import java.net.URL;

public class OrderTimeoutDetectWithoutCEP {

    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("orderTimeoutTag"){};

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

        // 自定义处理函数，主流输出正常匹配事件，侧输出流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = dataStream
                .keyBy(OrderEvent::getOrderId).process(new OrderPayMatchDetect());

        resultStream.print("payed normaly");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without CEP");

    }

    private static class OrderPayMatchDetect extends KeyedProcessFunction<Long,OrderEvent,OrderResult> {

        // 保存之前订单是否已经来过create，pay事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreateState;

        // 保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed",Boolean.class,false));
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-isCreateState",Boolean.class,false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState",Long.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
            // 获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreate = isCreateState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if("create".equals(orderEvent.getEventType())) {
                // 1. 如果来的是create，要判断是否支付过
                if (isPayed) {
                    // 1.1 如果已经正常支付，输出正常匹配结果
                    collector.collect(new OrderResult(orderEvent.getOrderId(),"pay successfully"));
                    // 清空状态，删除定时器
                    isPayedState.clear();
                    isCreateState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 1.2 没有支付过，注册15分钟之后的定时器
                    Long ts = (orderEvent.getTimestamp() + 15*60) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreateState.update(true);
                }
            } else if("pay".equals(orderEvent.getEventType())) {
                // 如果来的是pay，要判断是否有过下单事件
                if (isCreate) {
                    // 2.1 已经有过下单事件，要继续判断支付的时间戳是否超过15分钟
                    if (orderEvent.getTimestamp() * 1000L < timerTs) {
                        // 2.1.1. 在15分钟内，没有超时，正常匹配
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                    } else {
                        // 2.1.2 已经超时，侧输出流输出
                        context.output(orderTimeoutTag,new OrderResult(orderEvent.getOrderId(),"payed but timeout"));
                    }
                    // 清空状态，删除定时器
                    isPayedState.clear();
                    isCreateState.clear();
                    timerTsState.clear();
                    context.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 2.2 支付事件先来，下单事件还没来，乱序，注册一个定时器，等待下单事件
                    context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L);
                    timerTsState.update(orderEvent.getTimestamp());
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没来
             if (isPayedState.value()) {
                 // 如果pay来了，那么说明create没来
                 ctx.output(orderTimeoutTag,new OrderResult(ctx.getCurrentKey(),"payed but not found create"));
             } else {
                 // 如果pay没来，说明订单超时
                 ctx.output(orderTimeoutTag,new OrderResult(ctx.getCurrentKey(),"timeout"));
             }
            // 清空状态，删除定时器
            isPayedState.clear();
            isCreateState.clear();
            timerTsState.clear();
        }
    }
}
