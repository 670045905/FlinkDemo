package com.itguigu.OrderTimeoutDetect;

import com.itguigu.OrderTimeoutDetect.beans.OrderEvent;
import com.itguigu.OrderTimeoutDetect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxPayMatch {

    // 定义侧输出流标签
    private final static OutputTag<OrderEvent> unMatchPays = new OutputTag<OrderEvent>("unMatchPays"){};
    private final static OutputTag<ReceiptEvent> unMatchReceipt = new OutputTag<ReceiptEvent>("unMatchReceipt"){};


    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取订单数据并转为pojo类,同时筛选出pay事件
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderStream = env.readTextFile(orderResource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.getTimestamp() * 1000L;
            }
        }).filter(data -> !"".equals(data.getTxId()));

        // 读取到账数据并转为pojo类
        URL txResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
        DataStream<ReceiptEvent> receiptEventStream = env.readTextFile(txResource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
            @Override
            public long extractAscendingTimestamp(ReceiptEvent receiptEvent) {
                return receiptEvent.getTimestamp() * 1000L;
            }
        });

        // 将俩条流进行连接合并，进行匹配处理,不匹配的事件
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderStream.keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("match-pays");
        resultStream.getSideOutput(unMatchPays).print("unMatchPays");
        resultStream.getSideOutput(unMatchReceipt).print("unMatchReceipt");

        env.execute("TxPayMatch job");

    }

    // 实现自定义的coProcessfunction
    private static class TxPayMatchDetect extends CoProcessFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {

        // 定义状态，保存当前已经到来的订单支付事件，和到账事件
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receipState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueState<OrderEvent> payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
            ValueState<ReceiptEvent> receipState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipState", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            // 订单支付之间来了，判断是否已经有对应的到账事件
            ReceiptEvent receiptEvent = receipState.value();
            if (receiptEvent != null) {
                // 如果receiptEvent不为空，说明到账事件已经来过，输出匹配事件，清空状态
                collector.collect(new Tuple2<>(orderEvent,receiptEvent));
                payState.clear();
                receipState.clear();
            } else {
                // 如果如果receiptEvent还没来，注册一个定时器，开始等待
                context.timerService().registerEventTimeTimer((orderEvent.getTimestamp() + 5) * 1000L); // 等待5秒钟
                // 更新状态
                payState.update(orderEvent);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            // 到账支付之间来了，判断是否已经有对应的支付事件
            OrderEvent pay = payState.value();
            if (pay != null) {
                // 如果pay不为空，说明到账事件已经来过，输出匹配事件，清空状态
                collector.collect(new Tuple2<>(pay,receiptEvent));
                payState.clear();
                receipState.clear();
            } else {
                // 如果pay还没来，注册一个定时器，开始等待
                context.timerService().registerEventTimeTimer((receiptEvent.getTimestamp() + 5) * 1000L); // 等待5秒钟
                // 更新状态
                receipState.update(receiptEvent);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            // 定时器触发，有可能是某一个事件没来，也可能都来齐了，已经输出并清空状态
            // 判断哪个不为空，那么另一个就没来
            if (payState.value() != null) {
                // 说明到账事件没来
                ctx.output(unMatchPays,payState.value());
            }
            if (receipState != null) {
                ctx.output(unMatchReceipt,receipState.value());
            }
            // 清空状态
            payState.clear();
            receipState.clear();
        }
    }
}
