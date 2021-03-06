package com.itguigu.LoginFailDetect.LoginFailDetect;

import com.itguigu.LoginFailDetect.beans.LoginEvent;
import com.itguigu.LoginFailDetect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;
import java.util.List;

public class LoginFail {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1. 从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> dataStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent) {
                        return loginEvent.getTimestamp() * 1000L;
                    }
                });

        // 自定义处理函数检测连续登陆失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = dataStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));

        warningStream.print();

        env.execute("login fail job");

    }

    private static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long,LoginEvent, LoginFailWarning> {

        // 定义最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态，保存两秒内所有登陆失败事件
        ListState<LoginEvent> loginFailEventListState;
        // 定义状态，保存注册的定时器时间戳
        ValueState<Long> timesTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailEventListState",LoginEvent.class));
            timesTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timesTsState",Long.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前登陆事件的类型
            if("fail".equals(loginEvent.getLoginState())) {
                // 1. 如果是失败事件，添加到列表状态中
                loginFailEventListState.add(loginEvent);
                // 如果没有定时器，那么注册一个两秒之后的定时器
                if(timesTsState.value() == null ) {
                    Long ts = ( loginEvent.getTimestamp() + 2) * 1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timesTsState.update(ts);
                }
            } else {
                // 2. 如果是登陆成功，那么删除定时器，清空状态，重新开始
                if (timesTsState.value()!=null) {
                    context.timerService().deleteEventTimeTimer(timesTsState.value());
                }
                loginFailEventListState.clear();
                timesTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            // 定时器触发，说明至少两秒内没有登陆成功。判断两秒内登陆失败的个数
            List<LoginEvent> list = Lists.newArrayList(loginFailEventListState.get().iterator());
            Integer failTimes = list.size();

            if (failTimes >= maxFailTimes) {
                // 如果超出了设定的最大失败次数，输出报警
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        list.get(0).getTimestamp(),list.get(failTimes - 1).getTimestamp(),
                        "login fail in 2s for "+ failTimes + "tiems"));
            }

            // 清空状态
            loginFailEventListState.clear();
            timesTsState.clear();
        }
    }

    private static class LoginFailDetectWarning extends KeyedProcessFunction<Long,LoginEvent, LoginFailWarning> {

        // 定义最大连续登陆失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 定义状态，保存两秒内所有登陆失败事件
        ListState<LoginEvent> loginFailEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailEventListState",LoginEvent.class));
        }

        // 以登陆事件作为判断报警的触发条件，不再注册定时器
        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前事件登陆状态
            if ("fail".equals(loginEvent.getLoginState())) {
                // 如果是登陆失败，获取状态中之前的登录失败事件，继续判断已有失败事件
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if (iterator.hasNext()) {
                    // 1.1 如果已经有登录失败事件，继续判断时间戳是否在两秒之内
                    // 获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if (loginEvent.getTimestamp() - firstFailEvent.getTimestamp() <= 2) {
                        // 1.1.1 如果在2秒之内，输出报警
                        collector.collect(new LoginFailWarning(loginEvent.getUserId(),firstFailEvent.getTimestamp(),loginEvent.getTimestamp(), "login fail in 2s for "+ 2 + "tiems"));
                    }
                    // 不管报不报警，这次都已经处理完毕，直接更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(loginEvent);
                } else {
                    // 1.2 如果没有登陆失败事件，直接将当前时间存入listState
                    loginFailEventListState.add(loginEvent);
                }

            } else {
                // 登录成功，直接清空状态
                loginFailEventListState.clear();
            }
        }

    }
}
