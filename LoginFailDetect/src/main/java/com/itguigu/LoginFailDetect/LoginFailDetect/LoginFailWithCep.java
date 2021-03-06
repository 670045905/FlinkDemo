package com.itguigu.LoginFailDetect.LoginFailDetect;

import com.itguigu.LoginFailDetect.beans.LoginEvent;
import com.itguigu.LoginFailDetect.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1. 从文件中读取数据
        URL resource = LoginFailWithCep.class.getResource("/LoginLog.csv");
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

        // 2. 定义一个匹配模式，定义事件发生先后的模式
        // firstFail -> secondFail,within 2s
//        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("firstFail")
//                .where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getLoginState());
//            }
//        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {
//            @Override
//            public boolean filter(LoginEvent loginEvent) throws Exception {
//                return "fail".equals(loginEvent.getLoginState());
//            }
//        }).within(Time.seconds(2));
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("failEvent")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                }).times(8).consecutive();

        // 3. 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 4. 检出符合匹配条件的复杂事件，进行转换处理，得到报警信息
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());

        warningStream.print();

        env.execute("login fail job");
    }

    // 实现自定义的PatternSelectFunction
    private static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent,LoginFailWarning> {
        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
//            LoginEvent firstFail = map.get("firstFail").get(0);
//            LoginEvent secondFail = map.get("secondFail").get(0);
              LoginEvent firstFail = map.get("failEvent").get(0);
              LoginEvent lastFail = map.get("failEvent").get(map.get("failEvent").size() - 1);
            return new LoginFailWarning(firstFail.getUserId(),
                    firstFail.getTimestamp(),lastFail.getTimestamp(),
                    "login fail "+ map.get("failEvent").size() +" times in 2s");
        }
    }
}
