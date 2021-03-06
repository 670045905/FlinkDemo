package com.atguigu.otitem_analysis.hotitems;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaProducerUtil {

    public static void main(String[] args) throws Exception{
        writeToKafka("hotitems");
    }

    // 包装一个写入kafka的方法
    public static void writeToKafka(String topic) throws Exception{

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9200");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 用缓冲方式读取文本数据
        BufferedReader bufferedReader = new BufferedReader(new FileReader(""));
        String line;
        while((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            // 用kafka发送数据
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();

    }
}
