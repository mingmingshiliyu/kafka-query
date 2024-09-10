

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class kafkaqury {
    public static void main(String[] args) throws IOException, ParseException {


        Yaml yaml = new Yaml();
        FileInputStream fileInputStream = new FileInputStream(new File("kafka.yml"));
        Map<String,String> load = yaml.load(fileInputStream);
        Properties props = new Properties();

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("security.protocol", "SASL_SSL");
//        props.put("sasl.jaas.config","com.sun.security.auth.module.Krb5LoginModule required\n" +
//                        "useKeyTab=true\n" +
//                        "keyTab=\""+
//                        "principal=\""+
//                        "useTicketCache=false\n" +
//                        "storeKey=true\n" +
//                        "debug=true \n" +
//                        "refreshKrb5Config=true;");
        load.forEach((k,v)->{
            props.put(k,v);
        });
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("输入topic名称:");
        String topic = br.readLine();
        System.out.println("输入partition:");
        String par = br.readLine();
        System.out.println("输入起始时间yyyy-MM-dd HH:mm:ss");
        String start = br.readLine();
        System.out.println("输入结束时间yyyy-MM-dd HH:mm:ss");
        String end = br.readLine();


        // 设置时间段
        TopicPartition partition = new TopicPartition(topic, Integer.parseInt(par));
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition)); // 从最早的消息开始查询
//        long startTime = System.currentTimeMillis() - 7*24*3600000; // 7天
//        long endTime = System.currentTimeMillis()-6*24*3600000; // 当前时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = sdf.parse(start);
        Date endTime = sdf.parse(end);


        consumer.seek(partition, startTime.getTime());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                if (record.timestamp() <= endTime.getTime()) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            });

            if (System.currentTimeMillis() >= endTime.getTime()) {
                break;
            }
        }

        consumer.close();
    }
}
