public class kafkaqury {
}

import org.apache.kafka.clients.consumer.ConsumerConfig;
        import org.apache.kafka.clients.consumer.ConsumerRecords;
        import org.apache.kafka.clients.consumer.KafkaConsumer;
        import org.apache.kafka.common.TopicPartition;
        import org.apache.kafka.common.serialization.StringDeserializer;

        import java.time.Duration;
        import java.util.Collections;
        import java.util.Properties;

public class KafkaTimeRangeQuery {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 设置时间段
        TopicPartition partition = new TopicPartition("my-topic", 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition)); // 从最早的消息开始查询
        long startTime = System.currentTimeMillis() - 3600000; // 1小时前
        long endTime = System.currentTimeMillis(); // 当前时间

        consumer.seek(partition, startTime);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                if (record.timestamp() <= endTime) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            });

            if (System.currentTimeMillis() >= endTime) {
                break;
            }
        }

        consumer.close();
    }
}
