import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ConsumerFetcher {

    private static Logger log = Logger.getLogger(ConsumerFetcher.class);

    private static ConsumerConnector connector;
    private static Properties kafkaProps;
    private static KafkaStream<byte[], byte[]> kafkaStream;
    private static ConsumerIterator<byte[], byte[]> iterator;
    public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String DEFAULT_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String CONSUMER_TIMEOUT_MS = "consumer.timeout.ms";
    public static final int DEFAULT_BATCH_DURATION = 10000;

    public static void main(String[] args) {
        String topic = "fetcher1";
        kafkaProps = getTopicKafkaProperties();
        connector = Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(kafkaProps));
        Map<String, Integer> topicWorkerMap = new HashMap<String, Integer>();
        topicWorkerMap.put(topic.trim(), 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicWorkerMap);
        if (streams.get(topic).size() != 1) {
            System.out.println("Unexpected number of streams created.");
            log.info("Unexpected number of streams created expected: 1, actual:" + streams.get(topic).size());
            return;
        }
        kafkaStream = streams.get(topic).get(0);
        iterator = kafkaStream.iterator();

        log.info("starting consuming data for topic: " + topic);
        System.out.println("starting consuming data for topic: " + topic);
        long startTime = System.currentTimeMillis();
        log.info("start time: " + startTime);
        System.out.println("start time: " + startTime);

        long count = 0;
        boolean running = true;
        while (running) {
            try {
                if (!iterator.hasNext()) {
                    running = false;
                    continue;
                }
            } catch (ConsumerTimeoutException e) {
                System.out.println("exception: " + e.getMessage());
                running = false;
                continue;
            }
            MessageAndMetadata<byte[], byte[]> message = iterator.next();
            count++;
            if (count % 10000 == 0) {
                log.info("Number of messages read: " + count);
                System.out.println("Number of messages read: " + count);
                connector.commitOffsets(true);
            }

        }

        log.info("Number of messages read: " + count);
        log.info("Time taken(ms): " + (System.currentTimeMillis() - startTime));
        System.out.println("Number of messages read: " + count);
        System.out.println("Time taken(ms): " + (System.currentTimeMillis() - startTime));
        connector.commitOffsets(true);
        connector.shutdown();

    }

    private static Properties getTopicKafkaProperties() {
        Properties properties = new Properties();
        properties.put("group.id", "flume-collector-" + UUID.randomUUID().toString());
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "smallest");
        properties.put("num.consumer.fetchers", "1");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
        properties.put(CONSUMER_TIMEOUT_MS, Integer.toString(DEFAULT_BATCH_DURATION));
        return properties;
    }

}
