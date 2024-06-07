import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerWithTreadDemo {

    public static void main(String[] args) {
        new KafkaConsumerWithTreadDemo().run();
    }

    public KafkaConsumerWithTreadDemo() {
    }

    private void run() {


        Logger logger= LogManager.getLogger(KafkaConsumerWithTreadDemo.class);

        logger.info("Hey started");




        // CountDownLatch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the Consumer Runnable
        logger.info("Creating the consumer thread");


        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topic = System.getenv("TOPIC");
        String sleep = System.getenv("SLEEP");

        String groupId = System.getenv("GROUP_ID");


        ConsumerThread myConsumerThread = new ConsumerThread(topic, bootstrapServers, groupId, latch,
                Long.parseLong(sleep));

        // Start the Thread
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // Add a Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerThread.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is Closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private final CountDownLatch latch;
        KafkaConsumer<String, Customer> consumer;
        private final Logger logger = LogManager.getLogger(ConsumerThread.class);
        private final long sleep;

        public ConsumerThread(String topic, String bootstrapServer, String groupId, CountDownLatch latch, long sleep) {

            this.latch = latch;
            this.sleep = sleep;

            // Create Consumer Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap:9092");
            //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroup1");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");



            // Create Consumer
            consumer = new KafkaConsumer<>(properties);

            // Subscribe Consumer to Our Topics
            consumer.subscribe(List.of("testtopic1"), new RebalanceListener());

        }


        Instant lastCommitTime = Instant.now();


        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy'T'HH:mm:ss.SSSSSS");



        @Override
        public void run() {
            try {
                // Poll the data
                while (true) {
                    ConsumerRecords<String, Customer> records = consumer.poll(0);


                    if (Duration.between(lastCommitTime, Instant.now()).toMillis() >= 250) {
                        consumer.commitAsync();
                        //consumer.commitAsync();
                        lastCommitTime = Instant.now();

                    }



                    for (ConsumerRecord<String, Customer> record : records) {
                       /* logger.info("Key: " + record.key() +
                                " Value: " + record.value() +
                                " Partition: " + record.partition() +
                                " Offset: " + record.offset()
                        );*/
                        Thread.sleep(5);

                        Timestamp timestamp = new Timestamp(record.timestamp());
                        Date date = new Date(timestamp.getTime());

             /*           logger.info(" latency is {}, insertion time is {}, processing time is {}",
                                System.currentTimeMillis() - record.timestamp(), record.timestamp(), Instant.now());*/


                        logger.info(" latency is {}, insertion time is {}, processing time is {}",
                                System.currentTimeMillis() - record.timestamp(), simpleDateFormat.format(date), Instant.now());




                        //consumer.commitSync();

                    }

                   // consumer.commitSync();


                }
            } catch (WakeupException | InterruptedException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // Tell our main code
                // We are done
                // with the consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            // The wakeup() method is used
            // to interrupt consumer.poll()
            // It will throw WakeUpException
            consumer.wakeup();
        }
    }

}