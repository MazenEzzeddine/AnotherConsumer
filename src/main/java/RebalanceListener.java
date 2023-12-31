import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Date;

public class RebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LogManager.getLogger(RebalanceListener.class);

    static long rebalanceStartTime =0;
    static long rebalanceEndTime =0;
    static boolean firstTime = true;


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
      /*  try {
            log.info("Sleeping on rebalancing");
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

      /*  log.info(" Rebalancing started");
        rebalanceStartTime = System.currentTimeMillis(); //Instant.now();
        log.info("Hence, this timestamp is important, it indicates somehow provisioning/startup time");*/

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {


       /* log.info("current time is {}", new Date(System.currentTimeMillis()));


        if(firstTime) {
            firstTime = false;
            log.info("First time assigned, ");
        }


        else {

            log.info(" Rebalancing Completed");
            rebalanceEndTime = System.currentTimeMillis();//Instant.now();
            // Duration time = Duration.between(rebalanceStartTime, rebalanceEndTime);
            long time = rebalanceEndTime - rebalanceStartTime;

            log.info("Rebalancing took {} ms", time);
        }*/


    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
       // ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
