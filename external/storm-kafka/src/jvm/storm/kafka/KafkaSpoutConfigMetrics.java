package storm.kafka;

import backtype.storm.task.TopologyContext;

public class KafkaSpoutConfigMetrics {

	
    public static boolean registerMetric (final TopologyContext context, SpoutConfig spoutConfig)
    {
    	boolean ret_SuccesfullyRegistered = false;
    	
    	
    	String name_of_topic = spoutConfig.topic;
    	String name_of_spout = spoutConfig.id;
    	
    	
    	return(ret_SuccesfullyRegistered);
    }
	
}
