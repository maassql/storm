package storm.kafka;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import storm.kafka.PartitionManager.KafkaMessageId;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;


public class KafkaSpoutMetrics implements IMetric {

	
	String _topic ;
	PartitionCoordinator _coordinator;
	
	/*
	TopologyContext _context;
	SpoutConfig _spoutConfig
	DynamicPartitionConnections _connections;
	*/
    
    SortedMap<String, Long> _byBrokerPartition_FailCount_untracked = new TreeMap();
    SortedMap<String, Long> _byBrokerPartition_FailCount_tracked = new TreeMap();
    SortedMap<String, Long> _byBrokerPartition_AckCount_untracked = new TreeMap();
    SortedMap<String, Long> _byBrokerPartition_AckCount_tracked = new TreeMap();   
	
    
    @Override
    public Object getValueAndReset() 
    {
    	
		HashMap ret = new HashMap();
		
		String keyPre = _topic  + "/brokers";
		
		ret = putMetrics(ret, keyPre, "failed/untracked/count",_byBrokerPartition_FailCount_untracked);
		ret = putMetrics(ret, keyPre, "failed/tracked/count", _byBrokerPartition_FailCount_tracked);
		ret = putMetrics(ret, keyPre, "ackd/untracked/count", _byBrokerPartition_AckCount_untracked);
		ret = putMetrics(ret, keyPre, "ackd/tracked/count",_byBrokerPartition_AckCount_tracked);

		ret.putAll(getMetricsPerPartitionManager());
		
		return ret;
    }    

    
    private HashMap getMetricsPerPartitionManager()
    {
    	HashMap ret = new HashMap();
    	
    	String keyPre = _topic  + "/brokers";
    	
    	List<PartitionManager> pms = _coordinator.getMyManagedPartitions();

    	for (PartitionManager pm : pms) {
    		
    		String partitionMetricName = partitionName(pm.getPartition());

    		String keyPartition = keyPre + "/" + partitionMetricName;
    		
    	    ret.put( keyPartition + "/tuples/pending/count", pm.countOfTuplesPending());
    	    ret.put( keyPartition + "/tuples/acked/count", pm.countOfTuplesAcked());
    	    ret.put( keyPartition + "/tuples/failed/count", pm.countOfTuplesFailed());
    	    ret.put( keyPartition + "/tuples/failed/ThenSkipped/count", pm.countOfTuplesFailedSkipped());
    	    ret.put( keyPartition + "/tuples/removedFromPendingBCMaxOffsetBehind/count", pm.countOfTuplesRemovedFromPendingBCMaxOffsetBehind());
    	    ret.put( keyPartition + "/tuples/failed/retryAwaiting/count", pm.countOfTuplesWaitingToRetry());
    	    ret.put( keyPartition + "/tuples/failed/retrying/count", pm.countOfTuplesRetrying());
    	}    
    	return ret;
    }
    
    
    public KafkaSpoutMetrics(String topic, PartitionCoordinator coordinator) {
        _topic = topic;
        _coordinator = coordinator;
    }
    
    /*
     * _metricOfKafkaSpout = MetricOfSpout.registerMetric(context, _spoutConfig, conf, _coordinator);
     */
    public static KafkaSpoutMetrics registerMetric (final TopologyContext context, SpoutConfig spoutConfig, Map stormConf, PartitionCoordinator coordinator)
    {
    	KafkaSpoutMetrics toRegister = new KafkaSpoutMetrics(spoutConfig.topic, coordinator);
    	
        context.registerMetric("kafkaSpoutInternals", toRegister , spoutConfig.metricsTimeBucketSizeInSecs);
    	
    	return(toRegister);
    }
    
    
    
   
    
    public void anotherUntrackedMessageFail(KafkaMessageId id)
    {
    	incrSortedMap(_byBrokerPartition_FailCount_untracked, partitionName(id));
    }
    
    
    /*
    if (m != null) {
        m.ack(id.offset);
        _metricOfKafkaSpout.anotherMessageAckd(id);
    }
    else {
    	_metricOfKafkaSpout.anotherUntrackedMessageAck(id);
    }	
     */
    public void anotherUntrackedMessageAck(KafkaMessageId id)
    {
    	incrSortedMap(_byBrokerPartition_AckCount_untracked, partitionName(id));
    }    
    
    public void anotherMessageFailed(KafkaMessageId id)
    {
    	incrSortedMap(_byBrokerPartition_FailCount_tracked, partitionName(id));
    }
    
    public void anotherMessageAckd(KafkaMessageId id)
    {
    	incrSortedMap(_byBrokerPartition_AckCount_tracked, partitionName(id));
    }
    
    



    
    private HashMap putMetrics (HashMap ret,  String keyPre, String keyName, SortedMap<String, Long> mapdCounter) 
    {
        for (Map.Entry entry : mapdCounter.entrySet())
        {
        	String key = (String)entry.getKey();
        	Long value = (Long)entry.getValue();
        	ret.put(keyPre + "/" + key + "/" + keyName, value);
        }
    	return ret;
    }
    
    
    private void incrSortedMap(SortedMap<String, Long> toIncr, String key){
        if (!toIncr.containsKey(key)) {
        	toIncr.put(key, 1l);
        }  
        else
        {
        	toIncr.put(key, (Long)toIncr.get(key) + 1l);
        }
    }
    
    private String  partitionName (KafkaMessageId id)
    {
    	return partitionName(id.partition);
    }
    
    private String brokerName (Partition p)
    {
    	return p.host.toString();
    }
    
    private String  partitionName (Partition p)
    {
    	return brokerName(p) + "/partition_" + p.partition;
    }
    
}



/*
//---------------------------------------------------------------
List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
Map concatMetricsDataMaps = new HashMap();
for (PartitionManager pm : pms) {
    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
}
return concatMetricsDataMaps;    	
//---------------------------------------------------------------
ret.put(_topic + "/" + partition.getId() + "/" + "spoutLag", spoutLag);
ret.put(_topic + "/" + partition.getId() + "/" + "earliestTimeOffset", earliestTimeOffset);
ret.put(_topic + "/" + partition.getId() + "/" + "latestTimeOffset", latestTimeOffset);
ret.put(_topic + "/" + partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);


    return ret;
//return null;

Partition partition = id.partition;
Broker partition_host = partition.host;
String broker_name = partition_host.toString();
int partition_id = partition.partition;
long offset = id.offset;
*/
