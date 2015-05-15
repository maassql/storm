package storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.kafka.PartitionManager.KafkaMessageId;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;

public class KafkaSpoutMetrics implements IMetric {

	String _topic ;
	PartitionCoordinator _coordinator;
	
    RubeGoldbergMetrics _metrics_to_report = new RubeGoldbergMetrics();
    RubeGoldbergMetrics _metrics_partition_level_to_aggregate = new RubeGoldbergMetrics();
    RubeGoldbergMetrics _metrics_to_report_kafkaErrors = new RubeGoldbergMetrics();
    
    long _tuples_failed_total = 0;
    long _tuples_ackd_total = 0;
    long _tuplesEmittedTotal = 0;
    long _tupleRetriesTotal = 0;
    long _tuplesTotal_Pending = 0;
    long _tuplesNow_WaitingToEmit = 0;
    long _tuplesTotal_RemovedBCMaxOffsetBehind_Pending = 0;
    long _tuplesTotal_RemovedBCMaxOffsetBehind_Ackd = 0;
    long _tuplesTotal_RemovedBCMaxOffsetBehind_Failed = 0;
    long _tuplesTotal_Failed_Retry_Awaiting = 0;
    long _tuplesTotal_Failed_Retry_Retrying = 0;
    long _tuplesTotal_Failed_Retry_Successful = 0;
    long _tuplesTotal_Failed_Retry_Total = 0;    
    
    @Override
    public Object getValueAndReset() 
    {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		
		_metrics_to_report_kafkaErrors = new RubeGoldbergMetrics();
		
		//getMetricsPerPartitionManager();
		getInternallyManagedMetrics();

		// gather metrics from the RubeGolbergMetrics machine
		ret.putAll(_metrics_to_report.getMetricsAndReset());
		
		ret.putAll(_metrics_to_report_kafkaErrors.getMetricsAndReset());
		
		return ret;
    }    


    public KafkaSpoutMetrics(String topic, PartitionCoordinator coordinator) {
        _topic = topic;
        _coordinator = coordinator;
        
        
        _metrics_to_report.turnOnRollingMetrics( metricPath ( "ackd" ));
        /*
		_metrics_to_report.turnOnRollingMetrics( metricPath ( "failed" ));
		_metrics_to_report.turnOnRollingMetrics( metricPath ( "emitted" ));
		_metrics_to_report.turnOnRollingMetrics( metricPath ( "failed/retries" ));   
		_metrics_to_report.turnOnRollingMetrics( metricPath ( "pending" )); 
		_metrics_to_report.turnOnRollingMetrics( metricPath ( "waitingToEmit" ));
		 */
    }
    
    public static KafkaSpoutMetrics registerMetric (final TopologyContext context, final SpoutConfig spoutConfig, @SuppressWarnings("rawtypes") final Map stormConf, final PartitionCoordinator coordinator, final DynamicPartitionConnections connections)
    {
    	KafkaSpoutMetrics toRegister = new KafkaSpoutMetrics(spoutConfig.topic, coordinator);
        context.registerMetric("kafkaSpout", toRegister , spoutConfig.metricsTimeBucketSizeInSecs);
        
        registerMetric_KafkaOffset(context, spoutConfig, connections, coordinator);
        registerMetric_KafkaPartition(context, spoutConfig, coordinator);
        
    	return(toRegister);
    }
    

    public void anotherUntrackedMessageFail(KafkaMessageId id)
    {
    	// don't see a reason to keep partition level information
    	_metrics_to_report.incr(metricPath("failed/untracked"));
    	_tuples_failed_total++;
    }

    public void anotherUntrackedMessageAck(KafkaMessageId id)
    {
    	_metrics_to_report.incr(metricPath("ackd/untracked"));
    	_tuples_ackd_total++;
    }    
    
    public void anotherMessageFailed(KafkaMessageId id)
    {
    	_metrics_to_report.incr(metricPath("failed/tracked"));
    	_tuples_failed_total++;
    }
    
    public void anotherMessageAckd(KafkaMessageId id)
    {
    	_metrics_to_report.incr(metricPath("ackd/tracked"));
    	_tuples_ackd_total++;
    }
    
    
    
    
    
    private String partitionName (KafkaMessageId id)
    {
    	return partitionName(id.partition);
    }
    
    private String brokerName (Partition p)
    {
    	return p.host.toString();
    }
    
    private String partitionName (Partition p)
    {
    	return brokerName(p) + "/partition_" + p.partition;
    }
    
    private String partitionMetricPath(KafkaMessageId id)
    {
    	return  metricPath (partitionName(id));
    }

    @SuppressWarnings("unused")
	private String partitionMetricPath(KafkaMessageId id, String key)
    {
    	return partitionMetricPath(id) + "/" + key;
    }
    
    private String partitionMetricPath(Partition p)
    {
    	return metricPath (partitionName(p));
    }
    
    private String metricPath(String key)
    {
    	return this.keyPre() + "/" + key;
    }
    
    private String keyPre()
    {
    	return _topic  	;
    }
    
    private void getInternallyManagedMetrics()
    {
    	_metrics_to_report.setTotalRunningValue( metricPath ( "ackd" ), _tuples_ackd_total);
    	/*
    	_metrics_to_report.setTotalRunningValue( metricPath ( "failed" ) , _tuples_failed_total);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "emitted" ) , _tuplesEmittedTotal);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "failed/retries" ) , _tupleRetriesTotal);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "emitted/wo_retries" ) , _tuplesEmittedTotal - _tupleRetriesTotal); //emitted includes retries.  emitted >= retries
    	_metrics_to_report.setTotalRunningValue( metricPath ( "emitted/wo_completed" ) , _tuplesEmittedTotal - ( _tuples_ackd_total + _tuples_failed_total ));
    	_metrics_to_report.setGaugesCurrentValue( metricPath ( "pending" ) , _tuplesTotal_Pending);
    	_metrics_to_report.setGaugesCurrentValue( metricPath ( "waitingToEmit" ) , _tuplesNow_WaitingToEmit);
    	

    	_metrics_to_report.setTotalRunningValue( metricPath ( "removedBCMaxOffsetBehind/pending" ) , _tuplesTotal_RemovedBCMaxOffsetBehind_Pending);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "removedBCMaxOffsetBehind/ackd" ) , _tuplesTotal_RemovedBCMaxOffsetBehind_Ackd);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "removedBCMaxOffsetBehind/failed" ) , _tuplesTotal_RemovedBCMaxOffsetBehind_Failed);
    	_metrics_to_report.setGaugesCurrentValue( metricPath ( "failed/retries/awaiting" ) , _tuplesTotal_Failed_Retry_Awaiting);
    	_metrics_to_report.setGaugesCurrentValue( metricPath ( "failed/retries/retrying" ) , _tuplesTotal_Failed_Retry_Retrying);
    	_metrics_to_report.setTotalRunningValue( metricPath ( "failed/retries/successful" ) , _tuplesTotal_Failed_Retry_Successful);
    	*/
    }    
    
    
    
    private void getMetricsPerPartitionManager()
    {

    	List<PartitionManager> pms = _coordinator.getMyManagedPartitions();

    	for (PartitionManager pm : pms) {

    		String partitionMetricName = partitionMetricPath(pm.getPartition());	

    		/*
    		 * remove from collection:
    		 _metrics_to_report.setTotalRunningValue("partitionAckdTotal", partitionMetricName + "/ackd", pm.totalOfTuplesAcked());
    		 _metrics_to_report.setTotalRunningValue("partitionTuplesFailed", partitionMetricName + "/failed", pm.totalOfTuplesFailed());
    		 */
    	    _metrics_to_report.setTotalRunningValue("partitionTuplesRemovedBCMaxOffsetBehind-Pending", partitionMetricName + "/pending/removedFromPendingBCMaxOffsetBehind", pm.totalOfTuplesPendingRemoved_was_lt_MaxOffsetBehind());

    	    _metrics_to_report.setTotalRunningValue("partitionEmittedTotal", partitionMetricName + "/emitted", pm.totalOfTuplesEmitted());
    		
    	    _metrics_partition_level_to_aggregate.setGaugesCurrentValue("partitionPendingTotal", partitionMetricName + "/pending", pm.currentCountOfTuplesPending());
    	
    	    _metrics_partition_level_to_aggregate.setGaugesCurrentValue("partitionWaitingToEmitNow", partitionMetricName + "/waitingToEmit", pm.currentCountOfTuplesWaitingToEmit());
    	    
    	    _metrics_partition_level_to_aggregate.setTotalRunningValue("partitionTuplesRemovedBCMaxOffsetBehind-Ackd",partitionMetricName + "/ackd/removedFromPendingBCMaxOffsetBehind", pm.totalOfTuplesAckSkipped_was_lt_MaxOffsetBehind());
    	    
    	    _metrics_partition_level_to_aggregate.setTotalRunningValue("partitionTuplesRemovedBCMaxOffsetBehind-Failed", partitionMetricName + "/failed/removedFromPendingBCMaxOffsetBehind", pm.totalOfTuplesFailedSkipped_was_lt_MaxOffsetBehind());

    	    _metrics_partition_level_to_aggregate.setGaugesCurrentValue("partionTuplesAwaitingRetry", partitionMetricName + "/failed/retries/awaiting", pm.currentCountOfTuplesWaitingToRetry());
    	    
    	    _metrics_partition_level_to_aggregate.setGaugesCurrentValue("partionTuplesCurrentlyRetrying", partitionMetricName + "/failed/retries/inTopology", pm.currentCountOfTuplesRetrying());
    	    
    	    _metrics_partition_level_to_aggregate.setTotalRunningValue("partitionTuplesSuccessfullyRetried", partitionMetricName + "/failed/retries/succesful", pm.totalOfTuplesSuccessfullyRetried());
    	    
    	    _metrics_to_report.setTotalRunningValue("partionRetriedTotal", partitionMetricName + "/failed/retries", pm.totalOfRetryAttempts());
    	    	
			for (Map.Entry<String, Double> e : pm.kafkaErrorMetric().RubyMetrics().getMetricsCurrentValueOnly().entrySet() )
			{
				// because m.getMetricPath() should be the same for every partition ( without partition specific information )
				//		this should act as a topology / spout level counter...
				_metrics_to_report_kafkaErrors.incrBy(metricPath (e.getKey()), e.getValue());	
				//reporting count of errors per partition makes it very easy to see which broker is giving problems.
				_metrics_to_report_kafkaErrors.setTotalRunningValue(partitionMetricName + "/" + e.getKey(), e.getValue()); 		
			}
    	} 
    	
    	// if we only counted "current" partition managers
    	//		, tuplesEmittedTotal / tupleRetriesTotal *could* decrease
    	//		if partition managers are removed.
    	_tuplesEmittedTotal = totalOfRubyMetricsByClassification(_metrics_to_report, "partitionEmittedTotal");
    	_tupleRetriesTotal = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partionRetriedTotal");	
    	_tuplesTotal_Pending = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partitionPendingTotal");
    	_tuplesNow_WaitingToEmit = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partitionWaitingToEmitNow");
    	
        _tuplesTotal_RemovedBCMaxOffsetBehind_Pending = totalOfRubyMetricsByClassification(_metrics_to_report, "partitionTuplesRemovedBCMaxOffsetBehind");
        _tuplesTotal_RemovedBCMaxOffsetBehind_Ackd = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partitionTuplesRemovedBCMaxOffsetBehind-Ackd");
        _tuplesTotal_RemovedBCMaxOffsetBehind_Failed = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partitionTuplesRemovedBCMaxOffsetBehind-Failed");
        _tuplesTotal_Failed_Retry_Awaiting = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partionTuplesAwaitingRetry");
        _tuplesTotal_Failed_Retry_Retrying = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partionTuplesCurrentlyRetrying");
        _tuplesTotal_Failed_Retry_Successful = totalOfRubyMetricsByClassification(_metrics_partition_level_to_aggregate, "partitionTuplesSuccessfullyRetried");
    }  


    
    
    private long totalOfRubyMetricsByClassification(RubeGoldbergMetrics metrics, String metricClassification )
    {
    	long ret = 0;
    	for ( RubeGoldbergMetric rm : metrics.getRubyMetricsXMetricClassification(metricClassification))
    	{
    		ret = ret + (long)rm.getCurrentValue();
    	}
    	return ret;
    }
    
    
    private static void registerMetric_KafkaOffset(final TopologyContext context, final SpoutConfig spoutConfig, final DynamicPartitionConnections connections, final PartitionCoordinator coordinator)
    {
        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(spoutConfig.topic, connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = coordinator.getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet<Partition>();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, spoutConfig.metricsTimeBucketSizeInSecs);


    }
    private static void registerMetric_KafkaPartition(final TopologyContext context, final SpoutConfig spoutConfig, final PartitionCoordinator coordinator)
    {
        context.registerMetric("kafkaPartition", new IMetric() {
            @SuppressWarnings("unchecked")
			@Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = coordinator.getMyManagedPartitions();
                @SuppressWarnings("rawtypes")
				Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, spoutConfig.metricsTimeBucketSizeInSecs);
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
