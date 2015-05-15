/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.MaxMetric;

import java.util.*;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final KafkaErrorMetric _kErrorMetric ;
    
    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;
    Long _emittedToOffset;
    // _pending key = Kafka offset, value = time at which the message was first submitted to the topology
    private SortedMap<Long,Long> _pending = new TreeMap<Long,Long>();
    private final FailedMsgRetryManager _failedMsgRetryManager;

    // retryRecords key = Kafka offset, value = retry info for the given message
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    Partition _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;
    long numberFailed, numberAcked, numFailedSkipped_was_lt_maxOffsetBehind, numRemovedFromPendingBCMaxOffsetBehind, numAckedSkipped_was_lt_maxOffsetBehind, numEmitted ;
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, Partition id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        _stormConf = stormConf;
        numberAcked = numberFailed = numFailedSkipped_was_lt_maxOffsetBehind = numRemovedFromPendingBCMaxOffsetBehind = numAckedSkipped_was_lt_maxOffsetBehind = numEmitted = 0;
        _kErrorMetric = new KafkaErrorMetric("");
        
        
        long retryInitialDelayMs = 10l;
        double retryDelayMultiplier = 10d;
        long retryDelayMaxMs = Long.MAX_VALUE;
        
        _failedMsgRetryManager = new ExponentialBackoffMsgRetryManager(retryInitialDelayMs,
														        		retryDelayMultiplier,
														        		retryDelayMaxMs);

        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        Long currentOffset = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig);

        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = currentOffset;
            LOG.info("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }

        if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
            LOG.info("Last commit offset from zookeeper: " + _committedTo);
            Long lastCommittedOffset = _committedTo;
            _committedTo = currentOffset;
            LOG.info("Commit offset " + lastCommittedOffset + " is more than " +
                    spoutConfig.maxOffsetBehind + " behind latest offset " + currentOffset + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put( partitionMetricPath (_partition , "fetchAPILatencyMax" ), _fetchAPILatencyMax.getValueAndReset());
        ret.put( partitionMetricPath (_partition , "fetchAPILatencyMean" ), _fetchAPILatencyMean.getValueAndReset());
        ret.put( partitionMetricPath (_partition ,  "fetchAPICallCount" ), _fetchAPICallCount.getValueAndReset());
        ret.put( partitionMetricPath (_partition , "fetchAPIMessageCount" ), _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    
 
    private String partitionMetricPath(Partition p, String key)
    {
    	return _spoutConfig.topic + "/" +  p.host.toString() + "/partition_" + p.partition + "/" + key ;
    }
    

    
    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector) {
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = KafkaUtils.generateTuples(_spoutConfig, toEmit.msg);
            if (tups != null) {
                for (List<Object> tup : tups) {
                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                    numEmitted++;
                }
                break;
            } else {
                ack(toEmit.offset);
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }


    private void fill() {
        long start = System.nanoTime();
        Long offset;

        // Are there failed tuples? If so, fetch those first.
        offset = this._failedMsgRetryManager.nextFailedMessageToRetry();
        final boolean processingNewTuples = (offset == null);
        if (processingNewTuples) {
            offset = _emittedToOffset;
        }

        ByteBufferMessageSet msgs = null;
        try {
            msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset, _kErrorMetric);
        } catch (UpdateOffsetException e) {
            _emittedToOffset = KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition, kafka.api.OffsetRequest.EarliestTime());
            LOG.warn("Using new offset: {}", _emittedToOffset);
            // fetch failed, so don't update the metrics
            return;
        }
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        if (msgs != null) {
            int numMessages = 0;

            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                if (processingNewTuples || this._failedMsgRetryManager.shouldRetryMsg(cur_offset)) {
                    numMessages += 1;
                    if (!_pending.containsKey(cur_offset)) {
                        _pending.put(cur_offset, System.currentTimeMillis());
                    }
                    _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
                    if (_failedMsgRetryManager.shouldRetryMsg(cur_offset)) {
                        this._failedMsgRetryManager.retryStarted(cur_offset);
                    }
                }
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    
    
    public void ack(Long offset) {
        if (!_pending.isEmpty() && _pending.firstKey() < offset - _spoutConfig.maxOffsetBehind) {
            // Too many things pending!
        	SortedMap toRemove = _pending.headMap(offset - _spoutConfig.maxOffsetBehind);
        	numRemovedFromPendingBCMaxOffsetBehind = numRemovedFromPendingBCMaxOffsetBehind + toRemove.size();
        	toRemove.clear(); //changes in toRemove are reflected in _pending which backs toRemove.
        	numAckedSkipped_was_lt_maxOffsetBehind ++; //only 1 of these records was actually an ack...
        }
        _pending.remove(offset);
        this._failedMsgRetryManager.acked(offset);
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < _emittedToOffset - _spoutConfig.maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + _spoutConfig.maxOffsetBehind +
                            " behind _emittedToOffset=" + _emittedToOffset
            );
            numFailedSkipped_was_lt_maxOffsetBehind++;
            _pending.remove(offset);
            numRemovedFromPendingBCMaxOffsetBehind++;
        } else {
            LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pending.size() + " pending and _emittedToOffset=" + _emittedToOffset);
            numberFailed++;
            if (numberAcked == 0 && numberFailed > _spoutConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }

            this._failedMsgRetryManager.failed(offset);
        }
    }

    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (_committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId,
                            "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host,
                            "port", _partition.host.port))
                    .put("topic", _spoutConfig.topic).build();
            _state.writeJSON(committedPath(), data);

            _committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        } else {
            LOG.debug("No new offset for " + _partition + " for topology: " + _topologyInstanceId);
        }
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;
        } else {
            return _pending.firstKey();
        }
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
    
    
    public int currentCountOfTuplesPending() { return this._pending.size() ; }
    public int currentCountOfTuplesWaitingToEmit() { return this._waitingToEmit.size() ; }
    public long totalOfTuplesEmitted() {return this.numEmitted;}
    public long totalOfTuplesAcked() { return this.numberAcked ; }
    public long totalOfTuplesFailed() { return this.numberFailed ; }
    public long totalOfTuplesFailedSkipped_was_lt_MaxOffsetBehind() { return this.numFailedSkipped_was_lt_maxOffsetBehind ; }
    public long totalOfTuplesAckSkipped_was_lt_MaxOffsetBehind() { return this.numAckedSkipped_was_lt_maxOffsetBehind ; }
    public long totalOfTuplesPendingRemoved_was_lt_MaxOffsetBehind() { return this.numRemovedFromPendingBCMaxOffsetBehind ; }
    public int currentCountOfTuplesWaitingToRetry() { return this._failedMsgRetryManager.countOfTuplesWaitingToRetry(); }
    public int currentCountOfTuplesRetrying() {return this._failedMsgRetryManager.countOfTuplesRetrying(); }
    public long totalOfTuplesSuccessfullyRetried () { return this._failedMsgRetryManager.countOfTuplesSuccessfullyRetried();}
    public long totalOfRetryAttempts() { return this._failedMsgRetryManager.countOfRetryAttempts(); }
    public KafkaErrorMetric kafkaErrorMetric() { return this._kErrorMetric; }
}
