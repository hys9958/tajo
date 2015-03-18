/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;


public class KafKaSimpleConsumer {
    static final int CONSUMER_TIMEOUT = 30000;
    static final int CONSUMER_BUFFER_SIZE = 64 * 1024;
    static final int CONSUMER_FETCH_SIZE = 300 * 1024;
    static String clientId = "Client_";
    
    private SimpleConsumer consumer = null;
	private List<String> replicaBrokers = new ArrayList<String>();
    private String topic;
    private int partition;
    private String clientName;
	
	public KafKaSimpleConsumer(List<String> seedBrokers, int port, String topic, int partition) {
		replicaBrokers = new ArrayList<String>();
		this.topic = topic;
		this.partition = partition;
		
        PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        clientName = clientId + topic + "_" + partition;

        consumer = new SimpleConsumer(leadBroker, port, CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE, clientName);
	}
	
    public void close(){
    	if(null != consumer){
    		consumer.close();
    	}
    	consumer = null;
    }
    
    /**
     * TODO: Retry when fetch failure.
     * @param offset
     * @return
     */
    @SuppressWarnings("unchecked")
	public List<MessageAndOffset> fetch(long offset){
    	List<MessageAndOffset> returnData = null;
        FetchRequest req = new FetchRequestBuilder()
        .clientId(clientId)
        .addFetch(topic, partition, offset, CONSUMER_FETCH_SIZE)
        .build();
        if(null != consumer){
            FetchResponse fetchResponse = consumer.fetch(req); 
            Iterator<MessageAndOffset> messages = fetchResponse.messageSet(topic, partition).iterator();
            returnData = IteratorUtils.toList(messages);
        }
        return returnData;
    }
    
	public long getReadOffset(long whichTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
	
//    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
//        for (int i = 0; i < 3; i++) {
//            boolean goToSleep = false;
//            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
//            if (metadata == null) {
//                goToSleep = true;
//            } else if (metadata.leader() == null) {
//                goToSleep = true;
//            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
//                // first time through if the leader hasn't changed give ZooKeeper a second to recover
//                // second time, assume the broker did recover before failover, or it was a non-Broker issue
//                //
//                goToSleep = true;
//            } else {
//                return metadata.leader().host();
//            }
//            if (goToSleep) {
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ie) {
//                }
//            }
//        }
//        System.out.println("Unable to find new leader after Broker failure. Exiting");
//        throw new Exception("Unable to find new leader after Broker failure. Exiting");
//    }
    
    private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE, clientId+"leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                //call to topicsMetadata() asks the Broker you are connected to for all the details about the topic we are interested in
                List<TopicMetadata> metaData = resp.topicsMetadata();
                //loop on partitionsMetadata iterates through all the partitions until we find the one we want.
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        // add replica broker info to m_replicaBrokers
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
    public static void main(String args[]) {
    	KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(new ArrayList<String>(Arrays.asList("localhost".split(","))),
    			9092, "kafka_test", 0);
    	simpleConsumer.fetch((int)0);
    	simpleConsumer.close();    	
    }

}
