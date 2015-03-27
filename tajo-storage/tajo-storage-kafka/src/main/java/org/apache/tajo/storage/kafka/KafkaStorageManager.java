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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.fragment.KafkaFragment;

/**
 * StorageManager for Kafka topic.
 */
public class KafkaStorageManager extends StorageManager {
	private static final Log LOG = LogFactory.getLog(KafkaStorageManager.class);
	private Map<String, SimpleConsumerManager> connMap = new HashMap<String, SimpleConsumerManager>();
	
	public KafkaStorageManager(StoreType storeType) {
		super(storeType);
	}
	
	@Override
	public void closeStorageManager() {
	    synchronized (connMap) {
	        for (SimpleConsumerManager eachConn: connMap.values()) {
	          try {
	            eachConn.close();
	          } catch (Exception e) {
	            LOG.error(e.getMessage(), e);
	          }
	        }
	      }
	}
	
	@Override
	protected void storageInit() throws IOException {
	}

	@Override
	public void createTable(TableDesc tableDesc, boolean ifNotExists)
			throws IOException {
	    TableStats stats = new TableStats();
	    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
	    tableDesc.setStats(stats);
	}

	@Override
	public void purgeTable(TableDesc tableDesc) throws IOException {
		//TODO: Delete a topic of kafka correspond Table.
	}

	@Override
	public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
			ScanNode scanNode) throws IOException {
		return getFragmentList(tableDesc);
	}

	@Override
	public List<Fragment> getNonForwardSplit(TableDesc tableDesc,
			int currentPage, int numFragments) throws IOException {
		if(currentPage > 0){
			return new ArrayList<Fragment>(1);
		}else{
			return getFragmentList(tableDesc);
		}
	}

	@Override
	public StorageProperty getStorageProperty() {
	    StorageProperty storageProperty = new StorageProperty();
	    storageProperty.setSortedInsert(false);
	    storageProperty.setSupportsInsertInto(false);
	    return storageProperty;
	}

	@Override
	public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
			TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs,
			TupleRange dataRange) throws IOException {
		return null;
	}

	@Override
	public void beforeInsertOrCATS(LogicalNode node) throws IOException {
	}

	@Override
	public void rollbackOutputCommit(LogicalNode node) throws IOException {
	}
	
	private List<Fragment> getFragmentList(TableDesc tableDesc) throws IOException{
		String topic = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC);
		String brokers = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER);
		String partitions = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC_PARTITION,"ALL_PARTITION");
		Set<Integer> partitionSet = new HashSet<Integer>();
		if(partitions.equals("ALL_PARTITION")){
			partitionSet = SimpleConsumerManager.getPartitions(brokers, topic);
		} else{
	        for(String partitionId : partitions.split(",")){
	        	partitionSet.add(Integer.parseInt(partitionId));
	        }
		}
        List<Fragment> fragments = new ArrayList<Fragment>();  
        for(Integer partitionId : partitionSet){
        	SimpleConsumerManager simpleConsumerManager = getConnection(brokers, topic, partitionId);
                long lastOffset = simpleConsumerManager.getReadOffset(kafka.api.OffsetRequest.LatestTime());
                long startOffset = simpleConsumerManager.getReadOffset(kafka.api.OffsetRequest.EarliestTime());
                long messageSize = lastOffset - startOffset;
                if(0 == lastOffset || 0 == messageSize) continue;
                if(messageSize <= KafkaStorageConstants.FRAGMENT_SIZE){
    			    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, brokers, startOffset, lastOffset);
    			    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
    			    fragments.add(fragment);
                }else{
    				long nextFragmentStartOffset = startOffset;
    				while(nextFragmentStartOffset < lastOffset){
    					long nextFragmentlastOffset = nextFragmentStartOffset + KafkaStorageConstants.FRAGMENT_SIZE;
    				    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, brokers, nextFragmentStartOffset, nextFragmentlastOffset);
    				    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
    				    fragments.add(fragment);
    				    nextFragmentStartOffset = nextFragmentlastOffset;
    				}
                }
        }
        return fragments;
	}
	
	public SimpleConsumerManager getConnection(String seedBrokers, String topic, int partition) throws IOException {
		String conKey = topic + "_" + partition;
	    synchronized(connMap) {
	    	SimpleConsumerManager conn = connMap.get(conKey);
	      if (conn == null) {
	        conn = SimpleConsumerManager.getSimpleConsumerManager(seedBrokers, topic, partition);
	        connMap.put(conKey, conn);
	      }
	      return conn;
	    }
	  }
}
