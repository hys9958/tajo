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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
	
	public KafkaStorageManager(StoreType storeType) {
		super(storeType);
	}
	
	@Override
	public void closeStorageManager() {
		LOG.info(">>>>>closeStorageManager");
	}
	
	@Override
	protected void storageInit() throws IOException {
		LOG.info(">>>>>storageInit");
		
	}

	@Override
	public void createTable(TableDesc tableDesc, boolean ifNotExists)
			throws IOException {
	    TableStats stats = new TableStats();
	    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
	    tableDesc.setStats(stats);
		LOG.info(">>>>>createTable+tableDesc");
	}

	@Override
	public void purgeTable(TableDesc tableDesc) throws IOException {
		LOG.info(">>>>>purgeTable");
	}

	@Override
	public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
			ScanNode scanNode) throws IOException {
		LOG.info(">>>>>getSplits");
		return getFragmentList(tableDesc);
	}

	@Override
	public List<Fragment> getNonForwardSplit(TableDesc tableDesc,
			int currentPage, int numFragments) throws IOException {
		LOG.info(">>>>>getNonForwardSplit");
		if(currentPage > 0){
			return new ArrayList<Fragment>(1);
		}else{
			return getFragmentList(tableDesc);
		}
	}

	@Override
	public StorageProperty getStorageProperty() {
		LOG.info(">>>>>getStorageProperty");
	    StorageProperty storageProperty = new StorageProperty();
	    storageProperty.setSortedInsert(false);
	    storageProperty.setSupportsInsertInto(false);
	    return storageProperty;
	}

	@Override
	public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
			TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs,
			TupleRange dataRange) throws IOException {
		LOG.info(">>>>>getInsertSortRanges");
		return null;
	}

	@Override
	public void beforeInsertOrCATS(LogicalNode node) throws IOException {
		LOG.info(">>>>>beforeInsertOrCATS");
	}

	@Override
	public void rollbackOutputCommit(LogicalNode node) throws IOException {
		LOG.info(">>>>>rollbackOutputCommit");
	}
	
	private List<Fragment> getFragmentList(TableDesc tableDesc){
		String topic = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC);
		String broker = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER);
		int brokerPort = Integer.parseInt(tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER_PORT));
		String partitions = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC_PARTITION,"ALL_PARTITION");
		ArrayList<String> brokerList = new ArrayList<String>(Arrays.asList(broker.split(",")));
		Set<Integer> partitionSet = new HashSet<Integer>();
		if(partitions.equals("ALL_PARTITION")){
			partitionSet = KafKaSimpleConsumer.getPartitions(brokerList, brokerPort, topic);
		} else{
	        for(String partitionId : partitions.split(",")){
	        	partitionSet.add(Integer.parseInt(partitionId));
	        }
		}
        List<Fragment> fragments = new ArrayList<Fragment>();  
        for(Integer partitionId : partitionSet){
                KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(brokerList,
                                brokerPort, topic, partitionId);
                long lastOffset = simpleConsumer.getReadOffset(kafka.api.OffsetRequest.LatestTime());
                long startOffset = simpleConsumer.getReadOffset(kafka.api.OffsetRequest.EarliestTime());
                simpleConsumer.close();
                long messageSize = lastOffset - startOffset;
                if(0 == lastOffset || 0 == messageSize) continue;
                if(messageSize <= KafkaStorageConstants.FRAGMENT_SIZE){
    			    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, broker, brokerPort, startOffset, lastOffset);
    			    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
    			    fragments.add(fragment);
                }else{
    				long nextFragmentStartOffset = startOffset;
    				while(nextFragmentStartOffset < lastOffset){
    					long nextFragmentlastOffset = nextFragmentStartOffset + KafkaStorageConstants.FRAGMENT_SIZE;
    				    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, broker, brokerPort, nextFragmentStartOffset, nextFragmentlastOffset);
    				    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
    				    fragments.add(fragment);
    				    nextFragmentStartOffset = nextFragmentlastOffset;
    				}
                }
        }
        return fragments;
	}
}