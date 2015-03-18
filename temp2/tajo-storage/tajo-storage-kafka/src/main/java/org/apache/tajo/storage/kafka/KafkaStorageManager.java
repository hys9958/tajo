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
import java.util.List;

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
		LOG.info(">>>>>KafkaStorageManager+storeType: "+storeType.name());
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
		LOG.info(">>>>>createTable+tableDesc: "+tableDesc.toString());
	}

	@Override
	public void purgeTable(TableDesc tableDesc) throws IOException {
		LOG.info(">>>>>purgeTable");
	}

	@Override
	public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
			ScanNode scanNode) throws IOException {
		
		String topic = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC);
		String broker = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER);
		int brokerPort = Integer.parseInt(tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER_PORT));
		String partition = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC_PARTITION);
		ArrayList<String> brokerList = new ArrayList<String>(Arrays.asList(broker.split(",")));
		
		List<Fragment> fragments = new ArrayList<Fragment>();	
		for(String topicPartition : partition.split(",")){
			int partitionId = Integer.parseInt(topicPartition);
			KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(brokerList,
					brokerPort, topic, partitionId);
			long lastOffset = simpleConsumer.getReadOffset(kafka.api.OffsetRequest.LatestTime());
			simpleConsumer.close();
			if(0 == lastOffset) continue;
		    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, broker, brokerPort, 0L, lastOffset);
		    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
		    fragments.add(fragment);
		}
		
	    return fragments;
	}

	@Override
	public List<Fragment> getNonForwardSplit(TableDesc tableDesc,
			int currentPage, int numFragments) throws IOException {
		String topic = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC);
		String broker = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER);
		int brokerPort = Integer.parseInt(tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_BROKER_PORT));
		String partition = tableDesc.getMeta().getOption(KafkaStorageConstants.KAFKA_TOPIC_PARTITION);
		ArrayList<String> brokerList = new ArrayList<String>(Arrays.asList(broker.split(",")));
		
		List<Fragment> fragments = new ArrayList<Fragment>();	
		for(String topicPartition : partition.split(",")){
			int partitionId = Integer.parseInt(topicPartition);
			KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(brokerList,
					brokerPort, topic, partitionId);
			long lastOffset = simpleConsumer.getReadOffset(kafka.api.OffsetRequest.LatestTime());
			simpleConsumer.close();
			if(0 == lastOffset) continue;
		    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, partitionId, broker, brokerPort, 0L, lastOffset);
		    fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
		    fragments.add(fragment);
		}
		
	    return fragments;
	}

	@Override
	public StorageProperty getStorageProperty() {
		LOG.info(">>>>>getStorageProperty");
		return null;
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
}
