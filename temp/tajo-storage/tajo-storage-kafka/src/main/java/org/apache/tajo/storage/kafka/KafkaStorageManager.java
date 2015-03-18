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
		LOG.info(">>>>>getSplits+scanNode: "+scanNode.toString());
		return null;
	}

	@Override
	public List<Fragment> getNonForwardSplit(TableDesc tableDesc,
			int currentPage, int numFragments) throws IOException {
		
		String topic = tableDesc.getMeta().getOption("topic");
		String brokers = tableDesc.getMeta().getOption("brokers");
		int brokerPort = Integer.parseInt(tableDesc.getMeta().getOption("brokerPort"));
		
		KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(new ArrayList<String>(Arrays.asList(brokers.split(","))),
					brokerPort, topic, 0);
		
		long lastOffset = simpleConsumer.getReadOffset(kafka.api.OffsetRequest.LatestTime());
		simpleConsumer.close();
		
	    List<Fragment> fragments = new ArrayList<Fragment>(1);
	    
	    int start = currentPage * numFragments;
	    if (start >= lastOffset) {
	      return new ArrayList<Fragment>(1);
	    }
//	    long end = (currentPage + 1) * numFragments;
//	    if (end > lastOffset) {
//	        end = lastOffset;
//	    }
	      
	    KafkaFragment fragment = new KafkaFragment(tableDesc.getName(), topic, brokers, brokerPort, start, numFragments);
	    fragments.add(fragment);
	    LOG.debug("getFragments: fragment -> " + fragment);
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
