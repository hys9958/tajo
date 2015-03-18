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

package org.apache.tajo.storage.kafka.fragment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.StorageFragmentProtos.KafkaFragmentProto;

import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class KafkaFragment implements Fragment, Comparable<KafkaFragment>, Cloneable {
	private static final Log LOG = LogFactory.getLog(KafkaFragment.class);
	@Expose 
	private String tableName; 
	@Expose
	private String topicName;
	@Expose
	private String brokers;
	@Expose
	private int brokerPort;
	@Expose
	private long offset = -1;
	@Expose
	private int numOfreads;
	
	public KafkaFragment(String tableName, String topicName, String brokers, int brokerPort, long offset, int numOfreads) {
		this.tableName = tableName;
		this.topicName = topicName;
		this.brokers = brokers;
		this.brokerPort = brokerPort;
		this.offset = offset;
		this.numOfreads = numOfreads;
	}
	public String getTopicName() {
		return this.topicName;
	}
	public String getBrokers() {
		return this.brokers;
	}
	public int getBrokerPort() {
		return this.brokerPort;
	}
	public long getOffset() {
		return this.offset;
	}
	public int getNumOfreads() {
		return this.numOfreads;
	}
	public KafkaFragment(ByteString raw) throws InvalidProtocolBufferException {
		KafkaFragmentProto.Builder builder = KafkaFragmentProto.newBuilder();
		builder.mergeFrom(raw);
	    builder.build();
	    init(builder.build());
	}
	  
	private void init(KafkaFragmentProto proto) {
	    this.topicName = proto.getTopicName();
	    this.brokers = proto.getBrokers();
	    this.brokerPort = proto.getBrokerPort();
	    this.offset = proto.getOffset();
	    this.numOfreads = proto.getNumOfreads();
    }
	  
	@Override
	public int compareTo(KafkaFragment o) {
		LOG.info(">>>>>>compareTo");
		return 0;
	}

	@Override
	public String getTableName() {
		LOG.info(">>>>>>getTableName");
		return tableName;
	}

	@Override
	public FragmentProto getProto() {
		LOG.info(">>>>>>getProto");
		KafkaFragmentProto.Builder builder = KafkaFragmentProto.newBuilder();
		builder.setTopicName(topicName);
		builder.setBrokers(brokers);
		builder.setBrokerPort(brokerPort);
		builder.setOffset(offset);
		builder.setNumOfreads(numOfreads);
	    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
	    fragmentBuilder.setId(this.tableName);
	    fragmentBuilder.setContents(builder.buildPartial().toByteString());
	    fragmentBuilder.setStoreType(CatalogUtil.getStoreTypeString(StoreType.KAFKA));
	    return fragmentBuilder.build();
	}

	@Override
	public long getLength() {
		LOG.info(">>>>>>getLength");
		return 0;
	}

	@Override
	public String getKey() {
		LOG.info(">>>>>>getKey");
		return null;
	}

	@Override
	public String[] getHosts() {
		LOG.info(">>>>>>getHosts");
		return null;
	}

	@Override
	public boolean isEmpty() {
		LOG.info(">>>>>>isEmpty");
		return offset == -1;
	}

}
