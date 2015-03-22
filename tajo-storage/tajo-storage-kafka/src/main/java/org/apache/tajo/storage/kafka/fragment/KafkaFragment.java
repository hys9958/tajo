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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class KafkaFragment implements Fragment, Comparable<KafkaFragment>,
		Cloneable {
	private static final Log LOG = LogFactory.getLog(KafkaFragment.class);
	@Expose
	private String tableName;
	@Expose
	private String topicName;
	@Expose
	private int partitionId;
	@Expose
	private String brokers;
	@Expose
	private int brokerPort;
	@Expose
	private long startOffset = -1;
	@Expose
	private long lastOffset;
	@Expose
	private long length = -1;

	public KafkaFragment(String tableName, String topicName, int partitionId,
			String brokers, int brokerPort, long startOffset, long lastOffset) {
		this.tableName = tableName;
		this.topicName = topicName;
		this.partitionId = partitionId;
		this.brokers = brokers;
		this.brokerPort = brokerPort;
		this.startOffset = startOffset;
		this.lastOffset = lastOffset;
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

	public long getStartOffset() {
		return this.startOffset;
	}

	public long getLastOffset() {
		return this.lastOffset;
	}

	public int getPartitionId() {
		return this.partitionId;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public KafkaFragment(ByteString raw) throws InvalidProtocolBufferException {
		KafkaFragmentProto.Builder builder = KafkaFragmentProto.newBuilder();
		builder.mergeFrom(raw);
		builder.build();
		init(builder.build());
	}

	private void init(KafkaFragmentProto proto) {
		this.topicName = proto.getTopicName();
		this.partitionId = proto.getPartitionId();
		this.brokers = proto.getBrokers();
		this.brokerPort = proto.getBrokerPort();
		this.startOffset = proto.getStartOffset();
		this.lastOffset = proto.getLastOffset();
		this.length = proto.getLength();
	}

	@Override
	public int compareTo(KafkaFragment o) {
		LOG.info(">>>>>>compareTo");
		if (this.topicName.equals(o.topicName)
				&& this.partitionId == o.partitionId) {
			long diff = this.startOffset - o.startOffset;
			if (diff < 0) {
				return -1;
			} else if (diff > 0) {
				return 1;
			} else {
				return 0;
			}
		} else {
			return -1;
		}
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
		builder.setPartitionId(partitionId);
		builder.setBrokers(brokers);
		builder.setBrokerPort(brokerPort);
		builder.setStartOffset(startOffset);
		builder.setLastOffset(lastOffset);
		builder.setLength(length);
		FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
		fragmentBuilder.setId(this.tableName);
		fragmentBuilder.setContents(builder.buildPartial().toByteString());
		fragmentBuilder.setStoreType(CatalogUtil
				.getStoreTypeString(StoreType.KAFKA));
		return fragmentBuilder.build();
	}

	@Override
	public long getLength() {
		LOG.info(">>>>>>getLength");
		return this.length;
	}

	@Override
	public String getKey() {
		LOG.info(">>>>>>getKey");
		return this.topicName + "_" + this.partitionId + "_" + this.startOffset;
	}

	@Override
	public String[] getHosts() {
		LOG.info(">>>>>>getHosts");
	      return new String[0];
	}

	@Override
	public boolean isEmpty() {
		LOG.info(">>>>>>isEmpty");
		return startOffset == -1;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof KafkaFragment) {
			KafkaFragment t = (KafkaFragment) o;
			if (getKey().equals(t.getKey())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getKey());
	}

	@Override
	public String toString() {
		return "\"fragment\": {\"id\": \"" + tableName + "\", \"topic\": "
				+ getTopicName() + "\", \"partition\": " + getPartitionId()
				+ ",\"startOffset\": " + getStartOffset() + ",\"lastOffset\": "
				+ getLastOffset() + "}";
	}
}