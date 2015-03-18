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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.message.MessageAndOffset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.EmptyTuple;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.fragment.KafkaFragment;
import org.apache.tajo.storage.kafka.serDe.KafkaSerializerDeserializer;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;

public class KafkaScanner implements Scanner {
	private static final Log LOG = LogFactory.getLog(KafkaScanner.class);
	private TajoConf conf;
	private Schema schema;
	private TableMeta meta;
	private TableStats tableStats;
	private KafkaFragment fragment;
	private Column[] targets;
	private int[] targetColumnIndexes;
	private TextLineDeserializer deserializer;
	private AtomicBoolean finished = new AtomicBoolean(false);
	private List<MessageAndOffset> messages = null;
	private int messageIndex = 0;
	protected boolean inited = false;
	
	public KafkaScanner (Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
		this.conf = (TajoConf)conf;
	    this.schema = schema;
	    this.meta = meta;
	    this.fragment = (KafkaFragment)fragment;
	    this.tableStats = new TableStats();
	}
	
	/**
	 * Read message from kafka.
	 * @param messageSize
	 */
	private void readMessage() {
		int messageSize = fragment.getNumOfreads();
		int messageCount = 0;
		long offset = fragment.getOffset();
		KafKaSimpleConsumer simpleConsumer = new KafKaSimpleConsumer(new ArrayList<String>(Arrays.asList(fragment.getBrokers().split(","))),
				fragment.getBrokerPort(), fragment.getTopicName(), KafKaSimpleConsumer.PARTITION);
		try{
			while(messageCount < messageSize){
				List<MessageAndOffset> messages = simpleConsumer.fetch(offset);
				if(null == messages || messages.size() == 0 || messageSize <= messageCount) {
					break;
				}
				int readCount = messageSize - messageCount;
				if(readCount < messages.size()){
					MessageAndOffset message = null;
					for(int i = 0; i<readCount; i++){
						message = messages.get(i);
						this.messages.add(message);
						messageCount++;
					}
					if( null != message) offset = message.offset()+1;
				}else{
					this.messages.addAll(messages);
					offset = messages.get(messages.size()-1).offset()+1;
					messageCount += messages.size();
				}
			}			
		} finally{
			simpleConsumer.close();
		}
	}
	
	@Override
	public Schema getSchema() {
		LOG.info(">>>>>>getSchema");
		return null;
	}

	@Override
	public void init() throws IOException {
		LOG.info(">>>>>>init");	
		messages = new ArrayList<MessageAndOffset>();
		inited = true;
	    if (targets == null) {
	       targets = schema.toArray();
	    }
	    targetColumnIndexes = new int[targets.length];
	    for (int i = 0; i < targets.length; i++) {
	      targetColumnIndexes[i] = schema.getColumnId(targets[i].getQualifiedName());
	    }
	    Arrays.sort(targetColumnIndexes);
	    deserializer = KafkaSerializerDeserializer.getTextSerde(meta).createDeserializer(schema, meta, targetColumnIndexes);
	    deserializer.init();
	    readMessage();
	}

	@Override
	public Tuple next() throws IOException {
		LOG.info(">>>>>>next");
	    if (finished.get()) {
	        return null;
	      }
	    if(null == messages || messages.size() <= messageIndex){
	    	return null;
	    }
	    VTuple tuple;
        // this loop will continue until one tuple is build or EOS (end of stream).
	    MessageAndOffset message = messages.get(messageIndex);
	    ByteBuf buf = Unpooled.copiedBuffer(message.message().payload());
        // if no more line, then return EOT (end of tuple)
        if (buf == null) {
           return null;
        }
        // If there is no required column, we just read each line
        // and then return an empty tuple without parsing line.
        if (targets.length == 0) {
          return EmptyTuple.get();
        }

        tuple = new VTuple(schema.size());

        try {
          deserializer.deserialize(buf, tuple);

        } catch (TextLineParsingError tae) {
        	throw new IOException(tae);
        }
        messageIndex++;
        return tuple;
	}

	@Override
	public void reset() throws IOException {
		finished.set(false);
		LOG.info(">>>>>>reset");	
	}

	@Override
	public void close() throws IOException {
		finished.set(true);
//		messages.clear();
		messages = null;
		LOG.info(">>>>>>close");
	}

	@Override
	public boolean isProjectable() {
		LOG.info(">>>>>>isProjectable");
		return true;
	}

	@Override
	public void setTarget(Column[] targets) {
		LOG.info(">>>>>>setTarget");
	    if (inited) {
	        throw new IllegalStateException("Should be called before init()");
	    }
	    this.targets = targets;
	}

	@Override
	public boolean isSelectable() {
		LOG.info(">>>>>>isSelectable");
		return false;
	}

	@Override
	public void setSearchCondition(Object expr) {
		LOG.info(">>>>>>setSearchCondition");
		
	}

	@Override
	public boolean isSplittable() {
		LOG.info(">>>>>>isSplittable");
		return false;
	}

	@Override
	public float getProgress() {
		LOG.info(">>>>>>getProgress");
		return 0;
	}

	@Override
	public TableStats getInputStats() {
		LOG.info(">>>>>>getInputStats");
		return null;
	}

}
