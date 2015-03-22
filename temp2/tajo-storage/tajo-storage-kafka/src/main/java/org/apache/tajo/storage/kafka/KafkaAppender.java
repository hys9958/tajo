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

import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Tuple;

public class KafkaAppender implements Appender {

	@Override
	public void init() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addTuple(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getEstimatedOutputSize() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void enableStats() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TableStats getStats() {
		// TODO Auto-generated method stub
		return null;
	}

}