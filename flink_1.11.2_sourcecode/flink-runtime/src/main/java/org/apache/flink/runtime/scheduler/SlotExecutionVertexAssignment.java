/*
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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The slot assignment for a {@link ExecutionVertex}.
 */
public class SlotExecutionVertexAssignment {

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： ExecutionVertexID
	 */
	private final ExecutionVertexID executionVertexId;

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： 申请 slot 的响应
	 */
	private final CompletableFuture<LogicalSlot> logicalSlotFuture;

	public SlotExecutionVertexAssignment(ExecutionVertexID executionVertexId, CompletableFuture<LogicalSlot> logicalSlotFuture) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
	}

	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	public CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
		return logicalSlotFuture;
	}
}
