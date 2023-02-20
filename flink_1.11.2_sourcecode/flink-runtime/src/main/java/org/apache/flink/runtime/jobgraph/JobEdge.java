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

package org.apache.flink.runtime.jobgraph;

/**
 * // TODO 注释： 此类表示 job grap 中的边（communication channels）
 * This class represent edges (communication channels) in a job graph.
 *
 * // TODO 注释： edges 总是从 intermediate result partition 到 job vertex
 * The edges always go from an intermediate result partition to a job vertex.
 *
 * // TODO 注释： edge 通过其 {@link DistributionPattern} 进行参数化。
 * An edge is parametrized with its {@link DistributionPattern}.
 *
 * // TODO 注释： 代表了job graph中的一条数据传输通道。source 是 IntermediateDataSet，target 是 JobVertex。
 * // TODO 注释： 即数据通过 JobEdge 由 IntermediateDataSet 传递给目标 JobVertex
 */
public class JobEdge implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： 链接到下游的那个 JobVertex
	 */
	/** The vertex connected to this edge. */
	private final JobVertex target;

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： 分布式模式，有两种：
	 *  1、ALL_TO_ALL  上游每个 Task 跟下游每个 Task 都有链接
	 *  2、POINTWISE   上游 Task 不一定跟下游每个 Task 都有链接
	 */
	/** The distribution pattern that should be used for this job edge. */
	private final DistributionPattern distributionPattern;

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释：  这个 JobEdge 的中间数据集
	 */
	/** The data set at the source of the edge, may be null if the edge is not yet connected*/
	private IntermediateDataSet source;

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： 这个 JobEdge 的中间数据集 对应的 ID
	 */
	/** The id of the source intermediate data set */
	private IntermediateDataSetID sourceId;
	
	/** Optional name for the data shipping strategy (forward, partition hash, rebalance, ...),
	 * to be displayed in the JSON plan */
	private String shipStrategyName;

	/** Optional name for the pre-processing operation (sort, combining sort, ...),
	 * to be displayed in the JSON plan */
	private String preProcessingOperationName;

	/** Optional description of the caching inside an operator, to be displayed in the JSON plan */
	private String operatorLevelCachingDescription;
	
	/**
	 * Constructs a new job edge, that connects an intermediate result to a consumer task.
	 * 
	 * @param source The data set that is at the source of this edge.
	 * @param target The operation that is at the target of this edge.
	 * @param distributionPattern The pattern that defines how the connection behaves in parallel.
	 */
	public JobEdge(IntermediateDataSet source, JobVertex target, DistributionPattern distributionPattern) {
		if (source == null || target == null || distributionPattern == null) {
			throw new NullPointerException();
		}
		this.target = target;
		this.distributionPattern = distributionPattern;
		this.source = source;
		this.sourceId = source.getId();
	}
	
	/**
	 * Constructs a new job edge that refers to an intermediate result via the Id, rather than directly through
	 * the intermediate data set structure.
	 * 
	 * @param sourceId The id of the data set that is at the source of this edge.
	 * @param target The operation that is at the target of this edge.
	 * @param distributionPattern The pattern that defines how the connection behaves in parallel.
	 */
	public JobEdge(IntermediateDataSetID sourceId, JobVertex target, DistributionPattern distributionPattern) {
		if (sourceId == null || target == null || distributionPattern == null) {
			throw new NullPointerException();
		}
		this.target = target;
		this.distributionPattern = distributionPattern;
		this.sourceId = sourceId;
	}


	/**
	 * Returns the data set at the source of the edge. May be null, if the edge refers to the source via an ID
	 * and has not been connected.
	 * 
	 * @return The data set at the source of the edge
	 */
	public IntermediateDataSet getSource() {
		return source;
	}

	/**
	 * Returns the vertex connected to this edge.
	 * 
	 * @return The vertex connected to this edge.
	 */
	public JobVertex getTarget() {
		return target;
	}
	
	/**
	 * Returns the distribution pattern used for this edge.
	 * 
	 * @return The distribution pattern used for this edge.
	 */
	public DistributionPattern getDistributionPattern(){
		return this.distributionPattern;
	}
	
	/**
	 * Gets the ID of the consumed data set.
	 * 
	 * @return The ID of the consumed data set.
	 */
	public IntermediateDataSetID getSourceId() {
		return sourceId;
	}
	
	public boolean isIdReference() {
		return this.source == null;
	}

	// --------------------------------------------------------------------------------------------
	
	public void connecDataSet(IntermediateDataSet dataSet) {
		if (dataSet == null) {
			throw new NullPointerException();
		}
		if (this.source != null) {
			throw new IllegalStateException("The edge is already connected.");
		}
		if (!dataSet.getId().equals(sourceId)) {
			throw new IllegalArgumentException("The data set to connect does not match the sourceId.");
		}
		
		this.source = dataSet;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the name of the ship strategy for the represented input, like "forward", "partition hash",
	 * "rebalance", "broadcast", ...
	 *
	 * @return The name of the ship strategy for the represented input, or null, if none was set.
	 */
	public String getShipStrategyName() {
		return shipStrategyName;
	}

	/**
	 * Sets the name of the ship strategy for the represented input.
	 *
	 * @param shipStrategyName The name of the ship strategy. 
	 */
	public void setShipStrategyName(String shipStrategyName) {
		this.shipStrategyName = shipStrategyName;
	}

	/**
	 * Gets the name of the pro-processing operation for this input.
	 *
	 * @return The name of the pro-processing operation, or null, if none was set.
	 */
	public String getPreProcessingOperationName() {
		return preProcessingOperationName;
	}

	/**
	 * Sets the name of the pre-processing operation for this input.
	 *
	 * @param preProcessingOperationName The name of the pre-processing operation.
	 */
	public void setPreProcessingOperationName(String preProcessingOperationName) {
		this.preProcessingOperationName = preProcessingOperationName;
	}

	/**
	 * Gets the operator-level caching description for this input.
	 *
	 * @return The description of operator-level caching, or null, is none was set.
	 */
	public String getOperatorLevelCachingDescription() {
		return operatorLevelCachingDescription;
	}

	/**
	 * Sets the operator-level caching description for this input.
	 *
	 * @param operatorLevelCachingDescription The description of operator-level caching.
	 */
	public void setOperatorLevelCachingDescription(String operatorLevelCachingDescription) {
		this.operatorLevelCachingDescription = operatorLevelCachingDescription;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("%s --> %s [%s]", sourceId, target, distributionPattern.name());
	}
}
