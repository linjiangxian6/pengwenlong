/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The shuffle mode defines the data exchange mode between operators.
 */
@PublicEvolving
public enum ShuffleMode {
	/**
	 * Producer and consumer are online at the same time.
	 * Produced data is received by consumer immediately.
	 * // TODO 注释： 生产者和消费者同时在线。 产生的数据将立即被消费者接收。
	 * // TODO 注释： 流式处理
	 */
	PIPELINED,

	/**
	 * The producer first produces its entire result and finishes.
	 * After that, the consumer is started and may consume the data.
	 * // TODO 注释： 生产者首先产生其全部结果并完成。 之后，使用者将启动并可以使用数据。
	 * // TODO 注释： 批处理
	 */
	BATCH,

	/**
	 * The shuffle mode is undefined. It leaves it up to the framework to decide the shuffle mode.
	 * The framework will pick one of {@link ShuffleMode#BATCH} or {@link ShuffleMode#PIPELINED} in the end.
	 * // TODO 注释： shuffle mode 未定义。它留给框架来决定随机播放模式。
	 * // TODO 注释： 框架最后将选择{@link ShuffleMode＃BATCH}或{@link ShuffleMode＃PIPELINED}中的一个。
	 * // TODO 注释： 如果没决定，则由Flink框架自行决定使用 Batch 或者 Pipline
	 */
	UNDEFINED
}
