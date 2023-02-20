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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： Netty 服务端
	 */
	private final NettyServer server;

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： Netty 客户端
	 */
	private final NettyClient client;

	private final NettyBufferPool bufferPool;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	private final NettyProtocol nettyProtocol;

	public NettyConnectionManager(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher, NettyConfig nettyConfig) {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化一个 NettyServer
		 */
		this.server = new NettyServer(nettyConfig);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化一个 NettyClient
		 */
		this.client = new NettyClient(nettyConfig);

		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());
		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
		this.nettyProtocol = new NettyProtocol(checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
	}

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： 做了两件事
	 *  1、启动了一个 Netty 客户端
	 *  2、启动了一个 Netty 服务端
	 *
	 *  StreamTask（20） ---> StreamTask（10） ---> StreamTask（12）
	 */
	@Override
	public int start() throws IOException {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释：
		 */
		//TODO *****
		client.init(nettyProtocol, bufferPool);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释：
		 */
		int result = server.init(nettyProtocol, bufferPool);

		return result;
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
