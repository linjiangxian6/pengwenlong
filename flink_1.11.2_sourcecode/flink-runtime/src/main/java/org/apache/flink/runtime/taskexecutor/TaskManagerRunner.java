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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;
import static org.apache.flink.util.Preconditions.checkNotNull;

/*************************************************
 * TODO 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： 启动主类
 *  This class is the executable entry point for the task manager in yarn or standalone mode.
 *  It constructs the related components (network, I/O manager, memory manager,
 *                                        RPC service, HA service) and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	public static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/**
	 * Executor used to run future callbacks.
	 */
	private final ExecutorService executor;

	private final TaskExecutor taskManager;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	public TaskManagerRunner(Configuration configuration, ResourceID resourceId, PluginManager pluginManager) throws Exception {
		this.configuration = checkNotNull(configuration);
		this.resourceId = checkNotNull(resourceId);

		timeout = AkkaUtils.getTimeoutAsTime(configuration);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化进行回调处理的 线程池
		 *  future.xxx(() -> xxxxx(),  exceutor)
		 */
		this.executor = java.util.concurrent.Executors
			.newScheduledThreadPool(Hardware.getNumberCPUCores(), new ExecutorThreadFactory("taskmanager-future"));

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： HA 服务： ZooKeeperHaServices highAvailabilityServices
		 *  提供对高可用性所需的所有服务的访问注册，分布式计数器和领导人选举
		 *  已经解析过了 flink-conf.yaml ,  zookeeper
		 */
		highAvailabilityServices = HighAvailabilityServicesUtils
			//TODO *****
			.createHighAvailabilityServices(configuration, executor, HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化 RpcService
		 *  RpcServer 内部细节和主节点启动的 Rpc 服务的启动方式完全一致
		 *  从结点的RPC服务
		 */
		//TODO ***** createRpcService
		rpcService = createRpcService(configuration, highAvailabilityServices);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化 HeartbeatServices
		 *  1、TaskExecutor 这个组件是一定会启动的
		 *  2、JobMaster JobLeader Flink Job的主控程序，类似于Spark的Driver
		 *  这两个组件，都需要个 ResourceManager 维持心跳
		 *  将来不管有多少个需要发送心跳的组件，都可以通过 heartbeatServices 来创建对应的心跳组件
		 *
		 *  Flink:ResourceManager + TaskExecutor(负责slot的管理和task的执行)
		 *  yarn:ResourceManager + NodeManager
		 */
		//TODO *****
		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： Flink 集群监控
		 */
		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration, pluginManager));
		final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration, rpcService.getAddress());
		metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化 BlobCacheService
		 *  其实内部就是启动两个定时任务，用来定时执行检查，删除过期的 Job 的资源文件。
		 *  通过 引用计数(RefCount) 的方式，来判断是否文件过期（JVM）
		 *  1、主节点其实启动了一个 BlobServer
		 *  2、从节点：BlobCacheService
		 *  其实有两种具体的实现支撑：
		 *  1、永久的
		 *  2、临时的
		 */
		blobCacheService = new BlobCacheService(configuration, highAvailabilityServices.createBlobStore(), null);

		// TODO 注释： 提供外部资源的信息
		final ExternalResourceInfoProvider externalResourceInfoProvider = ExternalResourceUtils
			.createStaticExternalResourceInfoProvider(ExternalResourceUtils.getExternalResourceAmountMap(configuration),
				ExternalResourceUtils.externalResourceDriversFromConfig(configuration, pluginManager));

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 启动 TaskManager
		 *  1、负责创建 TaskExecutor，负责多个任务Task的运行
		 */
		//TODO *****
		taskManager = startTaskManager(this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			externalResourceInfoProvider,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;

		MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() throws Exception {
		taskManager.start();
	}

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释： TaskManager 启动入口
	 */
	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------
	public static void main(String[] args) throws Exception {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if(maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 启动入口
		 *  ResourceID: Flink集群： 主节点  从节点， 每个节点都有一个全局唯一的ID
		 */
		//TODO *****
		runTaskManagerSecurely(args, ResourceID.generate());
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized(lock) {
			if(!shutdown) {
				shutdown = true;

				final CompletableFuture<Void> taskManagerTerminationFuture = taskManager.closeAsync();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils
					.composeAfterwards(taskManagerTerminationFuture, this::shutDownServices);

				serviceTerminationFuture.whenComplete((Void ignored, Throwable throwable) -> {
					if(throwable != null) {
						terminationFuture.completeExceptionally(throwable);
					} else {
						terminationFuture.complete(null);
					}
				});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized(lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				blobCacheService.close();
			} catch(Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch(Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				highAvailabilityServices.close();
			} catch(Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if(exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		Throwable enrichedException = TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", enrichedException);

		// In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is possible,
		// as it does not usually require more class loading to fail again with the Metaspace OutOfMemoryError.
		if(ExceptionUtils.isJvmFatalOrOutOfMemoryError(enrichedException) && !ExceptionUtils.isMetaspaceOutOfMemoryError(enrichedException)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete((Void ignored, Throwable throwable) -> {
				terminateJVM();
			});
		}
	}

	private void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}


	public static Configuration loadConfiguration(String[] args) throws FlinkParseException {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 解析 main方法参数 和 flink-conf.yaml 配置信息
		 */
		return ConfigurationParserUtils.loadCommonConfiguration(args, TaskManagerRunner.class.getSimpleName());
	}

	public static void runTaskManager(Configuration configuration, ResourceID resourceId, PluginManager pluginManager) throws Exception {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 构建 TaskManager 实例
		 *  TaskManagerRunner 是 standalone 模式下 TaskManager 的可执行入口点。
		 *  它构造相关组件(network, I/O manager, memory manager, RPC service, HA service)并启动它们。
		 */
		//TODO ***** TaskManagerRunner
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId, pluginManager);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 发送 START 消息, 确认启动成功
		 */
		taskManagerRunner.start();
	}

	public static void runTaskManagerSecurely(String[] args, ResourceID resourceID) {
		try {

			/*************************************************
			 * TODO 马中华 https://blog.csdn.net/zhongqi2513
			 *  注释： 加载配置参数
			 *  解析 main方法参数 和 flink-conf.yaml 配置信息
			 */
			//TODO *****
			Configuration configuration = loadConfiguration(args);

			/*************************************************
			 * TODO 马中华 https://blog.csdn.net/zhongqi2513
			 *  注释： 启动TaskManager
			 */
			//TODO *****
			runTaskManagerSecurely(configuration, resourceID);

		} catch(Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	public static void runTaskManagerSecurely(Configuration configuration, ResourceID resourceID) throws Exception {
		replaceGracefulExitWithHaltIfConfigured(configuration);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 初始化插件
		 *  主节点启动的时候，也是启动了 PluginManager
		 */
		final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);

		// TODO 注释： 初始化文件系统
		FileSystem.initialize(configuration, pluginManager);

		SecurityUtils.install(new SecurityConfiguration(configuration));

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 包装启动
		 */
		SecurityUtils.getInstalledContext().runSecured(

			// TODO 注释： 通过一个线程来启动 TaskManager
			() -> {
				//TODO *****
				runTaskManager(configuration, resourceID, pluginManager);
				return null;
			});
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	/*************************************************
	 * TODO 马中华 https://blog.csdn.net/zhongqi2513
	 *  注释：
	 *  1、方法名称： startTaskManager
	 *  2、方法返回值： TaskExecutor
	 *  启动 TaskManager 最重要的事情，就是启动 TaskExecutor
	 */
	public static TaskExecutor startTaskManager(Configuration configuration, ResourceID resourceID, RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry,
		BlobCacheService blobCacheService, boolean localCommunicationOnly, ExternalResourceInfoProvider externalResourceInfoProvider,
		FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID);

		String externalAddress = rpcService.getAddress();

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 获取资源定义对象
		 *  一台真实的物理节点，到底有哪些资源（cpucore， memroy, network, ...）
		 *  HardwareDescription 硬件抽象
		 *  1、配置： 重点是来源于配置： taskManager.slots = ?
		 *  2、默认获取
		 *  作用： 将来这个 TaskExecutor 在进行注册的时候，会将当前节点的资源，汇报给 RM
		 */
		final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

		// TODO 注释： TaskManagerServicesConfiguration
		TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration
			.fromConfiguration(configuration, resourceID, externalAddress, localCommunicationOnly,
				taskExecutorResourceSpec
			);

		Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils
			.instantiateTaskManagerMetricGroup(metricRegistry, externalAddress, resourceID,
				taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		// TODO 注释： 初始化 ioExecutor
		final ExecutorService ioExecutor = Executors
			.newFixedThreadPool(taskManagerServicesConfiguration.getNumIoThreads(), new ExecutorThreadFactory("flink-taskexecutor-io"));

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 重点
		 *  taskManagerServices = TaskManagerServices
		 *  里头初始化了很多很多的 TaskManager 在运行过程中需要的服务
		 *  在这儿 TaskManager 启动之前，已经初始化了一些服务组件，基础服务，
		 *  这里面创建的服务，就是 TaskManager 在运行过程中，真正需要的用来对外提供服务的 各种服务组件
		 */
		//TODO ***** fromConfiguration
		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			blobCacheService.getPermanentBlobService(),
			taskManagerMetricGroup.f1,
			ioExecutor,
			fatalErrorHandler);

		// TODO 注释： TaskManagerConfiguration
		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration
			.fromConfiguration(configuration, taskExecutorResourceSpec, externalAddress);

		String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 创建 TaskExecutor 实例
		 *  内部会创建两个重要的心跳管理器：
		 *  1、JobManagerHeartbeatManager
		 *  2、ResourceManagerHeartbeatManager
		 *  -
		 *  这里才是 初始化  TaskManagerRunner 最重要的地方！
		 */
		//TODO *****
		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup.f0,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
			createBackPressureSampleService(configuration, rpcService.getScheduledExecutor())
		);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 记住： 由于当前这个 TaskExecutor 是 RpcEndpoint 的子类，所以在上述构造方法执行完毕的时候，要转到 onStart() 方法执行
		 */
	}

	static BackPressureSampleService createBackPressureSampleService(Configuration configuration, ScheduledExecutor scheduledExecutor) {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 构建一个 背压率 采样服务
		 */
		return new BackPressureSampleService(configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
			Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)), scheduledExecutor);
	}

	/**
	 * Create a RPC service for the task manager.
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices    to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(final Configuration configuration, final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释：
		 */
		//TODO ***** createRemoteRpcService
		return AkkaRpcServiceUtils.createRemoteRpcService(configuration, determineTaskManagerBindAddress(configuration, haServices),
			configuration.getString(TaskManagerOptions.RPC_PORT), configuration.getString(TaskManagerOptions.BIND_HOST),
			configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT));
	}

	private static String determineTaskManagerBindAddress(final Configuration configuration,
		final HighAvailabilityServices haServices) throws Exception {

		final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

		if(configuredTaskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
			return configuredTaskManagerHostname;
		} else {
			return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
		}
	}

	private static String determineTaskManagerBindAddressByConnectingToResourceManager(final Configuration configuration,
		final HighAvailabilityServices haServices) throws LeaderRetrievalException {

		final Duration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

		final InetAddress taskManagerAddress = LeaderRetrievalUtils
			.findConnectingAddress(haServices.getResourceManagerLeaderRetriever(), lookupTimeout);

		LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.", taskManagerAddress.getHostName(),
			taskManagerAddress.getHostAddress());

		HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
		return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
	}
}
