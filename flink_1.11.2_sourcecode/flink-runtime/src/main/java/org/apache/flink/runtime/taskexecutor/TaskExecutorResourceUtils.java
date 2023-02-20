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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Utility class for {@link TaskExecutorResourceSpec} of running {@link TaskExecutor}.
 */
public class TaskExecutorResourceUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorResourceUtils.class);

	static final List<ConfigOption<?>> CONFIG_OPTIONS = Arrays.asList(
		TaskManagerOptions.CPU_CORES,
		TaskManagerOptions.TASK_HEAP_MEMORY,
		TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
		TaskManagerOptions.NETWORK_MEMORY_MIN,
		TaskManagerOptions.NETWORK_MEMORY_MAX,
		TaskManagerOptions.MANAGED_MEMORY_SIZE
	);

	private static final List<ConfigOption<?>> UNUSED_CONFIG_OPTIONS = Arrays.asList(
		TaskManagerOptions.TOTAL_PROCESS_MEMORY,
		TaskManagerOptions.TOTAL_FLINK_MEMORY,
		TaskManagerOptions.FRAMEWORK_HEAP_MEMORY,
		TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY,
		TaskManagerOptions.JVM_METASPACE,
		TaskManagerOptions.JVM_OVERHEAD_MIN,
		TaskManagerOptions.JVM_OVERHEAD_MAX,
		TaskManagerOptions.JVM_OVERHEAD_FRACTION
	);

	static final MemorySize DEFAULT_SHUFFLE_MEMORY_SIZE = MemorySize.parse("64m");
	static final MemorySize DEFAULT_MANAGED_MEMORY_SIZE = MemorySize.parse("128m");

	private TaskExecutorResourceUtils() {}

	static TaskExecutorResourceSpec resourceSpecFromConfig(Configuration config) {
		try {

			/*************************************************
			 * TODO 马中华 https://blog.csdn.net/zhongqi2513
			 *  注释： 检查资源是否配置，如果没有默认值，则必须配置
			 */
			checkTaskExecutorResourceConfigSet(config);
		} catch (IllegalConfigurationException e) {
			throw new IllegalConfigurationException("Failed to create TaskExecutorResourceSpec", e);
		}

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 封装得到该 TaskManager 的资源信息： TaskExecutorResourceSpec
		 */
		return new TaskExecutorResourceSpec(
			new CPUResource(config.getDouble(TaskManagerOptions.CPU_CORES)),
			config.get(TaskManagerOptions.TASK_HEAP_MEMORY),
			config.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY),
			config.get(TaskManagerOptions.NETWORK_MEMORY_MIN),
			config.get(TaskManagerOptions.MANAGED_MEMORY_SIZE)
		);
	}

	private static void checkTaskExecutorResourceConfigSet(Configuration config) {

		// TODO 注释： 检查，一些必要的配置，是否有配置
		// TODO 注释： 如果没有配置，必须设置的有默认值
		CONFIG_OPTIONS.forEach(option -> checkConfigOptionIsSet(config, option));

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 解析配置文件，获取到资源配置
		 */
		checkTaskExecutorNetworkConfigSet(config);
	}

	private static void checkConfigOptionIsSet(Configuration config, ConfigOption<?> option) {
		if (!config.contains(option) && !option.hasDefaultValue()) {
			throw new IllegalConfigurationException("The required configuration option %s is not set", option);
		}
	}

	private static void checkTaskExecutorNetworkConfigSet(ReadableConfig config) {
		if (!config.get(TaskManagerOptions.NETWORK_MEMORY_MIN).equals(config.get(TaskManagerOptions.NETWORK_MEMORY_MAX))) {
			throw new IllegalConfigurationException(
				"The network memory min (%s) and max (%s) mismatch, " +
					"the network memory has to be resolved and set to a fixed value before task executor starts",
				config.get(TaskManagerOptions.NETWORK_MEMORY_MIN),
				config.get(TaskManagerOptions.NETWORK_MEMORY_MAX));
		}
	}

	static ResourceProfile generateDefaultSlotResourceProfile(TaskExecutorResourceSpec taskExecutorResourceSpec, int numberOfSlots) {

		/*************************************************
		 * TODO 马中华 https://blog.csdn.net/zhongqi2513
		 *  注释： 根据 TaskExecutorResourceSpec 生成 Slot Resource Profile
		 */
		return ResourceProfile.newBuilder()
			.setCpuCores(taskExecutorResourceSpec.getCpuCores().divide(numberOfSlots))
			.setTaskHeapMemory(taskExecutorResourceSpec.getTaskHeapSize().divide(numberOfSlots))
			.setTaskOffHeapMemory(taskExecutorResourceSpec.getTaskOffHeapSize().divide(numberOfSlots))
			.setManagedMemory(taskExecutorResourceSpec.getManagedMemorySize().divide(numberOfSlots))
			.setNetworkMemory(taskExecutorResourceSpec.getNetworkMemSize().divide(numberOfSlots))
			.build();
	}

	static ResourceProfile generateTotalAvailableResourceProfile(TaskExecutorResourceSpec taskExecutorResourceSpec) {

		// TODO 注释： 根据 TaskExecutorResourceSpec 对象生成 ResourceProfile
		return ResourceProfile.newBuilder()
			.setCpuCores(taskExecutorResourceSpec.getCpuCores())
			.setTaskHeapMemory(taskExecutorResourceSpec.getTaskHeapSize())
			.setTaskOffHeapMemory(taskExecutorResourceSpec.getTaskOffHeapSize())
			.setManagedMemory(taskExecutorResourceSpec.getManagedMemorySize())
			.setNetworkMemory(taskExecutorResourceSpec.getNetworkMemSize())
			.build();
	}

	@VisibleForTesting
	public static TaskExecutorResourceSpec resourceSpecFromConfigForLocalExecution(Configuration config) {
		return resourceSpecFromConfig(adjustForLocalExecution(config));
	}

	public static Configuration adjustForLocalExecution(Configuration config) {
		UNUSED_CONFIG_OPTIONS.forEach(option -> warnOptionHasNoEffectIfSet(config, option));

		setConfigOptionToPassedMaxIfNotSet(config, TaskManagerOptions.CPU_CORES, Double.MAX_VALUE);
		setConfigOptionToPassedMaxIfNotSet(config, TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.MAX_VALUE);
		setConfigOptionToPassedMaxIfNotSet(config, TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.MAX_VALUE);

		adjustNetworkMemoryForLocalExecution(config);
		setConfigOptionToDefaultIfNotSet(config, TaskManagerOptions.MANAGED_MEMORY_SIZE, DEFAULT_MANAGED_MEMORY_SIZE);

		return config;
	}

	private static void adjustNetworkMemoryForLocalExecution(Configuration config) {
		if (!config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN) &&
			config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)) {
			config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, config.get(TaskManagerOptions.NETWORK_MEMORY_MAX));
		}
		if (!config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX) &&
			config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN)) {
			config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, config.get(TaskManagerOptions.NETWORK_MEMORY_MIN));
		}
		setConfigOptionToDefaultIfNotSet(config, TaskManagerOptions.NETWORK_MEMORY_MIN, DEFAULT_SHUFFLE_MEMORY_SIZE);
		setConfigOptionToDefaultIfNotSet(config, TaskManagerOptions.NETWORK_MEMORY_MAX, DEFAULT_SHUFFLE_MEMORY_SIZE);
	}

	private static void warnOptionHasNoEffectIfSet(Configuration config, ConfigOption<?> option) {
		if (config.contains(option)) {
			LOG.warn(
				"The resource configuration option {} is set but it will have no effect for local execution, " +
					"only the following options matter for the resource configuration: {}",
				option,
				UNUSED_CONFIG_OPTIONS);
		}
	}

	private static <T> void setConfigOptionToDefaultIfNotSet(
			Configuration config,
			ConfigOption<T> option,
			T defaultValue) {
		setConfigOptionToDefaultIfNotSet(config, option, defaultValue, "its default value " + defaultValue);
	}

	private static <T> void setConfigOptionToPassedMaxIfNotSet(
			Configuration config,
			ConfigOption<T> option,
			T maxValue) {
		setConfigOptionToDefaultIfNotSet(config, option, maxValue, "the maximal possible value");
	}

	private static <T> void setConfigOptionToDefaultIfNotSet(
			Configuration config,
			ConfigOption<T> option,
			T defaultValue,
			String defaultValueLogExt) {
		if (!config.contains(option)) {
			LOG.info(
				"The configuration option {} required for local execution is not set, setting it to {}.",
				option.key(),
				defaultValueLogExt);
			config.set(option, defaultValue);
		}
	}
}
