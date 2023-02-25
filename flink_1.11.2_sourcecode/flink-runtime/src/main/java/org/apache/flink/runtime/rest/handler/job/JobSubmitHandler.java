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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * // TODO 注释： 该处理程序可用于将作业提交到 Flink 群集。
 * This handler can be used to submit jobs to a Flink cluster.
 */
public final class JobSubmitHandler extends AbstractRestHandler<DispatcherGateway, JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

	private static final String FILE_TYPE_JOB_GRAPH = "JobGraph";
	private static final String FILE_TYPE_JAR = "Jar";
	private static final String FILE_TYPE_ARTIFACT = "Artifact";

	private final Executor executor;
	private final Configuration configuration;

	public JobSubmitHandler(GatewayRetriever<? extends DispatcherGateway> leaderRetriever, Time timeout, Map<String, String> headers,
		Executor executor, Configuration configuration) {
		super(leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
		this.executor = executor;
		this.configuration = configuration;
	}

	/*************************************************
	 * TODO 
	 *  注释： 接收到客户端的请求（SubmitJob）
	 */
	@Override
	protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request,
		@Nonnull DispatcherGateway gateway) throws RestHandlerException {

		// TODO 注释： 从请求中获取文件： 包含 JobGraph 序列化文件
		final Collection<File> uploadedFiles = request.getUploadedFiles();
		final Map<String, Path> nameToFile = uploadedFiles.stream().collect(Collectors.toMap(File::getName, Path::fromLocalFile));

		if(uploadedFiles.size() != nameToFile.size()) {
			throw new RestHandlerException(String.format("The number of uploaded files was %s than the expected count. Expected: %s Actual %s",
				uploadedFiles.size() < nameToFile.size() ? "lower" : "higher", nameToFile.size(), uploadedFiles.size()),
				HttpResponseStatus.BAD_REQUEST);
		}

		/*************************************************
		 * TODO 
		 *  注释： 拿到请求体
		 */
		final JobSubmitRequestBody requestBody = request.getRequestBody();

		if(requestBody.jobGraphFileName == null) {
			throw new RestHandlerException(String.format("The %s field must not be omitted or be null.", JobSubmitRequestBody.FIELD_NAME_JOB_GRAPH),
				HttpResponseStatus.BAD_REQUEST);
		}

		/*************************************************
		 * TODO 
		 *  注释： 恢复得到 JobGragh
		 *  由此可见： 服务端接收到客户端提交的，其实就是一个 JobGragh
		 *  到此为止： 我们可以认为：客户端终于把 JobGragh 提交给 JobManager 了。 最终由 JobSubmitHandler 来执行处理
		 */
		CompletableFuture<JobGraph> jobGraphFuture = loadJobGraph(requestBody, nameToFile);

		/*************************************************
		 * TODO 
		 *  注释： 得到 jar
		 */
		Collection<Path> jarFiles = getJarFilesToUpload(requestBody.jarFileNames, nameToFile);

		/*************************************************
		 * TODO 
		 *  注释： 获取 依赖 jar
		 */
		Collection<Tuple2<String, Path>> artifacts = getArtifactFilesToUpload(requestBody.artifactFileNames, nameToFile);

		/*************************************************
		 * TODO 
		 *  注释： 上传： JobGraph + 程序jar + 依赖 jar
		 */
		CompletableFuture<JobGraph> finalizedJobGraphFuture = uploadJobGraphFiles(gateway, jobGraphFuture, jarFiles, artifacts, configuration);

		/*************************************************
		 * TODO 
		 *  注释： 提交任务
		 */
		CompletableFuture<Acknowledge> jobSubmissionFuture = finalizedJobGraphFuture.thenCompose(

			// TODO 注释： 提交任务
			// TODO 注释： gateway = Dispatcher
			// TODO 注释： Flink 集群主节点： JobManager(ResourceManager,  WebMonitorEndpoint,  Dispatcher)
			//ResourceManger 负责申请资源， WebMonitorEndpoint 接收请求处理endpoint请求，Dispatcher 提交job转交给Dispatcher
			//TODO ***** submitJob 实现类Dispatcher
			jobGraph -> gateway.submitJob(jobGraph, timeout)
		);

		return jobSubmissionFuture.thenCombine(jobGraphFuture,

			// TODO 注释： 封装处理响应返回！
			(ack, jobGraph) -> new JobSubmitResponseBody("/jobs/" + jobGraph.getJobID())
		);
	}

	private CompletableFuture<JobGraph> loadJobGraph(JobSubmitRequestBody requestBody, Map<String, Path> nameToFile) throws MissingFileException {

		// TODO 注释： 获取 JobGraph 的序列化文件的存储地
		final Path jobGraphFile = getPathAndAssertUpload(requestBody.jobGraphFileName, FILE_TYPE_JOB_GRAPH, nameToFile);

		return CompletableFuture.supplyAsync(() -> {

			/*************************************************
			 * TODO 
			 *  注释： 从 JobGraghFile 文件中，反序列化回来 JobGragh 对象
			 */
			JobGraph jobGraph;
			try(ObjectInputStream objectIn = new ObjectInputStream(jobGraphFile.getFileSystem().open(jobGraphFile))) {
				jobGraph = (JobGraph) objectIn.readObject();
			} catch(Exception e) {
				throw new CompletionException(new RestHandlerException("Failed to deserialize JobGraph.", HttpResponseStatus.BAD_REQUEST, e));
			}
			return jobGraph;
		}, executor);
	}

	private static Collection<Path> getJarFilesToUpload(Collection<String> jarFileNames,
		Map<String, Path> nameToFileMap) throws MissingFileException {
		Collection<Path> jarFiles = new ArrayList<>(jarFileNames.size());
		for(String jarFileName : jarFileNames) {
			Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFileMap);
			jarFiles.add(new Path(jarFile.toString()));
		}
		return jarFiles;
	}

	private static Collection<Tuple2<String, Path>> getArtifactFilesToUpload(Collection<JobSubmitRequestBody.DistributedCacheFile> artifactEntries,
		Map<String, Path> nameToFileMap) throws MissingFileException {
		Collection<Tuple2<String, Path>> artifacts = new ArrayList<>(artifactEntries.size());
		for(JobSubmitRequestBody.DistributedCacheFile artifactFileName : artifactEntries) {
			Path artifactFile = getPathAndAssertUpload(artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFileMap);
			artifacts.add(Tuple2.of(artifactFileName.entryName, new Path(artifactFile.toString())));
		}
		return artifacts;
	}

	private CompletableFuture<JobGraph> uploadJobGraphFiles(DispatcherGateway gateway, CompletableFuture<JobGraph> jobGraphFuture,
		Collection<Path> jarFiles, Collection<Tuple2<String, Path>> artifacts, Configuration configuration) {
		CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);

		return jobGraphFuture.thenCombine(blobServerPortFuture, (JobGraph jobGraph, Integer blobServerPort) -> {
			final InetSocketAddress address = new InetSocketAddress(gateway.getHostname(), blobServerPort);
			try {
				/*************************************************
				 * TODO 
				 *  注释： 通过 BlobClient 来上传 jar 资源和依赖 jar 和 jobGraph
				 */
				ClientUtils.uploadJobGraphFiles(jobGraph, jarFiles, artifacts, () -> new BlobClient(address, configuration));
			} catch(FlinkException e) {
				throw new CompletionException(new RestHandlerException("Could not upload job files.", HttpResponseStatus.INTERNAL_SERVER_ERROR, e));
			}
			return jobGraph;
		});
	}

	private static Path getPathAndAssertUpload(String fileName, String type, Map<String, Path> uploadedFiles) throws MissingFileException {
		final Path file = uploadedFiles.get(fileName);
		if(file == null) {
			throw new MissingFileException(type, fileName);
		}
		return file;
	}

	private static final class MissingFileException extends RestHandlerException {

		private static final long serialVersionUID = -7954810495610194965L;

		MissingFileException(String type, String fileName) {
			super(type + " file " + fileName + " could not be found on the server.", HttpResponseStatus.BAD_REQUEST);
		}
	}
}
