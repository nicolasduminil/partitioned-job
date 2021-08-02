package fr.simplex_software.tests.partioned_job.config;

import lombok.extern.slf4j.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.explore.*;
import org.springframework.batch.core.partition.*;
import org.springframework.batch.core.partition.support.*;
import org.springframework.batch.core.repository.*;
import org.springframework.batch.core.step.tasklet.*;
import org.springframework.batch.item.*;
import org.springframework.batch.repeat.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.cloud.deployer.resource.support.*;
import org.springframework.cloud.deployer.spi.task.*;
import org.springframework.cloud.task.batch.partition.*;
import org.springframework.cloud.task.repository.*;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.core.env.*;
import org.springframework.core.io.*;

import javax.sql.*;
import java.util.*;

@Configuration
@Slf4j
public class JobConfig
{
  private static final int GRID_SIZE = 4;
  @Autowired
  public JobBuilderFactory jobBuilderFactory;
  @Autowired
  public StepBuilderFactory stepBuilderFactory;
  @Autowired
  public DataSource dataSource;
  @Autowired
  public JobRepository jobRepository;
  @Autowired
  private ConfigurableApplicationContext context;
  @Autowired
  private DelegatingResourceLoader resourceLoader;
  @Autowired
  private Environment environment;

  @Bean
  public PartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer, TaskRepository taskRepository)
  {
    log.info ("### PartitionedJobApp.partitionHandler(): Entry tasklauncher {}, jobExplorer {}, taskRepository {}", taskLauncher, jobExplorer, taskRepository);
    Resource resource = this.resourceLoader
      .getResource("maven://fr.simplex_software.tests:partitioned-job:1.0-SNAPSHOT");
    log.info ("### PartitionedJobApp.partitionHandler(): Got resource {}", resource);
    DeployerPartitionHandler partitionHandler =
      new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "workerStep");
    List<String> commandLineArgs = new ArrayList<>(3);
    commandLineArgs.add("--spring.profiles.active=worker");
    commandLineArgs.add("--spring.cloud.task.initialize-enabled=false");
    commandLineArgs.add("--spring.batch.initializer.enabled=false");
    partitionHandler
      .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
    partitionHandler
      .setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(this.environment));
    partitionHandler.setMaxWorkers(2);
    partitionHandler.setApplicationName("PartitionedBatchJobTask");
    log.info ("### PartitionedJobApp.partitionHandler(): Exit partitionHandler {}", partitionHandler);
    return partitionHandler;
  }

  @Bean
  public Partitioner partitioner()
  {
    log.info ("### PartitionedJobApp.partitioner(): Entry");
    return gridSize ->
    {
      Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);
      for (int i = 0; i < GRID_SIZE; i++)
      {
        ExecutionContext context1 = new ExecutionContext();
        context1.put("partitionNumber", i);
        partitions.put("partition" + i, context1);
      }
      log.info ("### PartitionedJobApp.partitioner(): Exit partitions {}", partitions);
      return partitions;
    };
  }

  @Bean
  @Profile("worker")
  public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer)
  {
    log.info ("### PartitionedJobApp.stepExecutionHandler(): Entry jobExplorer {}", jobExplorer);
    return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
  }

  @Bean
  @StepScope
  public Tasklet workerTasklet(final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber)
  {
    return (contribution, chunkContext) ->
    {
      log.info("### PartitionedJobApp.workerTasklet(): This tasklet ran partition: {}", partitionNumber);
      return RepeatStatus.FINISHED;
    };
  }

  @Bean
  public Step step1(PartitionHandler partitionHandler)
  {
    log.info("### PartitionedJobApp.step1(): Entry");
    return this.stepBuilderFactory.get("step1")
      .partitioner(workerStep().getName(), partitioner())
      .step(workerStep())
      .partitionHandler(partitionHandler)
      .build();
  }

  @Bean
  public Step workerStep()
  {
    log.info("### PartitionedJobApp.workerStep(): Entry");
    return this.stepBuilderFactory.get("workerStep")
      .tasklet(workerTasklet(null))
      .build();
  }

  @Bean
  @Profile("!worker")
  public Job partitionedJob(PartitionHandler partitionHandler)
  {
    log.info("### PartitionedJobApp.partitionedJob(): Entry partitionHandler {}", partitionHandler);
    Random random = new Random();
    return this.jobBuilderFactory.get("partitionedJob" + random.nextInt())
      .start(step1(partitionHandler))
      .build();
  }
}
