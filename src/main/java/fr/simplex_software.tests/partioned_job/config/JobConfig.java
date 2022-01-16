package fr.simplex_software.tests.partioned_job.config;

import lombok.extern.slf4j.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.explore.*;
import org.springframework.batch.core.launch.support.*;
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
  public JobExplorer jobExplorer;
  @Autowired
  private ConfigurableApplicationContext context;
  @Autowired
  private DelegatingResourceLoader resourceLoader;
  @Autowired
  private Environment environment;
  @Autowired
  public TaskExplorer taskExplorer;
  @Autowired
  public TaskLauncher taskLauncher;
  @Autowired
  public TaskRepository taskRepository;

  @Bean
  @StepScope
  public PartitionHandler partitionHandler(@Value("#{stepExecution}") StepExecution stepExecution)  {
    // This below is to demo how to get the current step (in job controller that trigger this partitionerhandler)
    // Note, in this code case, it is called Step1 as that is the step name defined trigger
    String currentStepName = stepExecution.getStepName();

    // Utilize the job execution context
    // Can also use step1_corresponding_worker_job, but for sake of demo how to show step -> worker step1 relation
    // use following
    String workerStepName = stepExecution.getJobExecution().getExecutionContext().getString(currentStepName + "_corresponding_worker_job");

    Resource resource = this.resourceLoader
      .getResource("maven://fr.simplex_software.tests:partitioned-job:1.0-SNAPSHOT");


    DeployerPartitionHandler partitionHandler =
      new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, workerStepName, taskRepository);
    TaskExecution taskExecution = taskExplorer.getTaskExecution(taskExplorer.getTaskExecutionIdByJobExecutionId(stepExecution.getJobExecutionId()));
    partitionHandler.beforeTask(taskExecution);
    List<String> commandLineArgs = new ArrayList<>(3);
    commandLineArgs.add("--spring.profiles.active=worker");
    commandLineArgs.add("--spring.cloud.task.initialize-enabled=false");
    commandLineArgs.add("--spring.batch.initializer.enabled=false");
    partitionHandler
      .setCommandLineArgsProvider(new PassThroughCommandLineArgsProvider(commandLineArgs));
    partitionHandler
      .setEnvironmentVariablesProvider(new SimpleEnvironmentVariablesProvider(this.environment));
    partitionHandler.setMaxWorkers(GRID_SIZE); // Understanding is workers should be equal/larger than number of grid
    partitionHandler.setApplicationName("PartitionedBatchJobTask");
    return partitionHandler;
  }

  @Bean
  public Partitioner partitioner()
  {
    return gridSize ->
    {
      Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);
      for (int i = 0; i < GRID_SIZE; i++)
      {
        ExecutionContext context1 = new ExecutionContext();
        context1.put("partitionNumber", i);
        partitions.put("partition" + i, context1);
      }
      return partitions;
    };
  }

  @Bean
  @Profile("worker")
  public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer)
  {
     return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
  }

  @Bean
  @StepScope
  public Tasklet workerTasklet(final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber)
  {
    return (contribution, chunkContext) ->
    {
       return RepeatStatus.FINISHED;
    };
  }

  @Bean
  public Step step1()
  {
    return this.stepBuilderFactory.get("step1")
      .partitioner(workerStep().getName(), partitioner())
      .step(workerStep())
      .partitionHandler(partitionHandler( null))
      .build();
  }

  @Bean
  public Step workerStep()
  {
     return this.stepBuilderFactory.get("workerStep")
      .tasklet(workerTasklet(null))
      .build();
  }

  @Bean
  @Profile("!worker")
  public Job partitionedJob()
  {
    return this.jobBuilderFactory.get("partitionedJob")
      .incrementer(new RunIdIncrementer())
      .listener(jobExecutionListener()) // Add job execution listener to populate some setting, but can also use job parameters
      .start(step1())
      .build();
  }

  @Bean
  public JobExecutionListener jobExecutionListener() {
    JobExecutionListener listener = new JobExecutionListener() {


      // Define some context to be later used in job
      @Override
      public void beforeJob(JobExecution jobExecution) {
        // Set up value to map step1 to corresponding worker step
        // Can use other way like job parameters too
        jobExecution.getExecutionContext().putString("step1_corresponding_worker_job", "workerStep");

      }

      @Override
      public void afterJob(JobExecution jobExecution) {
      }
    };

    return listener;
  }
}
