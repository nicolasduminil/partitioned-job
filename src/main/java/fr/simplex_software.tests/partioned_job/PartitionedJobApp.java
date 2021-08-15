package fr.simplex_software.tests.partioned_job;

import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.cloud.task.configuration.*;

@EnableTask
@SpringBootApplication
@EnableBatchProcessing
public class PartitionedJobApp
{
  public static void main(String[] args)
  {
    SpringApplication.run(PartitionedJobApp.class, args);
  }
}
