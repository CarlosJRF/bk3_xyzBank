package com.example.demo.config;

import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.DTOs.TransactionDTO;
import com.example.demo.processor.TransactionProcessor;


@Configuration
public class BatchConfig {

    private final static int CHUNK_SIZE = 10;

    @Value("file:/home/carlos/Proyectos/bk_3/xyz_bank/transacciones.csv")
    private Resource transactionCSV;

    @Bean
    public FlatFileItemReader<TransactionDTO> sendTransactionItemReader(){
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "transactionDate", "amount", "type");

        BeanWrapperFieldSetMapper<TransactionDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(TransactionDTO.class);

        DefaultConversionService conversionService = new DefaultConversionService();
        conversionService.addConverter(String.class, LocalDate.class, LocalDate::parse);
        fieldSetMapper.setConversionService(conversionService);

        DefaultLineMapper<TransactionDTO> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return new FlatFileItemReaderBuilder<TransactionDTO>()
                .name("sendTransactionItemReader")
                .resource(transactionCSV)
                .linesToSkip(1) // encabezado del CSV
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<TransactionDTO> TransactionItemWriter(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<TransactionDTO>()
            .dataSource(dataSource)
            .sql("INSER INTO transaction (id, transaction_date, amount, operation_type) VALUES (:id, transactionDate, amount, type)").dataSource(dataSource).build();
    }
    
    @Bean
    public Step transactionStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
            FlatFileItemReader<TransactionDTO> sendTransactionItemReader, TransactionProcessor itemOut,
            JdbcBatchItemWriter<TransactionDTO> TransactionItemWriter) {
        return new StepBuilder("transactionStep", jobRepository)
                .<TransactionDTO, TransactionDTO>chunk(CHUNK_SIZE, transactionManager)
                .reader(sendTransactionItemReader)
                .processor(itemOut)
                .writer(TransactionItemWriter)
                .build();
    }

    @Bean
    public Job transactionJob(JobRepository jobRepository, Step transactionStep){
        return new JobBuilder("transactionJob" ,jobRepository).start(transactionStep).build();
    }

}
