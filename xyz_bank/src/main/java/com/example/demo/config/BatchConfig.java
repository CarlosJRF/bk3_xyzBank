package com.example.demo.config;

import java.time.LocalDate;

import javax.sql.DataSource;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
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
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import com.example.demo.DTOs.AccountDTO;
import com.example.demo.DTOs.TransactionDTO;
import com.example.demo.processor.InterestProcessor;
import com.example.demo.processor.TransactionProcessor;


@Configuration
public class BatchConfig {


    //corte para el commit de registro cada 10
    private final static int CHUNK_SIZE = 10;

    //ubicacion del archivo para transacciones
    @Value("file:/home/carlos/Proyectos/bk_3/xyz_bank/transacciones.csv")
    private Resource transactionCSV;


    /*Cofiguracion de batch encargado de la lectura y escritura de los datos relacionados con el job de transacciones */


    //componentes de lectura para transacciones
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

    //Componente de escritura para transacciones
    @Bean
    public JdbcBatchItemWriter<TransactionDTO> TransactionItemWriter(DataSource dataSource){
    return new JdbcBatchItemWriterBuilder<TransactionDTO>()
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .sql("INSERT INTO transaction_report (transaction_date, amount, operation_type) VALUES (:transactionDate, :amount, :type)")
        .dataSource(dataSource)
        .build();
}
    //Paso para el trabajo de transacciones
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
    //trabajo de registro de transacciones
    @Bean
    public Job transactionJob(JobRepository jobRepository, Step transactionStep){
        return new JobBuilder("transactionJob" ,jobRepository).start(transactionStep).build();
    }

    /*Componentes para la lectura y escritura del trabajo de calculo de interes de las cuentas*/

    
    @Bean
    public JdbcCursorItemReader<AccountDTO> accountItemReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<AccountDTO>()
                .name("accountItemReader")
                .dataSource(dataSource)
                .sql("SELECT id, account_number, account_type, balance, last_interest_date FROM account")
                .rowMapper(new BeanPropertyRowMapper<>(AccountDTO.class))
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<AccountDTO> accountItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<AccountDTO>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("UPDATE account SET balance = :balance, last_interest_date = :lastInterestDate WHERE id = :id")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step interestStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             JdbcCursorItemReader<AccountDTO> accountItemReader,
                             InterestProcessor interestProcessor,
                             JdbcBatchItemWriter<AccountDTO> accountItemWriter) {
        return new StepBuilder("interestStep", jobRepository)
                .<AccountDTO, AccountDTO>chunk(CHUNK_SIZE, transactionManager)
                .reader(accountItemReader)
                .processor(interestProcessor)
                .writer(accountItemWriter)
                .build();
    }

}
