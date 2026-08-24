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

import com.example.demo.DTOs.AccountDTO;
import com.example.demo.DTOs.StatementDTO;
import com.example.demo.DTOs.TransactionDTO;
import com.example.demo.processor.InterestProcessor;
import com.example.demo.processor.StatementProcessor;
import com.example.demo.processor.TransactionProcessor;


@Configuration
public class BatchConfig {


    //corte para el commit de registro cada 10
    private final static int CHUNK_SIZE = 10;

    //ubicacion del archivo para transacciones
    @Value("file:/home/carlos/Proyectos/bk_3/xyz_bank/xyz_bank/transacciones.csv")
    private Resource transactionCSV;
    //Ubicacion del archivo para lectura y calculo de intereses
    @Value("file:/home/carlos/Proyectos/bk_3/xyz_bank/xyz_bank/intereses.csv")
    private Resource interestCSV;
    //Ubicacion del archivo CSV para estados de cuenta
    @Value("file:/home/carlos/Proyectos/bk_3/xyz_bank/xyz_bank/cuentas_anuales.csv")
    private Resource statementCSV;


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

    /* @Bean
    public Job transactionJob(JobRepository jobRepository, Step transactionStep){
        return new JobBuilder("transactionJob" ,jobRepository).start(transactionStep).build();
    }*/
    

    @Bean
    public Job transactionJob(JobRepository jobRepository, 
                              Step transactionStep, 
                              Step interestStep, 
                              Step statementStep) {
        return new JobBuilder("transactionJob", jobRepository)
                .start(transactionStep) // Fase 1: Carga y validación del CSV
                .next(interestStep)     // Fase 2: Cálculo de intereses mensuales
                .next(statementStep)    // Fase 3: Generación del CSV de auditoría anual
                .build();
    }

    /*Componentes para la lectura y escritura del trabajo de calculo de interes de las cuentas*/

    
    @Bean
    public FlatFileItemReader<AccountDTO> interestItemReader() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "clientName", "balance", "age", "accountType"); 

        BeanWrapperFieldSetMapper<AccountDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(AccountDTO.class);

        DefaultLineMapper<AccountDTO> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return new FlatFileItemReaderBuilder<AccountDTO>()
                .name("interestItemReader")
                .resource(interestCSV)
                .linesToSkip(1) // encabezado
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<AccountDTO> interestItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<AccountDTO>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                // El campo id se omite en el INSERT para que PostgreSQL use el autoincremento (SERIAL)
                .sql("INSERT INTO account (client_name, balance, age, account_type) " +
                     "VALUES (:clientName, :balance, :age, :accountType)")
                .dataSource(dataSource)
                .build();
    }

   @Bean
    public Step interestStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             FlatFileItemReader<AccountDTO> interestItemReader,
                             InterestProcessor interestProcessor,
                             JdbcBatchItemWriter<AccountDTO> interestItemWriter) {
        return new StepBuilder("interestStep", jobRepository)
                .<AccountDTO, AccountDTO>chunk(CHUNK_SIZE, transactionManager)
                .reader(interestItemReader)
                .processor(interestProcessor)
                .writer(interestItemWriter)
                .build();
    }

    /*Componentes para la lectura y escritura del trabajo de registro de estado de las cuentas*/

    @Bean
    public FlatFileItemReader<StatementDTO> statementItemReader() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("accountId", "statementDate", "transaction", "amount", "description");

        BeanWrapperFieldSetMapper<StatementDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(StatementDTO.class);

        // Conversor de Fechas de String (CSV) a LocalDate (DTO)
        DefaultConversionService conversionService = new DefaultConversionService();
        conversionService.addConverter(String.class, LocalDate.class, LocalDate::parse);
        fieldSetMapper.setConversionService(conversionService);

        DefaultLineMapper<StatementDTO> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return new FlatFileItemReaderBuilder<StatementDTO>()
                .name("statementItemReader")
                .resource(statementCSV)
                .linesToSkip(1) // Salta el encabezado
                .lineMapper(lineMapper)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<StatementDTO> statementItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<StatementDTO>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO annual_statement (account_id, statement_date, transaction, amount, description) " +
                     "VALUES (:accountId, :statementDate, :transaction, :amount, :description)")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Step statementStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                              FlatFileItemReader<StatementDTO> statementItemReader,
                              StatementProcessor statementProcessor,
                              JdbcBatchItemWriter<StatementDTO> statementItemWriter) {
        return new StepBuilder("statementStep", jobRepository)
                .<StatementDTO, StatementDTO>chunk(CHUNK_SIZE, transactionManager)
                .reader(statementItemReader)
                .processor(statementProcessor)
                .writer(statementItemWriter)
                .build();
    }

}
