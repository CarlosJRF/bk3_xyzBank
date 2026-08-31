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
import java.lang.Exception;
import java.math.BigDecimal;

import com.example.demo.DTOs.AccountDTO;
import com.example.demo.DTOs.StatementDTO;
import com.example.demo.DTOs.TransactionDTO;
import com.example.demo.listener.CustomSkipListener;
import com.example.demo.policies.CustomSkipPolicies;
import com.example.demo.processor.InterestProcessor;
import com.example.demo.processor.StatementProcessor;
import com.example.demo.processor.TransactionProcessor;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.springframework.core.convert.converter.Converter;

import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.job.DefaultJobParametersExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.batch.core.launch.JobLauncher;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DeadlockLoserDataAccessException;

import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.infrastructure.item.support.builder.SynchronizedItemStreamReaderBuilder;


@Configuration
public class BatchConfig {


    //corte para el commit de registro cada 10
    private final static int CHUNK_SIZE = 5;

    //ubicacion del archivo para transacciones
    @Value("classpath:transacciones.csv")
    private Resource transactionCSV;

    @Value("classpath:intereses.csv")
    private Resource interestCSV;

    @Value("classpath:cuentas_anuales.csv")
    private Resource statementCSV;


    /*Cofiguracion de batch encargado de la lectura y escritura de los datos relacionados con el job de transacciones */


    //componentes de lectura para transacciones
    @Bean
    public FlatFileItemReader<TransactionDTO> sendTransactionItemReader(){
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "transactionDate", "amount", "type");

        BeanWrapperFieldSetMapper<TransactionDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(TransactionDTO.class);

        fieldSetMapper.setConversionService(createConversionService());

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
                            JdbcBatchItemWriter<TransactionDTO> transactionItemWriter, CustomSkipListener skipListener, CustomSkipPolicies skipPolicies) {
    return new StepBuilder("transactionStep", jobRepository)
            .<TransactionDTO, TransactionDTO>chunk(CHUNK_SIZE, transactionManager)
            
            // 1. Sincronización del Lector (Evita colisiones de memoria)
            .reader(new SynchronizedItemStreamReaderBuilder<TransactionDTO>()
                    .delegate(sendTransactionItemReader)
                    .build())
            .processor(itemOut)
            .writer(transactionItemWriter)
            
            // 2. Orquestación Multi-hilo
            .taskExecutor(taskExecutor())
            
            // 3. Resiliencia y Saltos
            .faultTolerant()
            .skipPolicy(skipPolicies)
            .listener(skipListener)
            
            // 4. Reintentos ante bloqueos concurrentes en PostgreSQL
            .retryLimit(3)
            .retry(CannotAcquireLockException.class)
            .retry(DeadlockLoserDataAccessException.class)
            .build();
}
    //trabajo de multiples steps: Step 1 -> Step 2 -> Step 3

    // Job 1: Reporte de Transacciones
    @Bean
    public Job transactionJob(JobRepository jobRepository, Step transactionStep) {
        return new JobBuilder("transactionJob", jobRepository)
                .start(transactionStep) 
                .build();
    }

    // Job 2: Cálculo de Intereses Mensuales
    @Bean
    public Job interestJob(JobRepository jobRepository, Step interestStep) {
        return new JobBuilder("interestJob", jobRepository)
                .start(interestStep)
                .build();
    }

    // Job 3: Generación de Estados de Cuenta Anuales
    @Bean
    public Job statementJob(JobRepository jobRepository, Step statementStep) {
        return new JobBuilder("statementJob", jobRepository)
                .start(statementStep)
                .build();
    }


    /*Componentes para la lectura y escritura del trabajo de calculo de interes de las cuentas*/

    
    @Bean
    public FlatFileItemReader<AccountDTO> interestItemReader() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "clientName", "balance", "age", "accountType"); 

        BeanWrapperFieldSetMapper<AccountDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(AccountDTO.class);
        fieldSetMapper.setConversionService(createConversionService());


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
                         JdbcBatchItemWriter<AccountDTO> interestItemWriter, CustomSkipListener skipListener, CustomSkipPolicies skipPolicies) {
    return new StepBuilder("interestStep", jobRepository)
            .<AccountDTO, AccountDTO>chunk(CHUNK_SIZE, transactionManager)
            
            // 1. Sincronización del Lector
            .reader(new SynchronizedItemStreamReaderBuilder<AccountDTO>()
                    .delegate(interestItemReader)
                    .build())
            .processor(interestProcessor)
            .writer(interestItemWriter)
            
            // 2. Orquestación Multi-hilo
            .taskExecutor(taskExecutor())
            
            // 3. Resiliencia y Saltos
            .faultTolerant()
            .skipPolicy(skipPolicies)
            .listener(skipListener)
            
            // 4. Reintentos ante bloqueos concurrentes en PostgreSQL
            .retryLimit(3)
            .retry(CannotAcquireLockException.class)
            .retry(DeadlockLoserDataAccessException.class)
            .build();
}

    /*Componentes para la lectura y escritura del trabajo de registro de estado de las cuentas*/

    @Bean
    public FlatFileItemReader<StatementDTO> statementItemReader() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("accountId", "statementDate", "transaction", "amount", "description");

        BeanWrapperFieldSetMapper<StatementDTO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(StatementDTO.class);
        fieldSetMapper.setConversionService(createConversionService());

        DefaultConversionService conversionService = new DefaultConversionService();
        conversionService.addConverter(String.class, LocalDate.class, new Converter<String, LocalDate>() {
        @Override
        public LocalDate convert(String source) {
                // Lista de formatos identificados en el log de errores
                String[] formatos = {"yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd"};
                
                for (String formato : formatos) {
                try {
                        return LocalDate.parse(source, DateTimeFormatter.ofPattern(formato));
                } catch (DateTimeParseException e) {
                        // Continúa con el siguiente formato de la lista si este falla
                }
                }
                // Si ninguno coincide, lanza excepción para que Spring Batch lo cuente como un Skip válido
                throw new IllegalArgumentException("Formato de fecha no soportado: " + source);
        }
        });
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
                          JdbcBatchItemWriter<StatementDTO> statementItemWriter, CustomSkipListener skipListener, CustomSkipPolicies skipPolicies) {
    return new StepBuilder("statementStep", jobRepository)
            .<StatementDTO, StatementDTO>chunk(CHUNK_SIZE, transactionManager)
            
            // 1. Sincronización del Lector
            .reader(new SynchronizedItemStreamReaderBuilder<StatementDTO>()
                    .delegate(statementItemReader)
                    .build())
            .processor(statementProcessor)
            .writer(statementItemWriter)
            
            // 2. Orquestación Multi-hilo
            .taskExecutor(taskExecutor())
            
            // 3. Resiliencia y Saltos
            .faultTolerant()
            .skipPolicy(skipPolicies)
            .listener(skipListener)
            
            // 4. Reintentos ante bloqueos concurrentes en PostgreSQL
            .retryLimit(3)
            .retry(CannotAcquireLockException.class)
            .retry(DeadlockLoserDataAccessException.class)
            .build();
}

    // Método centralizado para manejar datos sucios en todos los CSV
    private DefaultConversionService createConversionService() {
        DefaultConversionService conversionService = new DefaultConversionService();
        
        // 1. Reparador de Fechas
        conversionService.addConverter(String.class, LocalDate.class, new Converter<String, LocalDate>() {
            @Override
            public LocalDate convert(String source) {
                if (source == null || source.trim().isEmpty()) return null;
                String[] formatos = {"yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd"};
                for (String formato : formatos) {
                    try {
                        return LocalDate.parse(source.trim(), DateTimeFormatter.ofPattern(formato));
                    } catch (Exception e) {
                        // Ignora el error y prueba el siguiente formato
                    }
                }
                throw new IllegalArgumentException("Fecha no procesable: " + source);
            }
        });

        // 2. Reparador de Montos Enteros (transacciones vacías)
        conversionService.addConverter(String.class, Integer.class, new Converter<String, Integer>() {
            @Override
            public Integer convert(String source) {
                if (source == null || source.trim().isEmpty()) return 0;
                return Integer.parseInt(source.trim());
            }
        });

        // 3. Reparador de Saldos Decimales (cuentas vacías)
        conversionService.addConverter(String.class, BigDecimal.class, new Converter<String, BigDecimal>() {
            @Override
            public BigDecimal convert(String source) {
                if (source == null || source.trim().isEmpty()) return BigDecimal.ZERO;
                return new BigDecimal(source.trim());
            }
        });

        return conversionService;
    }

    /*Integrando TaskExecutor para aplicar metodo de procesamiento multi hilos */

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10); // Hilos base activos
        executor.setMaxPoolSize(20);  // Límite máximo de hilos paralelos
        executor.setQueueCapacity(50); // Capacidad de espera
        executor.setThreadNamePrefix("BatchThread-");
        executor.setAllowCoreThreadTimeOut(true);
        executor.initialize();
        return executor;
    }


    /* ----------------------------------------------------------------------
       SECCIÓN: ORQUESTACIÓN NATIVA CON JOB MAESTRO Y FLUJOS PARALELOS
       ---------------------------------------------------------------------- */

    // 1. Convertimos tus 3 Jobs independientes en "JobSteps"
    @Bean
    public Step transactionJobStep(JobRepository jobRepository, JobOperator jobOperator, @Qualifier("transactionJob") Job transactionJob) {
        return new StepBuilder("transactionJobStep", jobRepository)
                .job(transactionJob)
                .operator(jobOperator)
                .build();
    }

    @Bean
    public Step interestJobStep(JobRepository jobRepository, JobOperator jobOperator, @Qualifier("interestJob") Job interestJob) {
        return new StepBuilder("interestJobStep", jobRepository)
                .job(interestJob)
                .operator(jobOperator)
                .build();
    }

    @Bean
    public Step statementJobStep(JobRepository jobRepository, JobOperator jobOperator, @Qualifier("statementJob") Job statementJob) {
        return new StepBuilder("statementJobStep", jobRepository)
                .job(statementJob)
                .operator(jobOperator)
                .build();
    }

    // 2. Envolvemos cada JobStep en un Flujo (Flow)
    @Bean
    public Flow transactionFlow(Step transactionJobStep) {
        return new FlowBuilder<Flow>("transactionFlow").start(transactionJobStep).build();
    }

    @Bean
    public Flow interestFlow(Step interestJobStep) {
        return new FlowBuilder<Flow>("interestFlow").start(interestJobStep).build();
    }

    @Bean
    public Flow statementFlow(Step statementJobStep) {
        return new FlowBuilder<Flow>("statementFlow").start(statementJobStep).build();
    }

    // 3. El Job Maestro que divide la ejecución usando tus 3 hilos paralelos
    @Bean
    public Job masterJob(JobRepository jobRepository, TaskExecutor taskExecutor, 
                         Flow transactionFlow, Flow interestFlow, Flow statementFlow) {
        return new JobBuilder("masterJob", jobRepository)
                .start(transactionFlow)
                .split(taskExecutor) // Aquí inyectamos el multi-threading a nivel de Jobs
                .add(interestFlow, statementFlow) // Agregamos los otros dos flujos para que arranquen al mismo tiempo
                .end()
                .build();
    }

}
