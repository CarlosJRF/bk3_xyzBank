package com.example.demo.config;

import java.time.LocalDate;

import javax.sql.DataSource;

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

import com.example.demo.DTOs.TransactionDTO;


@Configuration
public class BatchConfig {

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
}
