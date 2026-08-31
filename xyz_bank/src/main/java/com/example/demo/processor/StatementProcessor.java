package com.example.demo.processor;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;
import com.example.demo.DTOs.StatementDTO;

@Component
public class StatementProcessor implements ItemProcessor<StatementDTO, StatementDTO> {
    @Override
    public StatementDTO process(StatementDTO statement) throws Exception {
        // Garantizar la consistencia: descartar filas sin monto
        if (statement.getAmount() == null) {
            return null; // Spring Batch ignora el registro automáticamente
        }
        
        if (statement.getDescription() != null) {
            statement.setDescription(statement.getDescription().toUpperCase());
        } else {
            statement.setDescription("SIN DESCRIPCIÓN");
        }
        return statement;
    }
}