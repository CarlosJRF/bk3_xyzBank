package com.example.demo.processor;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;
import com.example.demo.DTOs.StatementDTO;

@Component
public class StatementProcessor implements ItemProcessor<StatementDTO, StatementDTO> {

    @Override
    public StatementDTO process(StatementDTO statement) throws Exception {
        // Estandarizamos la descripción para facilitar búsquedas en auditoría
        if (statement.getDescription() != null) {
            statement.setDescription(statement.getDescription().toUpperCase());
        }
        return statement;
    }
}