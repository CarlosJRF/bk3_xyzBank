package com.example.demo.processor;

import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.example.demo.DTOs.TransactionDTO;

@Component
public class TransactionProcessor implements ItemProcessor<TransactionDTO, TransactionDTO> {

    @Override
    public TransactionDTO process(TransactionDTO item) throws Exception {
        // Validamos si el monto es negativo
        if (item.getAmount() < 0) {
            System.out.println("Registro invalido detectado");
            return null; // Al retornar null, Spring Batch descarta este registro y no lo inserta
        }

        // Si el registro es válido, retornamos el objeto original con todos sus datos intactos
        return item;
    }

}