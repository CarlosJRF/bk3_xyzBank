package com.example.demo.processor;

import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.example.demo.DTOs.TransactionDTO;

public class TransactionProcessor implements ItemProcessor<TransactionDTO, TransactionDTO> {

    @Override
    public TransactionDTO process(TransactionDTO item) throws Exception{
        if(item.getAmount() < 0){
            System.out.println("Registro invalido detectado");
            return null;
        }

        TransactionDTO itemOut = new TransactionDTO();
        itemOut.setAmount(item.getAmount());

        return itemOut;
    }

    



}
