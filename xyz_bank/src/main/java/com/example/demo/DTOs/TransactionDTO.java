package com.example.demo.DTOs;

import java.time.LocalDate;
import lombok.Data;

@Data
public class TransactionDTO {

    private int id;
    private LocalDate transactionDate;
    private int amount;
    private String type;

}
