package com.example.demo.DTOs;

import java.time.LocalDate;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class TransactionDTO {

    private int id;
    private LocalDate transactionDate;
    private int amount;
    private String type;

}
