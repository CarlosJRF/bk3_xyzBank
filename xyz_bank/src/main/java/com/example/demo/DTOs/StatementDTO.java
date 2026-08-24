package com.example.demo.DTOs;

import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.Data;

@Data
public class StatementDTO {

    private int id;
    private int accountId;
    private int statementYear;
    private BigDecimal totalIn;
    private BigDecimal totalOut;
    private BigDecimal finalBalance;
    private LocalDate createdAt;

}
