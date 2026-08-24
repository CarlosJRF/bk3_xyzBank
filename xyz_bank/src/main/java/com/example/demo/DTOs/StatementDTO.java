package com.example.demo.DTOs;

import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.Data;

@Data
public class StatementDTO {
    private int accountId;
    private LocalDate statementDate;
    private String transaction;
    private BigDecimal amount;
    private String description;
}
