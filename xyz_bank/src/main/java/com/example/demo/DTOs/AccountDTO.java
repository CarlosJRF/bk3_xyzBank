package com.example.demo.DTOs;

import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.Data;

@Data
public class AccountDTO {

    private int id;
    private String accountNumber;
    private String accountType;
    private BigDecimal balance;
    private LocalDate lastInterestDate;


}
