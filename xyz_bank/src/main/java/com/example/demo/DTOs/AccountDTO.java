package com.example.demo.DTOs;

import java.math.BigDecimal;
import lombok.Data;

@Data
public class AccountDTO {

    private int id;
    private String clientName;
    private BigDecimal balance;
    private String age;
    private String accountType;


}
