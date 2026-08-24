package com.example.demo.processor;

import java.math.BigDecimal;
import java.time.LocalDate;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;
import com.example.demo.DTOs.AccountDTO;


@Component
public class InterestProcessor implements ItemProcessor<AccountDTO, AccountDTO>{

    //Definiendo las tasas de interes

    private static final BigDecimal SAVINGS_RATE = new BigDecimal("0.005");// 0.5%
    private static final BigDecimal LOAN_RATE = new BigDecimal("0.02"); //2.0%
    private static final BigDecimal MORTAGE_RATE = new BigDecimal("0.03");//3.0% para Hipoteca, no esta en el instructivo pero si el csv

    @Override
    public AccountDTO process(AccountDTO account) throws Exception {
        BigDecimal currentBalance = account.getBalance();

        //Asignacion de la tasa utilizando switch

        BigDecimal interestRate = switch(account.getAccountType().toUpperCase()){
            case "AHORRO" -> SAVINGS_RATE;
            case "PRESTAMO" -> LOAN_RATE;
            case "HIPOTECA" -> MORTAGE_RATE;
            default -> BigDecimal.ZERO; //Otra cuenta por defecto
        };

        //Calculo de interes en caso de otra cuenta
        if(interestRate.compareTo(BigDecimal.ZERO) > 0){
            BigDecimal interest = currentBalance.multiply(interestRate);
            account.setBalance(currentBalance.add(interest));
        }


        //actualizacion de fecha en caso de aplicar interes
        account.setLastInterestDate(LocalDate.now());

        return account;

    }

    
    
}
