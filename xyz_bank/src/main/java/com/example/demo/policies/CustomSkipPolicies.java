package com.example.demo.policies;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;
import org.springframework.dao.DataIntegrityViolationException;
import java.sql.BatchUpdateException;



@Component
public class CustomSkipPolicies implements SkipPolicy {
    
    // Aumentamos el límite a 1000 para soportar la alta corrupción de los CSV legacy
    private static final int MAX_SKIP_COUNT = 1000;

    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        
        if (skipCount >= MAX_SKIP_COUNT) {
            throw new SkipLimitExceededException(MAX_SKIP_COUNT, t);
        }

        // Permitimos saltar errores de lectura, matemáticas y de conversión (BindException)
        if (t instanceof FlatFileParseException || 
            t instanceof NumberFormatException || 
            t instanceof org.springframework.validation.BindException ||
            t instanceof DataIntegrityViolationException ||
            t instanceof java.sql.BatchUpdateException) {
            return true; 
        }
        
        return false;
    }
}