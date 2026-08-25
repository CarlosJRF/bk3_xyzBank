package com.example.demo.policies;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

@Component
public class CustomSkipPolicies implements SkipPolicy {

    // Definimos el límite máximo de errores tolerados por Step
    private static final int MAX_SKIP_COUNT = 10;

    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        
        // 1. Si superamos el límite, lanzamos la excepción nativa para detener el Job
        if (skipCount >= MAX_SKIP_COUNT) {
            throw new SkipLimitExceededException(MAX_SKIP_COUNT, t);
        }

        // 2. Evaluamos si el error es "saltable"
        if (t instanceof FlatFileParseException) {
            // Permitimos saltar errores de lectura o malformación del CSV
            return true; 
        } 
        
        if (t instanceof NumberFormatException) {
            // Permitimos saltar errores matemáticos o de conversión de texto a número
            return true; 
        }

        // 3. Cualquier otro error crítico (ej. caída de base de datos) NO se salta y detiene el proceso
        return false;
    }
}
