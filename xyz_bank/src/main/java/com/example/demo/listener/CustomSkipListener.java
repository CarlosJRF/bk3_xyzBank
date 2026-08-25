package com.example.demo.listener;

import org.springframework.batch.core.listener.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class CustomSkipListener implements SkipListener<Object, Object> {

    @Override
    public void onSkipInRead(Throwable t) {
        // Se ejecuta si el error ocurre al leer el CSV (ej. formato incorrecto, faltan columnas)
        System.err.println("⏭️ SKIP (Lectura) - Fila con formato inválido detectada: " + t.getMessage());
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        // Se ejecuta si el error ocurre dentro de tus Processors
        System.err.println("⏭️ SKIP (Procesamiento) - Error al procesar el registro [" + item + "]: " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        // Se ejecuta si la base de datos rechaza el registro (ej. violación de llave única)
        System.err.println("⏭️ SKIP (Escritura) - Error al insertar en BD el registro [" + item + "]: " + t.getMessage());
    }
}