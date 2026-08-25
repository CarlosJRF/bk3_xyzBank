# XYZ Bank - Procesamiento Batch Financiero

Aplicación ETL (Extract, Transform, Load) desarrollada con Java y Spring Batch para automatizar la ingesta, cálculo y validación de registros financieros del banco XYZ. 

Este proyecto académico demuestra la implementación de procesamiento por lotes para manejar grandes volúmenes de datos mediante transacciones seguras, garantizando la integridad de los saldos y reportes de auditoría.

## 🚀 Tecnologías Utilizadas

*   **Java 21**
*   **Spring Boot** (v3.x / v2.x)
*   **Spring Batch**: Orquestación de trabajos (Jobs), pasos (Steps) y procesamiento por chunks.
*   **PostgreSQL**: Base de datos relacional para persistencia.
*   **Lombok**: Reducción de código repetitivo (boilerplate) en los DTOs.
*   **Maven**: Gestión de dependencias y construcción del proyecto.

## ⚙️ Arquitectura del Job (`transactionJob`)

El procesamiento está dividido en un flujo secuencial estricto de tres fases. Si un paso falla, el trabajo se detiene para prevenir inconsistencias en los datos financieros.

### Fase 1: Carga de Transacciones (`transactionStep`)
*   **Reader:** Lee registros desde `transacciones.csv`.
*   **Processor:** Invalida y descarta registros con montos negativos.
*   **Writer:** Inserta los datos validados en la tabla `transaction_report`.

### Fase 2: Cálculo de Intereses (`interestStep`)
*   **Reader:** Lee saldos y tipos de cuenta desde `intereses.csv`.
*   **Processor:** Aplica lógica de negocios utilizando *Switch Expressions* de Java para calcular el nuevo saldo en base a tasas específicas:
    *   `AHORRO`: +0.5%
    *   `PRESTAMO`: +2.0%
    *   `HIPOTECA`: +3.0%
*   **Writer:** Registra las cuentas y sus saldos actualizados en la tabla `account`.

### Fase 3: Auditoría Anual (`statementStep`)
*   **Reader:** Extrae el reporte detallado desde `cuentas_anuales.csv`.
*   **Processor:** Estandariza las descripciones a mayúsculas para facilitar búsquedas y audita los datos.
*   **Writer:** Guarda el estado de cuenta histórico en la tabla `annual_statement`.

## 🗄️ Modelo de Datos (PostgreSQL)

El sistema genera e interactúa con las siguientes tablas:
1.  `transaction_report`: Historial de operaciones transaccionales.
2.  `account`: Catálogo de cuentas, tipos y saldos actualizados post-intereses.
3.  `annual_statement`: Registro detallado de movimientos para auditorías anuales.

## 🛠️ Configuración y Ejecución

### 1. Preparar la Base de Datos
Asegúrate de tener PostgreSQL ejecutándose localmente en el puerto `5432`. Crea una base de datos llamada `xyzbank`.
Las credenciales por defecto configuradas en `application.properties` son:
*   **Usuario:** `postgres`
*   **Password:** `c1j9r9f3!psql`

### 2. Archivos de Entrada (CSV)
Los siguientes archivos deben estar ubicados en la ruta configurada en `BatchConfig`

*   `transacciones.csv`
*   `intereses.csv`
*   `cuentas_anuales_2.csv`

Los archivos estan integrados en el repositorio en la misma carpeta que contiene el README
La ruta especificada en el 'BatchConfig' fue la empleada para pruebas en local

### 3. Ejecutar la Aplicación
Puedes compilar y ejecutar el proyecto utilizando el wrapper de Maven:
```bash
./mvnw spring-boot:run