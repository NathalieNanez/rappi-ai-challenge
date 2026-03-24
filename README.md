# 🚀 Rappi AI System: Inteligencia Operacional Proactiva

## 📖 Resumen Ejecutivo
El **Rappi AI System** es un motor de decisiones diseñado para democratizar el acceso a métricas críticas en los equipos de Strategy, Planning & Analytics (SP&A) y Operaciones de Rappi. A través de una interfaz de lenguaje natural, el sistema permite diagnosticar anomalías, trackear tendencias y ejecutar benchmarks complejos, reduciendo la latencia de respuesta de horas de análisis manual a segundos de ejecución determinística.

## 🏗️ Arquitectura del Sistema
El proyecto sigue una **Arquitectura Hexagonal (Ports & Adapters)**, garantizando que la lógica de negocio sea independiente de las herramientas externas:

- **Domain Core:** Lógica de cálculo de métricas operacionales, detección de anomalías y reglas de negocio específicas de Rappi.
- **Adapters de Infraestructura:**
  - **Query Engine:** DuckDB para procesamiento OLAP de alto rendimiento (In-memory).
  - **LLM Interface:** Claude 3.5 Sonnet para razonamiento analítico y generación de SQL.
  - **Communication:** API de Brevo para la distribución proactiva de reportes.
- **Entry Points:** Interfaz dual vía FastAPI (para escalabilidad programática) y Streamlit (para consumo humano interactivo).

## 🧠 El Motor de Agente (ReAct + Self-Healing SQL)
El sistema implementa el patrón **ReAct** (Reasoning and Acting), permitiendo una observabilidad total sobre el proceso de pensamiento de la IA:

- **Pensamiento (Reasoning):** El agente analiza el esquema de las tablas y el diccionario de métricas para decidir la mejor estrategia de consulta.
- **Acción (Action):** Genera y ejecuta SQL sobre DuckDB.
- **Auto-Corrección (Self-Healing):** Si una consulta SQL falla por sintaxis o falta de datos, el agente captura el error, razona la causa y reescribe la consulta automáticamente antes de responder.
- **Memoria Conversacional:** Mantenimiento de estado para permitir hilos de investigación profundos (ej: "¿Y cuántas de esas zonas son de México?").

## 🛡️ Reglas y Guardrails de Calidad de Datos (Senior Constraints)
Para garantizar la confiabilidad necesaria en una operación de la escala de Rappi:

- **Deduplicación Estructural:** Capa de limpieza en `loader.py` que elimina redundancias en metadatos de zonas (Country/City/Zone) mediante llaves compuestas.
- **Data Audit Proactivo:** El sistema identifica valores imposibles (ej. Lead Penetration > 100% o valores anómalos en Ecuador) y los reporta como hallazgos de calidad de datos en lugar de procesarlos ciegamente.
- **Aislamiento de Dominio:** Credenciales y configuraciones sensibles gestionadas estrictamente vía variables de entorno (`.env`).

## 🖥️ Guía de Uso de la Interfaz (UI)
La UI está dividida estratégicamente según el perfil del usuario:

### 1. 💬 Chat de Datos (Análisis Ad-hoc)
- **Uso:** Preguntas libres sobre el dataset (Tendencias, Agregaciones, Comparaciones).
- **Visualización:** Generación dinámica de gráficas interactivas en **Plotly** (Líneas, Barras, Dispersión, Heatmaps y Boxplots) para explorar series temporales, distribuciones y correlaciones.
- **Exportación:** Botones integrados para descargar los resultados específicos de la consulta en CSV y PDF.

### 2. � Reporte de Insights (Análisis Proactivo)
- **Uso:** Visión ejecutiva de "incendios" y oportunidades sin necesidad de prompts.
- **Semáforos Operacionales:** Clasificación automática en 🔴 Anomalías, 🟠 Tendencias de deterioro y 🟢 Oportunidades de expansión.
- **Distribución:** Envío del reporte formateado en PDF directamente al correo del solicitante mediante un solo clic.

## 💰 Monitoreo de Costos (FinOps)
Como estándar de eficiencia en proyectos de IA Generativa, la interfaz incluye un Monitor de Gastos en Tiempo Real:

- **Costo Estimado (Claude 3.5 Sonnet):** Cálculo dinámico basado en tokens de entrada y salida de cada sesión.
- **Transparencia:** Permite a los administradores del sistema auditar el costo operativo por cada consulta analítica.

## 🛠️ Configuración y Despliegue (Docker)
El sistema está completamente contenedorizado para asegurar la reproducibilidad total.

### Pasos para Ejecución

1. **Configurar Variables de Env (`.env`):**
   ```env
   ANTHROPIC_API_KEY=tu_api_key_de_anthropic
   BREVO_API_KEY=tu_api_key_de_brevo
   BREVO_SENDER_EMAIL=tu_correo_verificado@gmail.com
   DATA_PATH=data/raw/rappi_data.xlsx
   ```

2. **Levantar el Sistema:**
   ```bash
   docker-compose up --build
   ```

## 📐 Estándares de Ingeniería
- **Tipado Estricto:** Uso de Pydantic para validación de esquemas bidireccionales.
- **Observabilidad:** Logging detallado de la cadena de razonamiento (Thought/Action/Observation) en la consola del contenedor.
- **Eficiencia OLAP:** Implementación de DuckDB para evitar latencias de red y costos de cómputo en la nube durante la fase de exploración.

## 🔒 Nota de Seguridad
Gestión de Credenciales: Los secretos y llaves de API se administran estrictamente mediante variables de entorno; no se incluye información sensible ni archivos .env en el control de versiones.

## Costo Estimado de Operación:
Basado en pruebas con Claude 3.5 Sonnet, el costo promedio es de ~$0.02 USD por consulta compleja. Una sesión de análisis profundo de 10 preguntas tiene un costo proyectado de $0.20 USD.
Guardrails del Agente: El agente ReAct está restringido a herramientas analíticas específicas y predefinidas, lo que mitiga el riesgo de inyección de prompts o ejecución de código arbitrario.

Infraestructura: El uso de Docker proporciona un entorno de ejecución aislado (sandboxed), garantizando la seguridad a nivel de sistema y protegiendo el host de accesos no autorizados.
