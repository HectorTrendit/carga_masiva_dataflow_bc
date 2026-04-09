# Plantilla Flexible de Dataflow (Flex Template)

Este documento describe cómo construir y ejecutar una **Flex Template** para el pipeline `LoadBigQueryPipeline`. Este enfoque es más moderno y flexible que las plantillas clásicas.

## Configuración Previa (Solo una vez)

Antes de construir, debes configurar tu proyecto.

1.  **Habilita las APIs:** Asegúrate de tener las siguientes APIs habilitadas en tu proyecto de GCP:
    *   `Compute Engine API`
    *   `Dataflow API`
    *   `Cloud Build API`
    *   `Container Registry API`

2.  **Actualiza los Archivos de Configuración:**
    *   **En `pom.xml`**:
        *   Busca la sección del `jib-maven-plugin`.
        *   En `<to><image>`, reemplaza `tu-gcp-project-id` con el ID de tu proyecto de Google Cloud.
        *   Ejemplo: `<image>gcr.io/mi-proyecto-123/bancopel/load-bigquery-flex-template</image>`
    *   **En `flex_template.json`**:
        *   En la línea `"image"`, reemplaza `tu-gcp-project-id` con el mismo ID de tu proyecto.
        *   Ejemplo: `"image": "gcr.io/mi-proyecto-123/bancopel/load-bigquery-flex-template:latest"`

## 1. Construir la Plantilla

El siguiente comando compilará tu código, lo empaquetará en una imagen de Docker, subirá la imagen a Google Container Registry y finalmente creará la especificación de la plantilla en un bucket de Google Cloud Storage.

**Variables de Entorno:**
Primero, define estas variables en tu terminal:

```sh
# Reemplaza con los valores de tu proyecto
export GCP_PROJECT="tu-gcp-project-id"
export GCS_BUCKET="tu-bucket-de-gcs"
export TEMPLATE_NAME="load-bigquery-flex"
export TEMPLATE_IMAGE="gcr.io/${GCP_PROJECT}/bancopel/load-bigquery-flex-template:latest"
```

**Comando de Construcción:**
Ahora, ejecuta los siguientes comandos desde el directorio `dataflow_sample/java`:

```sh
# 1. Empaqueta el código y construye la imagen de Docker
mvn clean package -DskipTests

# 2. Sube la imagen a Google Container Registry
gcloud auth configure-docker
mvn jib:build -Dimage=${TEMPLATE_IMAGE}

# 3. Crea la especificación de la plantilla en GCS
gcloud dataflow flex-template build "gs://${GCS_BUCKET}/templates/${TEMPLATE_NAME}.json" 
    --image "${TEMPLATE_IMAGE}" 
    --sdk-language "JAVA" 
    --metadata-file "flex_template.json"
```

**Verificación:**
Si todo ha ido bien, verás un archivo `load-bigquery-flex.json` en tu bucket de GCS, dentro de la carpeta `templates`.

## 2. Ejecutar el Job desde la Plantilla

¡Esta es la parte que querías! Ahora puedes ejecutar tu pipeline con un solo comando, pasando todos los parámetros que necesites.

```sh
gcloud dataflow flex-template run "load-bq-$(date +%Y%m%d-%H%M%S)" 
    --template-file-gcs-location "gs://${GCS_BUCKET}/templates/${TEMPLATE_NAME}.json" 
    --project "${GCP_PROJECT}" 
    --region "us-central1" 
    --parameters 
input="gs://tu-bucket-de-datos/entrada/*.json",bqTable="${GCP_PROJECT}:tu_dataset.tu_tabla",bqTempLocation="gs://${GCS_BUCKET}/temp/",deadletter="gs://${GCS_BUCKET}/deadletter/",errorTable="${GCP_PROJECT}:tu_dataset.tabla_errores"
```

### Control Total

Como puedes ver, en el comando `gcloud dataflow flex-template run` controlas todo:
- **El proyecto** donde se ejecuta (`--project`).
- **La región** (`--region`).
- **Todos los parámetros de tu pipeline** (`--parameters ...`).

Este es el flujo de trabajo moderno para Dataflow y te da la flexibilidad que estabas buscando.
