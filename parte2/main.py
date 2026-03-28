import os
import logging
from typing import List, Dict, Any
import requests
from google.cloud import bigquery


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class ApiDownloader:
    # Descarga datos desde una API pública

    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url
        self.timeout = timeout

    def fetch_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        # Descarga datos de la API.
        # Args: limit: número máximo de registros a descargar.
        # Returns: Lista de diccionarios con los datos descargados.
        
        # Parámetros de consulta para limitar el número de registros
        params = {"_limit": limit}

        # Realiza la solicitud GET a la API y maneja posibles errores
        logging.info("Descargando datos desde la API: %s", self.base_url)
        response = requests.get(self.base_url, params=params, timeout=self.timeout)
        response.raise_for_status()

        # Convierte la respuesta a JSON
        data = response.json()

        # Verifica que la respuesta sea una lista de registros
        if not isinstance(data, list):
            raise ValueError("La respuesta de la API no es una lista de registros.")

        logging.info("Se han descargado %s registros", len(data))
        return data


class BigQueryUploader:
    # Sube datos a una tabla de BigQuery.

    # Args: project_id: ID del proyecto de GCP. dataset_id: ID del dataset de BigQuery. table_id: ID de la tabla de BigQuery. client: instancia del cliente de BigQuery.
    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)

    # Devuelve la referencia completa a la tabla de BigQuery en formato "project.dataset.table".
    def table_ref(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def upload_rows(self, rows: List[Dict[str, Any]]) -> None:
        # Sube filas en formato JSON a BigQuery.
        # Args: rows: lista de registros a subir.

        if not rows:
            logging.warning("No hay datos para subir a BigQuery.")
            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        logging.info("Subiendo %s registros a %s", len(rows), self.table_ref())

        load_job = self.client.load_table_from_json(
            rows,
            self.table_ref(),
            job_config=job_config
        )

        load_job.result()
        logging.info("Carga completada correctamente en BigQuery.")


def main() -> None:
    # API pública de ejemplo
    api_url = "https://jsonplaceholder.typicode.com/comments"

    # Variables de entorno para BigQuery
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    table_id = os.getenv("BQ_TABLE_ID")

    if not project_id or not dataset_id or not table_id:
        raise EnvironmentError(
            "Faltan variables de entorno: GCP_PROJECT_ID, BQ_DATASET_ID y BQ_TABLE_ID"
        )

    downloader = ApiDownloader(api_url)
    uploader = BigQueryUploader(project_id, dataset_id, table_id)

    data = downloader.fetch_data(limit=100)
    uploader.upload_rows(data)


if __name__ == "__main__":
    main()
