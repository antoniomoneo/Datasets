# O*NET occupation utilities

Este módulo proporciona herramientas de línea de comandos y utilidades en Python para consultar el servicio beta de O*NET y almacenar resultados de ocupaciones.

## Requisitos

* Python 3.9 o superior.
* Dependencias: `requests`.
* Credenciales válidas de O*NET proporcionadas a través de las variables de entorno `ONET_USER` y `ONET_KEY`.

## Uso

El script principal es `fetch_onet_data.py`. Ejemplo de ejecución:

```bash
export ONET_USER="mi_usuario"
export ONET_KEY="mi_clave"
python -m onet.fetch_onet_data recursos humanos --max-results 10 --json-output onet/data/hr.json --csv-output onet/data/hr.csv
```

Parámetros disponibles:

* `keywords` (posicional): Palabras clave para la búsqueda. Se pueden indicar varias, separadas por espacios.
* `--max-results`: Número máximo de resultados que se recuperarán del endpoint de búsqueda (por defecto, 25).
* `--delay`: Pausa (en segundos) entre peticiones individuales al solicitar detalles de ocupaciones (por defecto, 0.2).
* `--base-url`: URL base del servicio O*NET (por defecto, `https://services-beta.onetcenter.org/ws`).
* `--json-output`: Ruta de archivo para los resultados completos en formato JSON.
* `--csv-output`: Ruta de archivo para la exportación resumida en formato CSV.

Los archivos de salida se escriben en el directorio `onet/data/` por defecto, que se crea automáticamente si no existe.

## Formato de salida

* **JSON**: Contiene un diccionario con las palabras clave utilizadas y una lista de ocupaciones. Cada ocupación incluye un resumen con código, título, familia profesional, descripción e indicador `is_hr_related`, además de los detalles completos devueltos por la API.
* **CSV**: Incluye una fila por ocupación con las columnas `onet_code`, `title`, `job_family`, `is_hr_related` y `description`.

## Notas

* Las peticiones al servicio O*NET incluyen un retardo configurable para evitar límites de velocidad.
* Si no se proporcionan credenciales válidas, el script finalizará mostrando un error.
