# Usera Housing Prices

## Resumen
- El script `fetch_usera_prices.py` descarga y normaliza series de precios de compraventa y alquiler de vivienda para el distrito de Usera y sus barrios.
- Las salidas por defecto son `data/usera_housing_prices_monthly.csv` (mensual con valores y metadatos) y `data/usera_housing_prices_yearly.csv` (promedios anuales).

## Fuentes originales
- Banco de Datos del Ayuntamiento de Madrid (Idealista). Series consultadas:
  - Ventas 2000-2018: `0504030000151` → https://servpub.madrid.es/CSEBD_WBINTER/seleccionSerie.html?numSerie=0504030000151
  - Ventas 2019-2022 (barrios): `0504030000202` → https://servpub.madrid.es/CSEBD_WBINTER/seleccionSerie.html?numSerie=0504030000202
  - Ventas 2023 en adelante (metodologia actual): `0504030000153` → https://servpub.madrid.es/CSEBD_WBINTER/seleccionSerie.html?numSerie=0504030000153
  - Alquiler por distritos: `0504030000213` → https://servpub.madrid.es/CSEBD_WBINTER/seleccionSerie.html?numSerie=0504030000213
- Cada URL abre la pagina de configuracion del Banco de Datos; desde ahi se descargan los CSV con los mismos filtros que aplica el script.

## Metodologia de procesamiento
- Se fuerza el agente de usuario y se respetan los limites del portal (`requests` con cabecera estilo navegador y `REQUEST_TIMEOUT=45`).
- Se seleccionan los valores de Ano, Distrito, Barrio (si existe) y la variable temporal disponible. Para series que solo devuelven columnas anuales (p. ej. 2019-2022), se interpretan como valores de diciembre.
- Los precios de venta se publican con punto como separador de miles (`2.043` significa 2043 €/m2). El script elimina los puntos y convierte la coma decimal antes de pasar a `float`.
- Los precios de alquiler mantienen formato decimal tradicional; solo se sustituyen comas por puntos en caso necesario.
- El CSV mensual preserva el detalle territorial (`district` o `barrio`) y marca `price_eur_m2` como `null` si el portal ofrece `..`, `-` o `0`.
- El CSV anual agrega promedios por territorio (ignorando meses sin dato) y expone el numero de observaciones.

## Ejecucion
- Prerrequisitos: Python 3.12+ y dependencias listadas (`requests`).
- Ejemplo rapido desde esta carpeta:
  ```bash
  python3 fetch_usera_prices.py \
      --district-label "12. Usera" \
      --monthly-output data/usera_housing_prices_monthly.csv \
      --yearly-output data/usera_housing_prices_yearly.csv
  ```
- El script admite otros distritos cambiando `--district-label` (el texto debe coincidir con el listado oficial del portal).

## Notas
- Valores anomalos muy bajos (p. ej. ~2 €/m2) suelen deberse a interpretar literalmente los puntos como decimales. La normalizacion incluida los convierte en miles, de modo que Orcasitas y el resto de barrios quedan en el rango 1.4k-2.5k €/m2.
- Si el portal cambia la estructura de filtros, puede ser necesario ajustar `build_time_mapping` o el flujo de seleccion para nuevos identificadores.
