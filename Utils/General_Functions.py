import yaml
import time
import pandas as pd
from loguru import logger
from datetime import timedelta


def Registro_tiempo(original_func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = original_func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(
            f"Tiempo de ejecución de {original_func.__name__}: {execution_time} segundos"
        )
        return result

    return wrapper


def Procesar_configuracion(nom_archivo_configuracion: str) -> dict:
    """Lee un archivo YAML de configuración para un proyecto.

    Args:
        nom_archivo_configuracion (str): Nombre del archivo YAML que contiene
            la configuración del proyecto.

    Returns:
        dict: Un diccionario con la información de configuración leída del archivo YAML.
    """
    try:
        with open(nom_archivo_configuracion, "r", encoding="utf-8") as archivo:
            configuracion_yaml = yaml.safe_load(archivo)
        logger.success("Proceso de obtención de configuración satisfactorio")
    except Exception as e:
        logger.critical(f"Proceso de lectura de configuración fallido {e}")
        raise e

    return configuracion_yaml


def imprimir_tiempo_estimado(diferencia: timedelta):
    # Convertimos la diferencia en segundos
    diferencia_en_segundos = diferencia.total_seconds()

    # Calculamos los minutos y segundos
    minutos = int(diferencia_en_segundos // 60)
    segundos = int(diferencia_en_segundos % 60)

    # Usamos logger.info para imprimir el mensaje
    logger.info(f"Tiempo estimado del proceso: {minutos} minutos y {segundos} segundos")


@Registro_tiempo
def exportar_a_excel(
    ruta_guardado: str, df: pd.DataFrame, nom_hoja: str, index: bool = False
) -> None:
    """
    Exporta un dataframe de pandas a un archivo excel en la ruta especificada.

    Args:
        ruta_guardado: Ruta donde se guardará el archivo excel.
        df: Dataframe de pandas que se exportará.
        nom_hoja: Nombre de la hoja de cálculo donde se exportará el dataframe.
        index: Indica si se debe incluir el índice del dataframe en el archivo excel.

    Returns:
        None.

    Raises:
        FileNotFoundError: Si la ruta de guardado no existe.
    """

    # Comprobar que la ruta de guardado existe
    try:
        logger.info(f"Exportando a excel: {nom_hoja}")
        df.to_excel(
            ruta_guardado + nom_hoja + ".xlsx", sheet_name=nom_hoja, index=index
        )
    except Exception as e:
        raise Exception


@Registro_tiempo
def Lectura_insumos_excel(
    path: str, nom_insumo: str, nom_hoja: str , engine = "openpyxl", cols: int | list = None, 
) -> pd.DataFrame:
    """Lee archivos de Excel con cualquier extensión y carga los datos de una hoja específica.

    Lee el archivo especificado por `nom_insumo` ubicado en la ruta `path` y carga los datos de la hoja
    especificada por `nom_Hoja`. Selecciona solo las columnas indicadas por `cols`.

    Args:
        path (str): Ruta de la carpeta donde se encuentra el archivo.
        nom_insumo (str): Nombre del archivo con extensión.
        nom_Hoja (str): Nombre de la hoja del archivo que se quiere leer.
        cols (int): Número de columnas que se desean cargar.

    Returns:
        pd.DataFrame: Dataframe que contiene los datos leídos del archivo Excel.

    Raises:
        Exception: Si ocurre un error durante el proceso de lectura del archivo.
    """
    base_leida = None

    try:
        logger.info(f"Inicio lectura {nom_insumo} Hoja: {nom_hoja}")
        base_leida = pd.read_excel(
            path + nom_insumo,
            sheet_name=nom_hoja,
            usecols=list(range(0, cols)),
            dtype=str,
            engine=engine,
        )

        logger.success(
            f"Lectura de {nom_insumo} Hoja: {nom_hoja} realizada con éxito"
        )  # Se registrará correctamente con el método "success"
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise Exception

    return base_leida


def Lectura_simple_excel(path: str, nom_insumo: str, nom_hoja=None) -> pd.DataFrame:
    """Lee archivos de Excel con cualquier extensión y carga los datos de una hoja específica.

    Lee el archivo especificado por `nom_insumo` ubicado en la ruta `path` y carga los datos de la hoja
    especificada por `nom_Hoja`. Selecciona solo las columnas indicadas por `cols`.

    Args:
        path (str): Ruta de la carpeta donde se encuentra el archivo.
        nom_insumo (str): Nombre del archivo con extensión.
        nom_Hoja (str): Nombre de la hoja del archivo que se quiere leer.
        

    Returns:
        Si nom_hoja != None 
            pd.DataFrame: Dataframe que contiene los datos leídos del archivo Excel.
        Si nom_hoja == None
            dict ({sheets : pd.Dataframe}) : Diccionario cuyas claves son los nombres de las hojas del archivo .xlsx , y los valores son de tipo pd.Dataframe. 
    Raises:
        Exception: Si ocurre un error durante el proceso de lectura del archivo.
    """
    base_leida = None

    try:
        logger.info(f"Inicio lectura {nom_insumo}")
        if nom_hoja == None:
            base_leida = pd.read_excel(
                path + nom_insumo, dtype=str, sheet_name=nom_hoja
            )
        else:
            base_leida = pd.read_excel(
                path + nom_insumo, dtype=str, 
            )

        logger.success(
            f"Lectura de {nom_insumo} realizada con éxito"
        )  # Se registrará correctamente con el método "success"
    except Exception as e:
        logger.error(f"Proceso de lectura fallido: {e}")
        raise Exception

    return base_leida


def eliminar_elementos_lista(lista_original, elementos_a_eliminar):
    # Filtrar los elementos que no están en el conjunto de elementos a eliminar
    lista_filtrada = list(
        filter(lambda x: x not in elementos_a_eliminar, lista_original)
    )
    return lista_filtrada


def listas_a_diccionario(claves: list, valores: list):
    """
    Intenta crear un diccionario a partir de dos listas, una para las claves y otra para los valores.
    Registra un mensaje de error y lanza una excepción si las listas no tienen el mismo tamaño.
    Utiliza loguru para el registro de mensajes.

    Parámetros:
    - claves (list): Lista de claves.
    - valores (list): Lista de valores correspondientes a cada clave.

    Retorna:
    - Un diccionario creado a partir de las listas de claves y valores si tienen el mismo tamaño.

    Lanza:
    - ValueError: Si las longitudes de las listas de claves y valores no son iguales.
    """
    try:
        # Asegurarse de que las listas tengan el mismo tamaño antes de proceder
        assert len(claves) == len(
            valores
        ), "Las listas de claves y valores deben tener el mismo tamaño."

        logger.info("Creando diccionario con claves y valores proporcionados.")

        return dict(zip(claves, valores))

    except AssertionError as error:
        logger.error(error)
        raise ValueError(error) from None
