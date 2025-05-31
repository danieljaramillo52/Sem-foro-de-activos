import copy
from typing import Type, List
import pyarrow as pa
from datetime import datetime
from pandas import DataFrame, merge
import pandas as pd
from functools import reduce


def Transformaciones_drivers(
    drivers: List[DataFrame],
    Pandas_Functions: Type,
    config: dict,
) -> pa.Table:
    """Encapsula el procedimiento para modificar los drivers necesarios para la automatización (Dfn dirver, antes de la instancia de esta función en el main.py) y retorna los drivers modificados.
    Modula el proceso de modificación de la drivers para evitar aplicarlo directamente en el módulo main.py.

    Args:
        drivers: List[DataFrame]: Lista de dataframes, que contiene los dataframes correspondientes ya cargados en memoria por pandas que contiene la información original del archivo en: config['Drivers']['nom_base']"

        Pandas_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto maestra_actitvos_sap. Localizada en: "Utils/Trasformation_functions/PyArrowColumnTransformer"

        config (dict): Diccionario extraido de config.yml. Del mismo se extraen los conjuntos de información necesarios.

    Returns:
        drivers_mods List[ Dataframe] : drivers modficiados"""

    """ Tener en cuenta que se toma parte de la configuración en (config.yml) para esta ejecución: """

    COL_ESTRATEGIA = config["Drivers"]["cols_necesarias"]["Activos y Estrategias"][0]
    COL_COD_BARRAS = config["Drivers"]["cols_necesarias"]["Activos y Estrategias"][1]
    N_INVENTAR = config["Drivers"]["Nev_mantto"]["cols_duplicar"]["CÓDIGO DE BARRAS"]

    drv_ac_estra_orig = drivers[0]
    driver_ac_carg_orig = drivers[1]
    driver_manto_neveras = drivers[2]
    # Proceso de transformación de los drivers.

    # Crear copia de los dataframes originales para trbajar.
    drv_ac_estra_copy = drv_ac_estra_orig.copy()
    drv_ac_carg_copy = driver_ac_carg_orig.copy()
    drv_manto_neve = driver_manto_neveras.copy()


    """Buscamos crear dos drivers para el proceso. 1.) Driver_activos_indirecta 2.) Driver información de topes y campañas. """

    # Dict de activos indirecta.
    dict_act_indir = Pandas_Functions.Crear_diccionario_desde_dataframe(
        df=drv_ac_carg_copy,
        col_clave=config["Drivers"]["cols_necesarias"]["driver_act_indir"][0],
        col_valor=config["Drivers"]["cols_necesarias"]["driver_act_indir"][1],
    )

    # Driver de topes y campañas.
    drv_top_camps = Pandas_Functions.Seleccionar_columnas_pd(
        df=drv_ac_carg_copy,
        cols_elegidas=config["Drivers"]["cols_necesarias"][
            "driver_topes_campañas"
        ].keys(),
    )

    # Eliminar valores nulos del drv_top_camps
    drv_top_camps = drv_top_camps.dropna()
    # Vamos a segmentar el driver por estrategia.

    driver_ac_estra_select = Pandas_Functions.Seleccionar_columnas_pd(
        df=drv_ac_estra_copy, cols_elegidas=[COL_COD_BARRAS, COL_ESTRATEGIA]
    )

    # Duplicar la columna CÓDIGO DE BARRAS.
    for cada_driver in [driver_ac_estra_select, drv_manto_neve]: 
        cada_driver.loc[:, N_INVENTAR] = cada_driver[COL_COD_BARRAS].copy()

    df_por_estrategia = [drv_ac_estra_copy]

    for cada_estrategia in drv_ac_estra_copy[COL_ESTRATEGIA].unique():

        df_filtrado = Pandas_Functions.Filtrar_por_valores_pd(
            df=driver_ac_estra_select,
            columna=COL_ESTRATEGIA,
            valores_filtrar=cada_estrategia,
        )

        df_filtrado = df_filtrado.rename(columns={N_INVENTAR: cada_estrategia})

        # Eliminar columna adicional Estrategia.
        df_filtrado = df_filtrado.drop(columns=COL_ESTRATEGIA)

        # Agregar el df filtrado por estategia-
        df_por_estrategia.append(df_filtrado)

    df_estrategias = Pandas_Functions.merge_dfs_on_column(
        df_list=df_por_estrategia, key=COL_COD_BARRAS
    )

    # Renombrar columnas driver
    # Renombrar columnas drivers

    # Top campos
    drv_top_camps = Pandas_Functions.Renombrar_columnas_con_diccionario(
        base=drv_top_camps,
        cols_to_rename=config["Drivers"]["cols_necesarias"]["driver_topes_campañas"],
    )

    # Renombrar columnas drivers

    # Mantenimiento Neveras
    drv_manto_neve_rename = Pandas_Functions.Renombrar_columnas_con_diccionario(
        base=drv_manto_neve,
        cols_to_rename=config["Drivers"]["Nev_mantto"]["cols_rename"],
    )

    # Estrategias
    df_estrategias = Pandas_Functions.Renombrar_columnas_con_diccionario(
        base=df_estrategias,
        cols_to_rename={COL_COD_BARRAS: N_INVENTAR},
    )

    col_clave = [config["dict_constantes"]["NºInventar"]]
    cols_estrategias = list(config["estrategias_semaforo"].keys())
    cols_estrategias_completas = col_clave + cols_estrategias
    
    # Seleccionar columnas df_estrategias
    df_estrategias_select = Pandas_Functions.Seleccionar_columnas_pd(
        df=df_estrategias,
        cols_elegidas=cols_estrategias_completas,
    )

    return (
        drv_top_camps,
        dict_act_indir,
        df_estrategias_select,
        drv_manto_neve_rename.drop_duplicates(),

    )
