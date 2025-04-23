from typing import Type
from pyarrow import Table


def Proceso_campañas_final(
    base_semaforos_vtas: Table,
    config: dict,
    Pandas_Functions: Type,
    General_Functions: Type,
) -> Table:
    """Encapsula el procedimiento para modificar la maestra_activos_sap y retorna la maestra_activos_sap modificada.
    Modula el proceso de modificación de la maestra_actitvos_sap para evitar aplicarlo directamente en el módulo main.py.

    Args:
        base_semaforos_vtas (pa.Table): Tabla bidimensional de PyArrow que contiene las tranformaciones hasta el momento de la instnaciación de la función"

        Pandas_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto maestra_actitvos_sap. Localizada en: "Utils/Trasformation_functions/PandasBase_Transformer"

        config (dict): Diccionario extraido de config.yml. Del mismo se extraen los conjuntos de información necesarios.

        drivers (List[Dataframes]): Lista de drivers de información, ahora procesados como tablas de pyarrow, necesarios para el proceso de maestra_activos_sap

    Returns:
        base_semaforos_vtas_campañas (pa.Table) : Tabla bidimensional de PyArrow que contiene las tranformaciones de los estados de las campañas , y las campañas agrupadas. (Diferentes transformaciones y generación de columnas a partir de ya establecidas.)
    """

    def reemplazar_valor(valor):
        if valor == "0":
            return False
        else:
            return True

    # Función para reemplazar los valores según la condición
    def reemplazar_estrategia(row):
        if row[config_camp["cols_necesarias"]["Estrategia_Agrupada"]] != 0 :
            return row[config_camp["cols_necesarias"]["Tipo Activo"]]
        else:
            return None

    # Crear una copia para trabajar.
    base_semaforos_vtas_copy = base_semaforos_vtas.copy()

    # Extraemos la configuración de la base campañas.
    config_camp = config["base_final"]["config_para_campañas"]

    # Duplicar columnas campañas en configuración.
    cols_campañas = list(config["estrategias_semaforo"].keys())

    # Generar lista de nombres para las columnas de estrategias (cols duplicadas)
    cols_estgia = [f"Estrg_{col}" for col in cols_campañas]

    # Generar diccionario a partir de las dos listas anteriores "cols_campañas" y "cols_estgia"
    dict_mapeo_estg = General_Functions.listas_a_diccionario(
        claves=cols_campañas, valores=cols_estgia
    )

    # Generar columnas duplicadas.
    base_semaforos_vtas_copy_dup = Pandas_Functions.duplicar_columnas_pd(
        df=base_semaforos_vtas_copy, mapeo_columnas=dict_mapeo_estg
    )

    # Reemplazar los valores por True o False según el caso especifico. Para toda la columna especifica."

    for cada_col in cols_estgia:
        base_semaforos_vtas_copy_dup[cada_col] = base_semaforos_vtas_copy_dup[
            cada_col
        ].apply(reemplazar_valor)

    # Generar la columna Estrategia_Agrupada.
    base_semaforos_vtas_copy_dup[
        config_camp["cols_necesarias"]["Estrategia_Agrupada"]
    ] = base_semaforos_vtas_copy_dup[cols_estgia].sum(axis=1)

    # Reemplazar los valores de la estrategia agrupada
    base_semaforos_vtas_copy_dup[
        config_camp["cols_necesarias"]["Estrategia_Agrupada"]
    ] = base_semaforos_vtas_copy_dup.apply(reemplazar_estrategia, axis=1)
    
    # Reemplazar finalmente los True y False por "Si" y "No"

    # Retrasformar el tipo de dato.
    base_semaforos_vtas_copy_tf = (
        Pandas_Functions.Cambiar_tipo_dato_multiples_columnas_pd(
            base=base_semaforos_vtas_copy_dup, list_columns=cols_estgia, type_data=str
        )
    )

    # Reemplazar valores anteriores segun dict con diccionario.
    for cada_col in cols_estgia:
        base_semaforos_vtas_camp = Pandas_Functions.Reemplazar_valores_con_dict_pd(
            df=base_semaforos_vtas_copy_tf,
            columna=cada_col,
            diccionario_mapeo=config_camp["dict_reemplazos_estg"],
        )

    return base_semaforos_vtas_camp
    
