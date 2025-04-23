# Transformaciones adicionales del universo de la directa.

import copy
from typing import Type
import pyarrow as pa
from datetime import datetime


def Trasformaciones_universos(
    universo_directa: pa.Table,
    universo_indirecta: pa.Table,
    Pyarrow_Functions: Type,
    Pandas_Functions: Type,
    config: dict,
) -> pa.Table:
    """Encapsula el procedimiento para modificar la base universo_directa y retorna la misma modificada.
    Modula el proceso de modificación de la base universo_directa para evitar aplicarlo directamente en el módulo main.py.

    Args:
        universo_directa (pa.Table): Tabla bidimensional de PyArrow que contiene la información original del archivo en: config['Insumos']['Universos_directa_indirecta']['nom_base_dir']"
        universo_indirecta (pa.Table): (pa.Table): Tabla bidimensional de PyArrow que contiene la información original del archivo en: config['Insumos']['Universos_directa_indirecta']['nom_base_indir']"
        Pyarrow_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto universo_directa. Localizada en: "Utils/Trasformation_functions/PyArrowColumnTransformer"

        config (dict): Diccionario extraido de config.yml. Del mismo se extraen los conjuntos de información necesarios.

    Returns:
        unvierso_directa_def (pa.Table) : universo_directa modificada.
    """

    """ Tener en cuenta que se toma parte de la configuración para esta ejecución: """

    # Creamos una copia profunda de la base original para trabajar con ella.
    universo_directa_copy = copy.deepcopy(universo_directa)
    universo_indirecta_copy = copy.deepcopy(universo_indirecta)

    # Transformaciones Universo directa.

    # Renombrar columnas necesrias. Universo directa.
    universo_directa_copy = Pandas_Functions.Renombrar_columnas_con_diccionario(
        base=universo_directa_copy,
        cols_to_rename=config["Insumos"]["Universos_directa_indirecta"][
            "renombrar_cols"
        ],
    )
    universo_indirecta_copy = Pandas_Functions.Renombrar_columnas_con_diccionario(
        base=universo_indirecta_copy,
        cols_to_rename=config["Insumos"]["Universos_directa_indirecta"][
            "renombrar_cols"
        ],
    )

    Universo_clientes_def = Pandas_Functions.concatenate_dataframes(dataframes=[universo_directa_copy,universo_indirecta_copy])
    return Universo_clientes_def
