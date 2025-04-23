import copy
from typing import Type
import pyarrow as pa
from datetime import datetime


def Transformaciones_ma_act_indirecta(
    maestra_activos_indirecta: pa.Table,
    Pyarrow_Functions: Type,
    Pandas_Functions: Type,
    config: dict,
    drivers: list,
) -> pa.Table:
    """Encapsula el procedimiento para modificar la maestra_activos_indirecta y retorna la maestra_activos_indirecta modificada.
    Modula el proceso de modificación de la maestra_activos_indirecta para evitar aplicarlo directamente en el módulo main.py.

    Args:
        maestra_activos_indirecta (pa.Table): Tabla bidimensional de PyArrow que contiene la información original del archivo en: config['Insumos']['maestra_activos_indirecta']['nom_base']"

        Pyarrow_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto maestra_activos_indirecta. Localizada en: "Utils/Trasformation_functions/PyArrowColumnTransformer"

        config (dict): Diccionario extraido de config.yml. Del mismo se extraen los conjuntos de información necesarios.

        drivers (List[Dataframes]): Lista de drivers de información, ahora procesados como tablas de pyarrow, necesarios para el proceso de maestra_activos_indirecta

    Returns:
        maestra_act_def_i directa (pa.Table) : maestra_activos_indirecta modificada.
    """

    """ Tener en cuenta que se toma parte de la configuración para esta ejecución: """

    # Definimos constantes
    N_INVENTAR = config["Drivers"]["Nev_mantto"]["cols_duplicar"]["CÓDIGO DE BARRAS"]

    # Exraemos los drivers:
    df_drv_estrategias = drivers[0]
    df_drv_top_camps = drivers[1]
    df_drv_manto_neve = drivers[2]
    df_drv_nev_grtia = drivers[3]
    dict_act_indir = drivers[4]

    # Transformar drivers (dfs) a tablas de pyarrow
    table_drv_ac_estra = Pandas_Functions.Transform_dfs_pandas_a_pyarrow(
        df=df_drv_estrategias
    )
    table_drv_top_camps = Pandas_Functions.Transform_dfs_pandas_a_pyarrow(
        df=df_drv_top_camps
    )
    table_drv_manto_neve = Pandas_Functions.Transform_dfs_pandas_a_pyarrow(
        df=df_drv_manto_neve
    )
    table_drv_nev_grtia = Pandas_Functions.Transform_dfs_pandas_a_pyarrow(
        df=df_drv_nev_grtia
    )

    table_drv_top_camps = Pyarrow_Functions.cambiar_tipo_dato_columnas_pa(
        tabla=table_drv_top_camps,
        columnas=config["Drivers"]["cols_necesarias"]["Activos y Cargues"][
            "cols_cambiar_tipo_dato"
        ],
        nuevo_tipo=pa.float64(),
    )

    # Creamos una copia profunda de la base original para trabajar con ella.
    maestra_inactivos_copy = copy.deepcopy(maestra_activos_indirecta)

    # Renombar columnas de la base.
    maestra_inactivos_copy_rename = Pyarrow_Functions.Renombrar_cols_con_dict_pa(
        tabla=maestra_inactivos_copy,
        dict_renombrar=config["Insumos"]["Maestra_Activos_Indirecta"]["cols_renombrar"],
    )

    # Reemplazar valores en la columna Denominación objeto.
    maestra_inactivos_copy_rename = (
        Pyarrow_Functions.reemplazar_valores_con_diccionario_pa(
            tabla=maestra_inactivos_copy_rename,
            nombre_columna=config["Insumos"]["Maestra_Activos_Indirecta"][
                "cols_renombrar"
            ]["Nombre Activo Indirecta"],
            diccionario_de_mapeo=dict_act_indir,
        )
    )

    # Filtros maestra de inactivos.

    # Instanciamos la tabla para filtrar.
    Filtrar_tabla = Pyarrow_Functions.PyArrowTablefilter(maestra_inactivos_copy_rename)

    config_filtros = config["Insumos"]["Maestra_Activos_Indirecta"]["Filtros"]

    # Crear mascaras de filtrado.
    # 1.) Clientes no "nulos"
    mask_not_null_cliente = Filtrar_tabla.mask_filter_not_null_rows(
        column_name=config_filtros["Cod. Cliente"]["Columna"]
    )
    # 2.) Clientes != 0
    mask_cliente_not_zero = Filtrar_tabla.mask_no_equivalente_pa(
        columna=config_filtros["Cod. Cliente"]["Columna"],
        valor=str(config_filtros["Cod. Cliente"]["Valor"]),
    )

    # 3.) Filtrar activos necesarios.
    mask_denm_obj = Filtrar_tabla.Mascara_is_in_pa(
        columna=config_filtros["Denominación objeto"]["Columna"],
        valores=list(dict_act_indir.values()),
    )

    # Combinar_mascaras
    mask_completa = Pyarrow_Functions.PyArrowTablefilter.Combinar_mask_and_pa(
        mask_cliente_not_zero, mask_denm_obj, mask_not_null_cliente
    )

    # Filtrar_Tabla
    maestra_inactivos_copy_filter = Filtrar_tabla.Filtrar_tabla_pa(mask=mask_completa)

    # Imputamos los valores nulos de la fecha de suministro con la fecha de creación.
    cols_reemplazar_nulos = config["Insumos"]["Maestra_Activos_Indirecta"][
        "cols_mod_nulos"
    ]

    maestra_act_indir_mod_cols = (
        Pyarrow_Functions.LLenar_valores_nulos_con_otra_columna_pa(
            tabla=maestra_inactivos_copy_filter,
            col_fuente=cols_reemplazar_nulos["Fecha"]["col_fuente"],
            col_destino=cols_reemplazar_nulos["Fecha"]["col_destino"],
        )
    )

    cols_constantes_M_act_indir = config["Insumos"]["Maestra_Activos_Indirecta"][
        "cols_constantes"
    ]

    # Agregar ambas columnas constante comas.
    # next(iter(dict)) : => Recibe un diccionario de una clave y me devuelve la primera llave de un diccionario
    for cada_columna, cada_valor in cols_constantes_M_act_indir.items():

        col_nueva = Pyarrow_Functions.crear_columna_constante_pa(
            num_filas=maestra_act_indir_mod_cols.num_rows,
            valor_constante=cada_valor[0],
            tipo_dato=cada_valor[1],
        )
        maestra_act_indir_mod_cols = Pyarrow_Functions.agregar_nueva_columna_pa(
            tabla=maestra_act_indir_mod_cols,
            array_resultado=col_nueva,
            nombre_nueva_columna=cada_columna,
        )

    # Traigamos los activos y las campañas a la tabla de valores.
    maestra_act_indir_mod_cols = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_act_indir_mod_cols,
        table_right=table_drv_top_camps,
        join_key="Denominación objeto",
    )

    # Traemos neveras en mantenimiento y garantia.
    maestra_act_indir_mod_cols = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_act_indir_mod_cols,
        table_right=table_drv_manto_neve,
        join_key=N_INVENTAR,
    )

    maestra_act_indir_mod_cols = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_act_indir_mod_cols,
        table_right=table_drv_nev_grtia,
        join_key=N_INVENTAR,
    )

    # Agregamos la información de las campañas.
    maestra_act_indir_mod_cols = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_act_indir_mod_cols,
        table_right=table_drv_ac_estra,
        join_key=N_INVENTAR,
    )

    maestra_act_indir_mod_cols2 = maestra_act_indir_mod_cols.to_pandas()
    
    maestra_act_indir_mod_cols2 = Pandas_Functions.Reemplazar_valores_con_dict_pd(
        df=maestra_act_indir_mod_cols2,
        columna="Tipo Activo",
        diccionario_mapeo={
            "Neveras": "Neverízate",
            "Snackeros": "Snackermanía",
            "Puestos de Pago": "Puestos de Pago MM",
        }
    )
    maestra_act_indir_mod_cols_pd = Pandas_Functions.Transform_dfs_pandas_a_pyarrow(df=maestra_act_indir_mod_cols2)
    
    array_concat = (
        Pyarrow_Functions.TableColumnConcatenator.concatenar_cols_seleccionadas(
            table=maestra_act_indir_mod_cols_pd,
            column_names=["r_id_agente_comercial", "Cod. Cliente", "Tipo Activo"],
        )
    )
    maestra_inactivos_copy_filter_cli = Pyarrow_Functions.agregar_nueva_columna_pa(
        tabla=maestra_act_indir_mod_cols_pd,
        array_resultado=array_concat,
        nombre_nueva_columna="Cliente / Estrategia",
    )
    maestra_inactivos_indiv_pd = maestra_inactivos_copy_filter_cli.to_pandas()

    maestra_inactivos_indiv_pd_select = maestra_inactivos_indiv_pd[
        ["Cliente / Estrategia", "Denominación objeto", "NºInventar"]
    ]

    columnas_a_concatenar = config["Insumos"]["Maestra_Activos_Indirecta"][
        "cols_concat"
    ]

    for cada_col, columna_concat in columnas_a_concatenar.items():
        col_concat = (
            Pyarrow_Functions.TableColumnConcatenator.concatenar_cols_seleccionadas(
                table=maestra_act_indir_mod_cols,
                column_names=[cada_col, columna_concat],
            )
        )
        # Eliminar columna anterior a sustituir.
        maestra_act_indir_mod_cols = Pyarrow_Functions.Eliminar_columnas_pa(
            tabla=maestra_act_indir_mod_cols, columnas_a_eliminar=cada_col
        )

        # Agregar concatenada con el nombre original en conlumnas_a_concatenar.keys()
        maestra_act_indir_mod_cols = Pyarrow_Functions.agregar_nueva_columna_pa(
            tabla=maestra_act_indir_mod_cols,
            array_resultado=col_concat,
            nombre_nueva_columna=cada_col,
        )

    # Eliminar y sustituir columna cliente.
    maestra_act_indir_mod_cols = Pyarrow_Functions.Eliminar_columnas_pa(
        tabla=maestra_act_indir_mod_cols,
        columnas_a_eliminar=config["Insumos"]["Maestra_Activos_Indirecta"][
            "cols_eliminar"
        ],
    )

    # Renombrar columna "r_id_agente_comercial" como "Cliente"
    maestra_act_indir_mod_cols = Pyarrow_Functions.Renombrar_cols_con_dict_pa(
        tabla=maestra_act_indir_mod_cols,
        dict_renombrar={"r_id_agente_comercial": "Cliente"},
    )

    cols_agrup = config["Insumos"]["Maestra_Activos_SAP"]["cols_agrup_concat"]
    cols_estrategias = list(config["estrategias_semaforo"].keys())
    cols_agrup_completas = cols_agrup + cols_estrategias

    maestra_act_indir_select = Pyarrow_Functions.Seleccionar_columnas_pa(
        tabla=maestra_act_indir_mod_cols,
        columnas=cols_agrup_completas,
    )

    maestra_concatenada_agrupada = Pyarrow_Functions.Group_by_pa_whit_pd(
        tabla=maestra_act_indir_select,
        group_col=cols_agrup_completas[0:3],
        sum_cols=cols_agrup_completas[3:],
    )

    return (
        maestra_concatenada_agrupada.to_pandas().drop_duplicates(),
        maestra_inactivos_indiv_pd_select,
    )
