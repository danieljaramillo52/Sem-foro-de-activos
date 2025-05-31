import copy
from typing import Type
import pyarrow as pa
from datetime import datetime


def Transformaciones_ma_act_directa(
    maestra_activos_sap: pa.Table,
    maestra_act_sap_compta: pa.Table,
    Pyarrow_Functions: Type,
    Pandas_Functions: Type,
    config: dict,
    drivers: list,
) -> pa.Table:
    """Encapsula el procedimiento para modificar la maestra_activos_sap y retorna la maestra_activos_sap modificada.
    Modula el proceso de modificación de la maestra_actitvos_sap para evitar aplicarlo directamente en el módulo main.py.

    Args:
        maestra_activos_sap (pa.Table): Tabla bidimensional de PyArrow que contiene la información original del archivo en: config['Insumos']['maestra_activos_sap']['nom_base']"

        Pyarrow_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto maestra_actitvos_sap. Localizada en: "Utils/Trasformation_functions/PyArrowColumnTransformer"

        Pandas_Functions (pa.Type): Clase que contiene métodos necesarios para modificar el objeto maestra_actitvos_sap. Localizada en: "Utils/Trasformation_functions/PandasBase_Transformer"

        config (dict): Diccionario extraido de config.yml. Del mismo se extraen los conjuntos de información necesarios.

        drivers (List[Dataframes]): Lista de drivers de información, ahora procesados como tablas de pyarrow, necesarios para el proceso de maestra_activos_sap

    Returns:
        maestra_act_def_sap (pa.Table) : maestra_actitvos_sap modificada.
    """

    """ Tener en cuenta que se toma parte de la configuración para esta ejecución: """

    # Definimos constantes
    N_INVENTAR = config["Drivers"]["Nev_mantto"]["cols_duplicar"]["CÓDIGO DE BARRAS"]# Constantes de nombres de columnas
    COL_COD_BARRAS = "Cód. Barras"
    COL_NOMBRE_ACTIVO = "Nombre del Activo"
    COL_MODELO = "Modelo"
    COL_REGIONAL = "Regional"
    COL_COD_AGENTE = "Cód. Agente Comercial"
    COL_NOMBRE_AGENTE = "Nombre Agente Comercial"

    # Valor por defecto para campos faltantes
    VALOR_POR_DEFECTO = "-"


    # Exraemos los drivers:
    df_drv_estrategias = drivers[0]
    df_drv_top_camps = drivers[1]
    df_drv_manto_neve = drivers[2]

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

    table_drv_top_camps = Pyarrow_Functions.cambiar_tipo_dato_columnas_pa(
        tabla=table_drv_top_camps,
        columnas=config["Drivers"]["cols_necesarias"]["Activos y Cargues"][
            "cols_cambiar_tipo_dato"
        ],
        nuevo_tipo=pa.float64(),
    )

    # Creamos una copia profunda de la base original para trabajar con ella.
    maestra_activos_sap_copy = copy.deepcopy(maestra_activos_sap)

    maestra_activos_sin_cliente = copy.deepcopy(maestra_act_sap_compta)

    """ Filtrar información requerida de la maestra de activos. """

    # Instancia de la clase.
    Filtrar_tabla = Pyarrow_Functions.PyArrowTablefilter(maestra_activos_sap_copy)
    Filtrar_tabla2 = Pyarrow_Functions.PyArrowTablefilter(maestra_activos_sin_cliente)

    config_filtrado = config["Insumos"]["Maestra_Activos_SAP"]["Filtros"]

    # Valores unicos Denominación objeto
    unicos_col_dn_objt = list(
        df_drv_top_camps[
            config["Drivers"]["cols_necesarias"]["driver_topes_campañas"][
                "NOMBRE SAP ORIGINAL"
            ]
        ]
    )

    # Creación de las mascaras de filtrado.
    mask_cliente = Filtrar_tabla.mask_filter_rows_starting_with(
        columna=config_filtrado["Cliente"]["Columna"],
        start_string=config_filtrado["Cliente"]["Valor"],
    )
    mask_sin_cliente = Filtrar_tabla2.mask_filter_null_rows(
        column_name=config_filtrado["Cliente"]["Columna"],
    )
    mask_activos = Filtrar_tabla.mask_filter_filas_sin_letras(
        columna=config_filtrado["NºInventar"]["Columna"],
    )
    mask_status = Filtrar_tabla.Mascara_is_in_pa(
        columna=config_filtrado["StatUsu"]["Columna"],
        valores=config_filtrado["StatUsu"]["Valor"],
    )
    mask_status2 = Filtrar_tabla2.Mascara_is_in_pa(
        columna="Status",
        valores=config_filtrado["StatUsu"]["Valor"],
    )
    mask_status_invertida = Filtrar_tabla.Invertir_mascara_pa(mask=mask_status)
    mask_status_invertida2 = Filtrar_tabla2.Invertir_mascara_pa(mask=mask_status2)

    mask_modelo = Filtrar_tabla.mask_equivalente_pa(
        columna=config_filtrado["Modelo"]["Columna"],
        valor=config_filtrado["Modelo"]["Valor"],
    )
    mask_denominacion_objeto = Filtrar_tabla.Mascara_is_in_pa(
        columna=config_filtrado["Denominación objeto"]["Columna"],
        valores=unicos_col_dn_objt,
    )
    mask_denominacion_objeto2 = Filtrar_tabla2.Mascara_is_in_pa(
        columna="Nombre del Activo",
        valores=unicos_col_dn_objt,
    )

    mask_completa_sin_act = Pyarrow_Functions.PyArrowTablefilter.Combinar_mask_and_pa(
        mask_sin_cliente, mask_denominacion_objeto2, mask_status_invertida2
    )

    maestra_activos_sin_cliente = Filtrar_tabla2.Filtrar_tabla_pa(mask_completa_sin_act)

    # Combinar mascaras de filtrado.
    mask_completa = Pyarrow_Functions.PyArrowTablefilter.Combinar_mask_and_pa(
        mask_cliente,
        mask_modelo,
        mask_status_invertida,
        mask_activos,
        mask_denominacion_objeto,
    )

    # Tabla filtrada.
    maestra_activos_sap_filtrada = Filtrar_tabla.Filtrar_tabla_pa(mask_completa)

    cols_constantes_M_act_sap = config["Insumos"]["Maestra_Activos_SAP"][
        "cols_constantes"
    ]
    cols_reemplazar_nulos = config["Insumos"]["Maestra_Activos_SAP"]["cols_mod_nulos"]
    # Homologar fechas nulas en "Fecha suministro" con los valores en "Fechas de creación".

    # Rellenar para llenar los valores nulos en la columna destino con los valores de la columna fuente
    maestra_activos_sap_filtrada = (
        Pyarrow_Functions.LLenar_valores_nulos_con_otra_columna_pa(
            tabla=maestra_activos_sap_filtrada,
            col_fuente=cols_reemplazar_nulos["Fecha"]["col_fuente"],
            col_destino=cols_reemplazar_nulos["Fecha"]["col_destino"],
        )
    )
    # Agregar ambas columnas constante comas.
    # next(iter(dict)) : => Recibe un diccionario de una clave y me devuelve la primera llave de un diccionario
    for cada_columna, cada_valor in cols_constantes_M_act_sap.items():

        col_nueva = Pyarrow_Functions.crear_columna_constante_pa(
            num_filas=maestra_activos_sap_filtrada.num_rows,
            valor_constante=cada_valor[0],
            tipo_dato=cada_valor[1],
        )

        maestra_activos_sap_filtrada = Pyarrow_Functions.agregar_nueva_columna_pa(
            tabla=maestra_activos_sap_filtrada,
            array_resultado=col_nueva,
            nombre_nueva_columna=cada_columna,
        )

    # Traigamos los activos y las campañas a la tabla de valores.
    maestra_activos_sap_filtrada = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_activos_sap_filtrada,
        table_right=table_drv_top_camps,
        join_key="Denominación objeto",
    )

    # Traemos neveras en mantenimiento y garantia.
    maestra_activos_sap_filtrada = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_activos_sap_filtrada,
        table_right=table_drv_manto_neve,
        join_key=N_INVENTAR,
    )

    # Agregamos la información de las campañas.
    maestra_activos_sap_filtrada = Pyarrow_Functions.Join_combine_pyarrow(
        table_left=maestra_activos_sap_filtrada,
        table_right=table_drv_ac_estra,
        join_key=N_INVENTAR,
    )

    # Concatenar columna nueva (col_comas) con la columna "NºInventar" (Contiene los codigos de los activos)
    # Generar la columnas concatenadas.
    columnas_a_concatenar = config["Insumos"]["Maestra_Activos_SAP"]["cols_concat"]

    maestra_activos_indiv_pd = maestra_activos_sap_filtrada.to_pandas()

    maestra_activos_indiv_pd_replace = Pandas_Functions.Reemplazar_valores_con_dict_pd(
        df=maestra_activos_indiv_pd,
        columna="Tipo Activo",
        diccionario_mapeo={
            "Neveras": "Neverízate",
            "Snackeros": "Snackermanía",
            "Puestos de Pago": "Puestos de Pago MM",
        },
    )
    maestra_activos_indiv_pd_concat = Pandas_Functions.concatenar_columnas_pd(
        dataframe=maestra_activos_indiv_pd_replace,
        cols_elegidas=["Cliente", "Tipo Activo"],
        nueva_columna="Cliente / Estrategia",
    )

    maestra_activos_indiv_pd_select = maestra_activos_indiv_pd_concat[
        ["Cliente / Estrategia", "Denominación objeto", "NºInventar"]
    ]
    for cada_col, columna_concat in columnas_a_concatenar.items():
        col_concat = (
            Pyarrow_Functions.TableColumnConcatenator.concatenar_cols_seleccionadas(
                table=maestra_activos_sap_filtrada,
                column_names=[cada_col, columna_concat],
            )
        )

        # Eliminar columna anterior a sustituir.
        maestra_activos_sap_filtrada = Pyarrow_Functions.Eliminar_columnas_pa(
            tabla=maestra_activos_sap_filtrada, columnas_a_eliminar=cada_col
        )

        # Agregar concatenada con el nombre original en conlumnas_a_concatenar.keys()
        maestra_activos_sap_filtrada = Pyarrow_Functions.agregar_nueva_columna_pa(
            tabla=maestra_activos_sap_filtrada,
            array_resultado=col_concat,
            nombre_nueva_columna=cada_col,
        )

    # Agrupar por Cliente, para unificar las columnas anteriores. por registro.

    """Tomar un subconjutno de información para "Agrupar" ~ "Concatenar" los codigos de activos, las fechas de suministro y el nombre de activos. por cada cliente."""

    cols_agrup = config["Insumos"]["Maestra_Activos_SAP"]["cols_agrup_concat"]
    cols_estrategias = list(config["estrategias_semaforo"].keys())
    cols_agrup_completas = cols_agrup + cols_estrategias

    maestra_activos_sap_select = Pyarrow_Functions.Seleccionar_columnas_pa(
        tabla=maestra_activos_sap_filtrada,
        columnas=cols_agrup_completas,
    )

    maestra_concatenada_agrupada = Pyarrow_Functions.Group_by_pa_whit_pd(
        tabla=maestra_activos_sap_select,
        group_col=cols_agrup_completas[0:3],
        sum_cols=cols_agrup_completas[3:],
    )
    maestra_activos_sin_cliente_pd = maestra_activos_sin_cliente.to_pandas()

    maestra_activos_sin_cliente_pd_select = maestra_activos_sin_cliente_pd[
        [COL_COD_BARRAS, COL_NOMBRE_ACTIVO, COL_MODELO, COL_REGIONAL]
    ]

    maestra_activos_sin_cliente_pd_select.loc[:,COL_COD_AGENTE] = VALOR_POR_DEFECTO
    maestra_activos_sin_cliente_pd_select.loc[:,COL_NOMBRE_AGENTE] = VALOR_POR_DEFECTO


    return (
        maestra_concatenada_agrupada.to_pandas().drop_duplicates(),
        maestra_activos_indiv_pd_select, maestra_activos_sin_cliente_pd_select
    )
