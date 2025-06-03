# Archivo main del proceso.
# Importamos librerias preliminares.
import sys
import os
import numpy as np
import pandas as pd


# Configuramos la rutas para traer modulos de las funciones.
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

"""Agregamos al path las direcciones de los directorios "Utils => funciones del proyecto, Scrits => Modulos del proyecto )"""
sys.path.extend([f"{parent_dir}\\Utils", f"{parent_dir}\\Scripts"])


"""Ubicamos la ruta de ejecución."""
lugar_de_ejecucion = input(
    "Está ejecutando esta automatización desde Python IDLE ó desde cmd?: (si/no): "
)

if lugar_de_ejecucion == "si":
    ruta_actual = os.getcwd()
    ruta_padre = os.path.dirname(ruta_actual)
    os.chdir(ruta_padre)
else:
    pass

# Importamos todos los modulos necesarios
from datetime import datetime
from loguru import logger
import Transformation_Functions as TF
import General_Functions as GF
import Lectura_bases_drivers_excel as LBDE
import Transformaciones_UNVSOS as TUD
import Transformaciones_Ma_Act_sap as TMAD
import Transformaciones_Ma_Indir as TMID
import Transformaciones_drivers as TDV
import Transformaciones_Vtas as TVTAS
import Proceso_Semaforos as PS
import Proceso_camapañas_final as PCF
from Procesador_omitidos import aplicar_logica_omitidos


Inicio = datetime.now()
# Ejecucuión del proceso.

""" 1.) Carga archivo config.yml para arbitrar el proyecto, definición de constantes"""
config = GF.Procesar_configuracion(nom_archivo_configuracion="config.yml")
dict_drivers = config["Drivers"]
dict_estados_meses = GF.Lectura_simple_excel(
    path=dict_drivers["path_drivers"],
    nom_insumo=dict_drivers["Estado_acumulado"]["nom_base"],
)
list_meses = [*dict_drivers["Estado_acumulado"]["nom_hojas"]]
list_series_cli_est = [
    dict_estados_meses[cada_mes]["Concatenar Cliente y Estrategia"]
    for cada_mes in list_meses
]
""" 2.) Lectura de archivos.  """
# Instanciar modulo de lectura y cargue de archivos del proyecto

# Lectura de archivos en directorio "Insumos".
(
    universo_directa_select,
    universo_indirecta_select,
    maestra_activos_sap,
    maestra_activos_indirecta,
    driver_ac_estra,
    driver_ac_carg,
    driver_manto_neveras,
    driver_activos_a_omitir,
    driver_topes_acum,
    driver_region,
    driver_cli_atrb,
    driver_jefes_agentes,
    driver_ldcr,
    driver_venta_cero,
    driver_top_rojo,
    vts_neveras_select,
    vts_snkros_pp_select,
    maestra_clientes_inac_indir,
    maestra_clientes_inac_dir,
    maestra_activos_sap_reg,
    maestra_activos_indirecta,
    concat_ser_cli_est,
) = LBDE.Lectura_archivos(
    funcion_lectura=GF.Lectura_insumos_excel,
    funcion_select_cols=TF.PandasBaseTransformer.Seleccionar_columnas_pd,
    dict_insumos=config["Insumos"],
    dict_drivers=config["Drivers"],
)

maestra_activos_sap_select = maestra_activos_sap[
    config["Insumos"]["Maestra_Activos_SAP"]["cols_necesarias"]
]

# Maestra inactivos indirecta
maestra_activos_indir_select = maestra_activos_indirecta[
    config["Insumos"]["Maestra_Activos_Indirecta"]["cols_necesarias"]
]

nuevas_cols_ma_act_sap = config["Insumos"]["Maestra_Activos_SAP"][
    "cols_nuevas_necesarias"
]

# Modificacion formato de las fechas.
col_fecha_sap = config["Insumos"]["Maestra_Activos_SAP"]["cols_mod_fecha"]
col_fecha_indir = config["Insumos"]["Maestra_Activos_Indirecta"]["cols_mod_fecha"]

maestra_activos_sap_select = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=maestra_activos_sap_select, cols_to_rename=nuevas_cols_ma_act_sap
    )
)

maestra_activos_sap_select[col_fecha_sap] = maestra_activos_sap_select[
    col_fecha_sap
].apply(lambda x: x.str.split().str[0] if x.dtypes == object else x)

maestra_activos_indir_select[col_fecha_indir] = maestra_activos_indir_select[
    col_fecha_indir
].apply(lambda x: x.str.split().str[0] if x.dtypes == object else x)

# Transformar Universos y Mestras a tablas de pyarrow.
table_mtra_act_sap = TF.PandasBaseTransformer.Transform_dfs_pandas_a_pyarrow(
    df=TF.PandasBaseTransformer.Remove_duplicates(df=maestra_activos_sap_select)
)
table_mtra_indir_sap = TF.PandasBaseTransformer.Transform_dfs_pandas_a_pyarrow(
    df=TF.PandasBaseTransformer.Remove_duplicates(df=maestra_activos_indir_select)
)
# Transformar Universos y Mestras a tablas de pyarrow.
table_mtra_act_sap_compta = TF.PandasBaseTransformer.Transform_dfs_pandas_a_pyarrow(
    df=TF.PandasBaseTransformer.Remove_duplicates(df=maestra_activos_sap)
)
table_mtra_indir_compta = TF.PandasBaseTransformer.Transform_dfs_pandas_a_pyarrow(
    df=TF.PandasBaseTransformer.Remove_duplicates(df=maestra_activos_indirecta)
)

# Definir los diccionarios de campañas estrategias.


drv_top_camps, dict_act_indir, drv_estrategias, drv_manto_neve = (  # drv_nev_grtia = (
    TDV.Transformaciones_drivers(
        drivers=[
            driver_ac_estra,
            driver_ac_carg,
            driver_manto_neveras,
        ],
        Pandas_Functions=TF.PandasBaseTransformer,
        config=config,
    )
)

# Trasformaciones adicionales universo de la directa.
universo_de_clientes = TUD.Trasformaciones_universos(
    universo_directa=universo_directa_select,
    universo_indirecta=universo_indirecta_select,
    Pyarrow_Functions=TF.PyArrowColumnTransformer,
    Pandas_Functions=TF.PandasBaseTransformer,
    config=config,
)

# Proceso maestras de activos.

maestra_act_sap_def, maestra_activos_indiv_pd_select, maestra_activos_sin_cliente = (
    TMAD.Transformaciones_ma_act_directa(
        maestra_activos_sap=table_mtra_act_sap,
        maestra_act_sap_compta = table_mtra_act_sap_compta,
        Pyarrow_Functions=TF.PyArrowColumnTransformer,
        Pandas_Functions=TF.PandasBaseTransformer,
        config=config,
        drivers=[drv_estrategias, drv_top_camps, drv_manto_neve],
    )
)

# Proceso maestras de activos.
maestra_act_indir_def, maestra_inactivos_indiv_pd_select, maestra_inactivos_sin_cliente = (
    TMID.Transformaciones_ma_act_indirecta(
        maestra_activos_indirecta=table_mtra_indir_sap,
        maestra_act_indir_compta = table_mtra_indir_compta,
        Pyarrow_Functions=TF.PyArrowColumnTransformer,
        Pandas_Functions=TF.PandasBaseTransformer,
        config=config,
        drivers=[
            drv_estrategias,
            drv_top_camps,
            drv_manto_neve,
            dict_act_indir,
        ],
    )
)
base_lista_activos = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[maestra_inactivos_indiv_pd_select, maestra_activos_indiv_pd_select]
)

base_lista_activos_sin_cliente = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[maestra_activos_sin_cliente, maestra_inactivos_sin_cliente]
)

# Vamos a agregar las correspondientes ventas a las maestras.
maestra_activos_completa = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[maestra_act_sap_def, maestra_act_indir_def]
)

# Maestra activos finales.

# Merge entre las bases y los universos para traer la información correspondiente.
base_activos_clientes = TF.PandasBaseTransformer.pd_left_merge(
    base_left=maestra_activos_completa,
    base_right=universo_de_clientes,
    key=config["dict_constantes"]["Cliente"],
)

# Proceso ventas.
vtas_completa = TVTAS.Transformaciones_Ventas(
    ventas_snak_pp=vts_snkros_pp_select,
    ventas_neveras=vts_neveras_select,
    config=config,
    Pandas_Functions=TF.PandasBaseTransformer,
)

config_vtas = config["Insumos"]["Ventas_Muebles_PPago_Neve"]

vtas = config_vtas["cols_constantes"]
# Generar llave para traer vtas.
base_activos_clientes_def = TF.PandasBaseTransformer.concatenar_columnas_pd(
    dataframe=base_activos_clientes,
    cols_elegidas=["Cliente", "Tipo Activo"],
    nueva_columna=config_vtas["cols_concat"]["Directa"]["Nombre"],
)
# Merge con la base principal para traer las vtas del mes actual.
base_activos_clientes_vtas = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_activos_clientes_def,
    base_right=vtas_completa,
    key=config_vtas["cols_concat"]["Directa"]["Nombre"],
)

# Rellenar filas nulas de Vtas.
base_activos_clientes_vtas = (
    TF.PandasBaseTransformer.Remplazar_nulos_multiples_columnas_pd(
        base=base_activos_clientes_vtas,
        list_columns=config["dict_constantes"]["Venta $"],
        value="0",
    )
)

# Ingreso meses.
while True:
    try:
        # Solicitar al usuario que ingrese la abreviatura del mes actual
        ACTUAL = str(
            input(
                "Ingrese por favor el mes actual de ventas: Ingreselo de acuerdo a las abreviaturas a continuación: ENE, FEB, MAR, ABR, MAY, JUN, JUL, AGO, SEP, OCT, NOV, DIC. Ejemplo: si su mes actual es enero ingrese las siglas: ENE , si es Agosto ingrese las siglas: AGO: "
            )
        )

        # Verificar si la entrada del usuario está en la tupla de meses válidos
        if ACTUAL in config["list_meses"]:
            break  # Salir del bucle si la entrada es válida
        else:
            logger.info(
                "Entrada no válida. Por favor, ingrese una abreviatura de mes válida."
            )
    except Exception as e:
        logger.critical(f"Error: {e}")

lista_meses = config["list_meses"]

# Obtenemos el indice del mes actual en la lista.
indice_mes_act = lista_meses.index(ACTUAL)

# Filtramos la lista a partir del indice
list_meses_filtrada = lista_meses[indice_mes_act + 1 :]

# Renombrar columna vtas actual.
clave_venta = config["dict_constantes"]["Venta $"]
nueva_clave_venta = f"{clave_venta} {ACTUAL}"

base_activos_clientes_vtas = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=base_activos_clientes_vtas, cols_to_rename={clave_venta: nueva_clave_venta}
    )
)

# Generación de Historico si el mes actual es Enero , o se trata de otros meses.

# Preparación inicial común
clave_inicial = config["dict_constantes"]["key_vtas"]
columnas_para_exportar = [clave_inicial, nueva_clave_venta]


if ACTUAL == "ENE":
    # Exportamos el dataframe del historico
    Historico_para_exportar = base_activos_clientes_vtas[
        columnas_para_exportar
    ].drop_duplicates()
else:
    # Procesamiento para meses distintos de enero
    Historico = GF.Lectura_simple_excel(
        path=config["path_Resultados"],
        nom_insumo=config["Insumos"]["Historico_Vtas"]["nom_base"],
        nom_hoja=config["Insumos"]["Historico_Vtas"]["nom_hoja"],
    ).drop_duplicates()

    GF.exportar_a_excel(
        ruta_guardado=config["path_Resultados"],
        df=Historico,
        nom_hoja="Historico_copia",
    )

    if nueva_clave_venta in list(Historico.columns):
        Historico = Historico.drop(columns=nueva_clave_venta)
    # Merge histórico y base actual, asumiendo TF.PandasBaseTransformer ya tiene este método implementado
    base_activos_clientes_vtas = TF.PandasBaseTransformer.pd_left_merge(
        base_left=base_activos_clientes_vtas,
        base_right=Historico,
        key=clave_inicial,
    )

    # Prepara el DataFrame actualizado para la exportación
    columnas_para_exportar = [
        col
        for col in base_activos_clientes_vtas.columns
        if clave_inicial == col or config["dict_constantes"]["Venta $"] in col
    ]

    Historico_para_exportar = base_activos_clientes_vtas[columnas_para_exportar]

# Exportación final a Excel, común para ambos casos
GF.exportar_a_excel(
    ruta_guardado=config["path_Resultados"],
    df=Historico_para_exportar,
    nom_hoja=config["Insumos"]["Historico_Vtas"]["nom_hoja"],
)


base_semaforos_vtas = PS.Proceso_semaforo_activos(
    base_insumo_semaforos=base_activos_clientes_vtas,
    driver_topes_acum=driver_topes_acum,
    concat_ser_cli_est=concat_ser_cli_est,
    config=config,
    mes_actual=ACTUAL,
)

# Proceso adicional con las campañas de activos.
base_semaforos_vtas_campañas = PCF.Proceso_campañas_final(
    base_semaforos_vtas=base_semaforos_vtas,
    config=config,
    Pandas_Functions=TF.PandasBaseTransformer,
    General_Functions=GF,
)

# Agregar Informacion adicional a los clientes inactivos.
# Separar tabla en nulos y no nulos en clientes inactivos.
# Base no nulos.

base_semf_vtas_camp_not_null = base_semaforos_vtas_campañas.loc[
    base_semaforos_vtas_campañas["Cliente Inactivo"].notnull()
]

# Base nulos
base_semf_vtas_camp_is_null = base_semaforos_vtas_campañas.loc[
    base_semaforos_vtas_campañas["Cliente Inactivo"].isnull()
]

# Filtrar base de no nulos por directa e indirecta
base_sem_vtas_cli_inac_dir = TF.PandasBaseTransformer.Filtrar_por_valores_pd(
    df=base_semf_vtas_camp_not_null,
    columna=config["dict_constantes"]["MODELO"],
    valores_filtrar=["Directa"],
)

base_sem_vtas_cli_inac_indir = TF.PandasBaseTransformer.Filtrar_por_valores_pd(
    df=base_semf_vtas_camp_not_null,
    columna=config["dict_constantes"]["MODELO"],
    valores_filtrar=["Indirecta"],
)

# Tomemos eliminemos las columnas a actualizar en las bases "base_sem_vtas_cli_inac_dir" y "base_sem_vtas_cli_inac_indir"
cols_eliminar = list(
    config["Insumos"]["Universos_directa_indirecta"]["renombrar_cols"].values()
)[:-1]

# Eliminar columnas.
base_sem_vtas_cli_inac_dir_del = TF.PandasBaseTransformer.Eliminar_columnas(
    df=base_sem_vtas_cli_inac_dir, columnas_a_eliminar=cols_eliminar
)
base_sem_vtas_cli_inac_indir_del = TF.PandasBaseTransformer.Eliminar_columnas(
    df=base_sem_vtas_cli_inac_indir, columnas_a_eliminar=cols_eliminar
)

# Renombrar cols maestras clientes inactivos.
maestra_clientes_inac_dir = TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
    base=maestra_clientes_inac_dir,
    cols_to_rename=config["Insumos"]["maestra_clientes_dir_indir"]["renombrar_cols"],
)
maestra_clientes_inac_indir = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=maestra_clientes_inac_indir,
        cols_to_rename=config["Insumos"]["maestra_clientes_dir_indir"][
            "renombrar_cols"
        ],
    )
)
# Extraer los clientes inactivos

# Merge para ambas bases
base_sem_vtas_cli_inac_dir_def = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_sem_vtas_cli_inac_dir_del,
    base_right=maestra_clientes_inac_dir,
    key=config["dict_constantes"]["Cliente"],
)

base_sem_vtas_cli_inac_indir_def = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_sem_vtas_cli_inac_indir_del,
    base_right=maestra_clientes_inac_indir,
    key=config["dict_constantes"]["Cliente"],
)

# Concatenar ambas bases.
base_clientes_inac_completa = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[base_sem_vtas_cli_inac_dir_def, base_sem_vtas_cli_inac_indir_def]
)

# Reordenar la base que contiene los clientes inactivos nulos , y la base de los clientes inactivos no nulos para unificar toda la base completa

# Reordenar columnas finales.
dict_final_cols = config["base_final"]["orden_final_cols"]


base_semf_vtas_camp_is_null = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=base_semf_vtas_camp_is_null, cols_to_rename=dict_final_cols
    )
)

base_clientes_inac_completa = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=base_clientes_inac_completa, cols_to_rename=dict_final_cols
    )
)

# Ordenar ambos resultados
base_semf_vtas_camp_is_null = base_semf_vtas_camp_is_null[
    list(dict_final_cols.values())
]
base_clientes_inac_completa = base_clientes_inac_completa[
    list(dict_final_cols.values())
]

# Concatenar base final.
base_final_semaforos = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[base_semf_vtas_camp_is_null, base_clientes_inac_completa]
)

dict_reemplazos_finales = config["dict_reemplazos_finales"]

# Reemplazar los valores de la directa
for cada_columna, cada_valor in dict_reemplazos_finales.items():
    base_final_semaforos = TF.PandasBaseTransformer.Reemplazar_valores_con_dict_pd(
        df=base_final_semaforos,
        columna=dict_reemplazos_finales[cada_columna]["col"],
        diccionario_mapeo=dict_reemplazos_finales[cada_columna]["dict_reemplazos"],
    )

# Cambiar el tipo de dato de la columna Cargue.
base_final_semaforos = TF.PandasBaseTransformer.Cambiar_tipo_dato_multiples_columnas_pd(
    base=base_final_semaforos,
    list_columns=[
        config["dict_constantes"]["Cargue"],
        config["dict_constantes"]["PENDIENTE DE META"],
        config["dict_constantes"]["PENDIENTE DE VENTA"],
        dict_final_cols["N.de Activos"],
    ],
    type_data=int,
)

base_final_semaforos_dir = base_final_semaforos[
    base_final_semaforos["MODELO"] == "Directa"
]
base_final_semaforos_indir = base_final_semaforos[
    base_final_semaforos["MODELO"] == "Indirecta"
]

maestra_activos_sap_reg_fil = maestra_activos_sap_reg.dropna(
    subset=dict_final_cols["Cliente"], inplace=False
)
# Reemplazo solo en los valores nulos
base_final_semaforos_dir.loc[
    :, dict_final_cols["Oficina de Ventas"]
] = base_final_semaforos_dir[dict_final_cols["Oficina de Ventas"]].fillna(
    base_final_semaforos_dir[dict_final_cols["Cliente"]].map(
        TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
            df=maestra_activos_sap_reg,
            col_clave=dict_final_cols["Cliente"],
            col_valor="Regional",
        )
    )
)
maestra_activos_indirecta_concat = TF.PandasBaseTransformer.concatenar_columnas_pd(
    dataframe=maestra_activos_indirecta,
    cols_elegidas=["Cód. Agente Comercial", "Cód. Cliente"],
    nueva_columna=dict_final_cols["Cliente"],
)
maestra_activos_indirecta_concat_fil = maestra_activos_indirecta_concat.dropna(
    subset=dict_final_cols["Cliente"], inplace=False
)

base_final_semaforos_indir.loc[
    :, dict_final_cols["Oficina de Ventas"]
] = base_final_semaforos_indir[dict_final_cols["Oficina de Ventas"]].fillna(
    base_final_semaforos_indir[dict_final_cols["Cliente"]].map(
        TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
            df=maestra_activos_indirecta,
            col_clave=dict_final_cols["Cliente"],
            col_valor=dict_final_cols["Regional"],
        )
    )
)

maestra_inactivos_concat_fil_rename = (
    TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
        base=maestra_activos_indirecta_concat_fil,
        cols_to_rename=config["base_final"]["ajustar_cols_remp_indir"],
    )
)
for cada_columna in list(config["base_final"]["ajustar_cols_remp_indir"].values()):
    dict_reemplzs_cols = TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
        df=maestra_inactivos_concat_fil_rename,
        col_clave=dict_final_cols["Cliente"],
        col_valor=cada_columna,
    )

    base_final_semaforos_indir.loc[:, cada_columna] = base_final_semaforos_indir[
        cada_columna
    ].fillna(
        base_final_semaforos_indir[dict_final_cols["Cliente"]].map(dict_reemplzs_cols)
    )


dict_remp_reg1 = TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
    df=driver_region,
    col_clave=dict_final_cols["Oficina de Ventas"],
    col_valor=dict_final_cols["Región"],
)

dict_remp_reg2 = TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
    df=driver_region,
    col_clave=dict_final_cols["Oficina de Ventas"],
    col_valor="Cód. Oficina de Ventas",
)

dict_remp_reg3 = TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
    df=driver_region,
    col_clave=dict_final_cols["Oficina de Ventas"],
    col_valor=dict_final_cols["Regional"],
)

# Crear los diccionarios de mapeo
diccionarios_reemplazo = {
    dict_final_cols["Región"]: dict_remp_reg1,
    dict_final_cols["Cód. OV"]: dict_remp_reg2,
    dict_final_cols["Regional"]: dict_remp_reg3,
}

# Reemplazar columnas para base_final_semaforos_dir
for col_destino, mapeo in diccionarios_reemplazo.items():
    base_final_semaforos_dir = (
        TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
            df=base_final_semaforos_dir,
            nom_columna_a_reemplazar=col_destino,
            nom_columna_de_referencia=dict_final_cols["Oficina de Ventas"],
            mapeo=mapeo,
        )
    )

# Reemplazar columnas para base_final_semaforos_indir
for col_destino, mapeo in diccionarios_reemplazo.items():
    base_final_semaforos_indir = (
        TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
            df=base_final_semaforos_indir,
            nom_columna_a_reemplazar=col_destino,
            nom_columna_de_referencia=dict_final_cols["Oficina de Ventas"],
            mapeo=mapeo,
        )
    )


for cada_columna, cada_valor in config["base_final"]["ajustar_cols_constantes"][
    "Directa"
].items():
    base_final_semaforos_dir = (
        TF.PandasBaseTransformer.Remplazar_nulos_multiples_columnas_pd(
            base=base_final_semaforos_dir, list_columns=[cada_columna], value=cada_valor
        )
    )

base_final_semaforos_dir.loc[:, "Cod Cliente"] = base_final_semaforos_dir["Cliente"]

for cada_columna, cada_valor in config["base_final"]["ajustar_cols_constantes"][
    "Indirecta"
].items():
    base_final_semaforos_indir = (
        TF.PandasBaseTransformer.Remplazar_nulos_multiples_columnas_pd(
            base=base_final_semaforos_indir,
            list_columns=[cada_columna],
            value=cada_valor,
        )
    )


base_final = TF.PandasBaseTransformer.concatenate_dataframes(
    dataframes=[base_final_semaforos_dir, base_final_semaforos_indir]
)
base_final.loc[:, "Cod Cliente"] = base_final["Cod Cliente"].fillna("-")

# Nuevas columnas adicionales. Producto de agregar columnas adicionales
dict_mod_cols = config["base_final"]["orden_modificado"]

# Ajustar columnas Descrip activo y Cod barras.
base_final = TF.PandasBaseTransformer.eliminar_last_caracteres(
    df=base_final,
    columnas=[
        dict_final_cols["Denominación objeto"],
        "Cod Barras",
    ],
)
base_final = TF.reemplazar_cero_por_si_no(
    df=base_final, columnas=[*config["estrategias_semaforo"]]
)

base_final = TF.marcar_cliente_inactivo(df=base_final)

base_final["Cliente Al Margen X 10000"] = np.where(
    (base_final["PENDIENTE DE VENTA"] > 0)
    & (base_final["PENDIENTE DE VENTA"] <= 10000),
    "x",
    "",
)

cols = (["Mantenimiento Nev"],)  # "Garantia Nev"]
for col in cols:
    base_final[col] = np.where(base_final[col] == "0", "", "x")

dict_reemplazos_cliente_indir = (
    TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
        df=maestra_activos_indirecta_concat,
        col_clave="Cliente",
        col_valor="Cód. Cliente",
    )
)

# Crear diccionario con df.
dict_cod_jefe_vtas = TF.PandasBaseTransformer.Crear_diccionario_desde_dataframe(
    df=driver_jefes_agentes,
    col_clave=dict_final_cols["Cód. JV"],
    col_valor=dict_final_cols["Jefe de Ventas"],
)

base_final = TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
    df=base_final,
    nom_columna_a_reemplazar=dict_final_cols["Cod Cliente"],
    nom_columna_de_referencia=dict_final_cols["Cliente"],
    mapeo=dict_reemplazos_cliente_indir,
)

base_final_merge = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final, base_right=driver_cli_atrb, key="Cod Cliente"
)

base_final_merge = TF.PandasBaseTransformer.Remplazar_nulos_multiples_columnas_pd(
    base=base_final_merge,
    list_columns=["Socios Nutresa", "Mercaderismo", "Vendedor AU"],
    value="No",
)


base_final_merge = TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
    df=base_final_merge,
    nom_columna_a_reemplazar="Jefe de Ventas",
    nom_columna_de_referencia="Cód. JV",
    mapeo=dict_cod_jefe_vtas,
)


base_final_merge.loc[:, "Estado Mes"] = base_final_merge[f"Estatus_Venta $ {ACTUAL}"]


"""Si un cliente ha vendido algo, se borra su marca de "Cliente Inactivo".
Si un cliente tiene ventas cero o negativas, se marca con "x" en "Venta Cero o Negativo Mes".

Si un cliente ya está inactivo, se borra cualquier marca en "Venta Cero o Negativo Mes", porque ya está considerado como inactivo.
"""

# Paso 1
base_final_merge["Cliente Inactivo"] = np.where(
    base_final_merge[f"Venta $ {ACTUAL}"] != 0, "", base_final_merge["Cliente Inactivo"]
)

# Paso 2
base_final_merge["Venta Cero o Negativo Mes"] = np.where(
    base_final_merge[f"Venta $ {ACTUAL}"] <= 0, "x", ""
)
# Paso 3
base_final_merge["Venta Cero o Negativo Mes"] = np.where(
    base_final_merge["Cliente Inactivo"] == "x",
    "",
    base_final_merge["Venta Cero o Negativo Mes"],
)

base_final_merge.loc[
    base_final_merge[config["dict_constantes"]["MODELO"]] == "Directa", "Cod Actual"
] = base_final_merge["Cod Cliente"]

base_final_merge = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final_merge,
    base_right=driver_ldcr,
    key=[dict_final_cols["Oficina de Ventas"], dict_final_cols["Canal Trans."]],
)


# Controlar valores a omitir columnas.
base_final_renombrada = TF.PandasBaseTransformer.Renombrar_columnas_con_diccionario(
    base=base_final_merge, cols_to_rename=config["base_final"]["orden_modificado"]
)
base_final_replace = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final_renombrada,
    base_right=driver_top_rojo,
    key="Cliente / Estrategia",
)

base_final_replace = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final_replace,
    base_right=driver_venta_cero,
    key="Cliente / Estrategia")


base_final_replace = aplicar_logica_omitidos(
    driver_activos_a_omitir, base_final_renombrada, TF
)

base_final_replace = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final_replace,
    base_right=driver_top_rojo,
    key="Cliente / Estrategia",
)

base_final_replace = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_final_replace,
    base_right=driver_venta_cero,
    key="Cliente / Estrategia")


base_final_ordenada = TF.PandasBaseTransformer.Seleccionar_columnas_pd(
    df=base_final_replace,
    cols_elegidas=list(config["base_final"]["orden_modificado"].values()),
)
VENTA_CERO_NEGATIVO = "Venta Cero o Negativo Mes"


base_final_ordenada2 = TF.PandasBaseTransformer.Cambiar_tipo_dato_multiples_columnas_pd(
    base=base_final_ordenada,
    list_columns=["Venta CERO Consecutivo"],
    type_data=float
)
base_final_ordenada2.loc[:,"Venta ROJO Consecutivo"] = (
    base_final_ordenada2["Venta ROJO Consecutivo"]
    .replace("", 0)
    .astype(float) 
)
base_final_ordenada2["Venta CERO Consecutivo"] = base_final_ordenada2.apply(
    lambda fila: fila["Venta CERO Consecutivo"] + 1 if fila[VENTA_CERO_NEGATIVO] == "x" else "",
    axis=1
)
base_final_ordenada2["Venta ROJO Consecutivo"] = base_final_ordenada2.apply(
    lambda fila: fila["Venta ROJO Consecutivo"] + 1 if fila[f"{ACTUAL}{config['año_actual']}"] == "ROJO" else "",
    axis=1
)

base_final_ordenada2.loc[:, "N. Meses"] = base_final_ordenada2["N. Meses"].fillna(1)

base_lista_activos_merge = TF.PandasBaseTransformer.pd_left_merge(
    base_left=base_lista_activos,
    base_right=base_final_ordenada[
        [
            "Cliente / Estrategia",
            "Descripción Activo",
            "Cód. Barras",
            "Estrategia",
            "Modelo",
            "Cód. Cliente PDV",
            "Oficina de Ventas",
            "Cód. Jefe de Ventas",
            "Jefe de Ventas",
        ]
    ],
    key="Cliente / Estrategia",
)

GF.exportar_a_excel(
    ruta_guardado=config["path_Resultados"],
    df=base_lista_activos,
    nom_hoja="Lista_activos_cliente",
)
GF.exportar_a_excel(
    ruta_guardado=config["path_Resultados"],
    df=base_lista_activos_sin_cliente,
    nom_hoja="Lista_activos_sin_cliente",
)
# Exportar resultado final a excel.
GF.exportar_a_excel(
    ruta_guardado=config["path_Resultados"],
    df=base_final_ordenada2,
    nom_hoja="Base_semaforo_activos",
)


Fin = datetime.now()
diferencia = Fin - Inicio
GF.imprimir_tiempo_estimado(diferencia=diferencia)
