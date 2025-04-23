from pandas import DataFrame
from typing import Type


def Transformaciones_Ventas(
    ventas_snak_pp: DataFrame,
    ventas_neveras: DataFrame,
    config: dict,
    Pandas_Functions: Type,
):

    # Documentación.

    config_vtas = config["Insumos"]["Ventas_Muebles_PPago_Neve"]
    vtas = config_vtas["cols_constantes"]

    # Columnas vtas.
    cols_vtas = config["Insumos"]["Ventas_Muebles_PPago_Neve"]["dict_cols_necesarias"]

    # Crear copias de los archivos de ventas para trabajar.
    vts_nevrs = ventas_neveras.copy()
    vts_snkros = ventas_snak_pp.copy()
    vtas_pp = ventas_snak_pp.copy()

    # Crear columna constante tipo de activo.
    vts_snkros.loc[:, vtas["Act_snk"]["Columna"]] = vtas["Act_snk"]["Valor"]
    vtas_pp.loc[:, vtas["Act_PP"]["Columna"]] = vtas["Act_PP"]["Valor"]
    vts_nevrs.loc[:, vtas["Act_Nev"]["Columna"]] = vtas["Act_Nev"]["Valor"]

    vtas_concat = Pandas_Functions.concatenate_dataframes(
        dataframes=[vts_nevrs, vtas_pp, vts_snkros]
    )

    # Filtrar consultas vtas. por Tipo "Directa/Indirecta"
    vtas_completas_D = vtas_concat[vtas_concat[cols_vtas["Tipo de Venta"]].isin(["D"])]
    vtas_completas_I = vtas_concat[vtas_concat[cols_vtas["Tipo de Venta"]].isin(["I"])]

    # Concatenar las columnas "Código ECOM" Y "Agente Comercial - Clave" para usarlas como llave de indirecta.
    cols_concat = config_vtas["cols_concat"]
    cols_agrup_indir = config_vtas["cols_agrup"]["Indirecta"]

    vtas_completas_I.loc[
        vtas_completas_I["Código ECOM"] == "Sin asignar", "Código ECOM"
    ] = vtas_completas_I["Cliente - Clave"]


    vtas_completas_I = Pandas_Functions.concatenar_columnas_pd(
        dataframe=vtas_completas_I,
        cols_elegidas=cols_concat["Indirecta"]["Columnas"],
        nueva_columna=cols_concat["Indirecta"]["Nombre"],
    )

    vtas_completas_D = Pandas_Functions.concatenar_columnas_pd(
        dataframe=vtas_completas_D,
        cols_elegidas=cols_concat["Directa"]["Columnas"],
        nueva_columna=cols_concat["Directa"]["Nombre"],
    )

    # Selecionar columnas ambas bases.
    vtas_completas_D_select = Pandas_Functions.Seleccionar_columnas_pd(
        df=vtas_completas_D, cols_elegidas=config_vtas["cols_select_final"]
    )

    vtas_completas_I_select = Pandas_Functions.Seleccionar_columnas_pd(
        df=vtas_completas_I, cols_elegidas=config_vtas["cols_select_final"]
    )

    vtas_completas_I_select.loc[:, cols_agrup_indir["sum_col"]] = (
        vtas_completas_I_select[cols_agrup_indir["sum_col"]].astype(float)
    )

    vtas_completas_D_select.loc[:, cols_agrup_indir["sum_col"]] = (
        vtas_completas_D_select[cols_agrup_indir["sum_col"]].astype(float)
    )

    vtas_completas_I_agrup = Pandas_Functions.Group_by_and_sum_cols_pd(
        df=vtas_completas_I_select,
        group_col=cols_agrup_indir["group_col"],
        sum_col=cols_agrup_indir["sum_col"],
    )

    # Concatenar bases anteriores
    vtas_completa = Pandas_Functions.concatenate_dataframes(
        dataframes=[vtas_completas_I_agrup, vtas_completas_D_select]
    )

    return vtas_completa
