from General_Functions import Lectura_simple_excel
from Transformation_Functions import PandasBaseTransformer as PBT


# Modulo de lectura de insumos
def Lectura_archivos(
    funcion_lectura: callable,
    funcion_select_cols: callable,
    dict_insumos: dict,
    dict_drivers: dict,
) -> tuple:
    """Lectura_plantillas : funcion que instancia repetidamente a function_lectura.
        Plantilla: Archivo de excel.
    Args:
        funcion_lectura: Lectura insumos excel (Localizada en: Utils/General_functions)
            Acerca función_lectura:
                Description:
                    Funcion que lee archivos excel en formato xlsx/xlsm y los carga en memoria.
                Args:
                    - path: (type : str)
                    - nom_insumo (type : str)
                    - nom_hoja (type: str)
                    - cols (type: int)
                Returns:
                    - pd.DataFrame:

        function_select_cols: Seleccionar_columnas_pd ((Localizada en: Utils/Transformation_functions)

        dict_insumos (type : dict)  diccionario con los información de los insumos a cargar en memoria.

        dict_drivers (type : dict)  diccionario con los información de los insumos a cargar en memoria.)

        (Driver (dfn:) Se conoce como un driver a otro insumo de excel .xlsx , .xlsm que sirve de apoyo para transformar la data de los insumos.)

    Returns: dataframes: (type: tuple) Tupla con todos los datasframes leidos."""

    ventas_snkros_pp = funcion_lectura(
        path=dict_insumos["path_ventas"],
        nom_insumo=dict_insumos["Ventas_Muebles_PPago_Neve"]["nom_base"][0],
        nom_hoja=dict_insumos["Ventas_Muebles_PPago_Neve"]["nom_hoja"][0],
        cols=dict_insumos["Ventas_Muebles_PPago_Neve"]["cols"],
        modo_pruebas=False,
    )
    ventas_neveras = funcion_lectura(
        path=dict_insumos["path_ventas"],
        nom_insumo=dict_insumos["Ventas_Muebles_PPago_Neve"]["nom_base"][1],
        nom_hoja=dict_insumos["Ventas_Muebles_PPago_Neve"]["nom_hoja"][1],
        cols=dict_insumos["Ventas_Muebles_PPago_Neve"]["cols"],
        modo_pruebas=False,
    )

    maestra_activos_sap = funcion_lectura(
        path=dict_insumos["path_insumos"],
        nom_insumo=dict_insumos["Maestra_Activos_SAP"]["nom_base"],
        nom_hoja=dict_insumos["Maestra_Activos_SAP"]["nom_hoja"],
        cols=dict_insumos["Maestra_Activos_SAP"]["cols"],
        modo_pruebas=False,
    )

    maestra_activos_indirecta = funcion_lectura(
        path=dict_insumos["path_insumos"],
        nom_insumo=dict_insumos["Maestra_Activos_Indirecta"]["nom_base"],
        nom_hoja=dict_insumos["Maestra_Activos_Indirecta"]["nom_hoja"],
        cols=dict_insumos["Maestra_Activos_Indirecta"]["cols"],
        modo_pruebas=False,
    )

    universo_directa = funcion_lectura(
        path=dict_insumos["path_univeros"],
        nom_insumo=dict_insumos["Universos_directa_indirecta"]["nom_base_dir"],
        nom_hoja=dict_insumos["Universos_directa_indirecta"]["nom_hoja"],
        cols=dict_insumos["Universos_directa_indirecta"]["cols"],
        engine=dict_insumos["Universos_directa_indirecta"]["engine"],
        modo_pruebas=False,
    )

    universo_indirecta = funcion_lectura(
        path=dict_insumos["path_univeros"],
        nom_insumo=dict_insumos["Universos_directa_indirecta"]["nom_base_indir"],
        nom_hoja=dict_insumos["Universos_directa_indirecta"]["nom_hoja"],
        cols=dict_insumos["Universos_directa_indirecta"]["cols"],
        engine=dict_insumos["Universos_directa_indirecta"]["engine"],
        modo_pruebas=False,
    )

    # Lectura_maestras_clientes_inactivos
    maestra_clientes_inac_indir = funcion_lectura(
        path=dict_insumos["path_cli_inac"],
        nom_insumo=dict_insumos["maestra_clientes_dir_indir"]["nom_base_indir"],
        nom_hoja=dict_insumos["maestra_clientes_dir_indir"]["nom_hoja"],
        cols=dict_insumos["maestra_clientes_dir_indir"]["cols"],
        engine=dict_insumos["maestra_clientes_dir_indir"]["engine"],
        modo_pruebas=False,
    )

    maestra_clientes_inac_dir = funcion_lectura(
        path=dict_insumos["path_cli_inac"],
        nom_insumo=dict_insumos["maestra_clientes_dir_indir"]["nom_base_dir"],
        nom_hoja=dict_insumos["maestra_clientes_dir_indir"]["nom_hoja"],
        cols=dict_insumos["maestra_clientes_dir_indir"]["cols"],
        engine=dict_insumos["maestra_clientes_dir_indir"]["engine"],
        modo_pruebas=False,
    )

    # Lectura Drivers.
    # Driver Activos y Estrategias
    driver_ac_estra = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["Activos y Estrategias"],
        cols=dict_drivers["cols"]["Activos y Estrategias"],
        modo_pruebas=False,
    )
    driver_ac_carg = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["Activos y Cargues"],
        cols=dict_drivers["cols"]["Activos y Cargues"],
        modo_pruebas=False,
    )

    driver_manto_neveras = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["Nev_mantto"]["nom_base"],
        nom_hoja=dict_drivers["Nev_mantto"]["nom_hoja"],
        cols=dict_drivers["Nev_mantto"]["cols"],
        modo_pruebas=False,
    )

    driver_activos_a_omitir = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["Activos a Omitir"],
        cols=dict_drivers["cols"]["Activos a Omitir"],
        modo_pruebas=False,
    )

    driver_topes_acum = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["HISTÓRICO TOPES"],
        cols=dict_drivers["cols"]["HISTÓRICO TOPES"],
        modo_pruebas=False,
    )
    driver_region = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["DRIVER REGIONALES"],
        cols=dict_drivers["cols"]["DRIVER REGIONALES"],
        modo_pruebas=False,
    )
    driver_jefes_agentes = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["Jefes y Agentes"],
        cols=dict_drivers["cols"]["Jefes y Agentes"],
        modo_pruebas=False,
    )
    driver_cli_atrb = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["Clientes con Atributos"],
        cols=dict_drivers["cols"]["Clientes con Atributos"],
        modo_pruebas=False,
    )
    driver_ldcr = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["LDCR"],
        cols=dict_drivers["cols"]["LDCR"],
        modo_pruebas=False,
    )

    driver_venta_cero = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["VENTA CERO"],
        cols=2,
        modo_pruebas=False,
    )
    driver_top_rojo = funcion_lectura(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["nom_base"],
        nom_hoja=dict_drivers["nom_hoja"]["ROJOS"],
        cols=dict_drivers["cols"]["ROJOS"],
        modo_pruebas=False,
    )

    ## Selecionar columnas necesarias de las bases.
    vts_neveras_select = funcion_select_cols(
        df=ventas_neveras,
        cols_elegidas=dict_insumos["Ventas_Muebles_PPago_Neve"]["cols_necesarias"],
    )

    vts_snkros_pp_select = funcion_select_cols(
        df=ventas_snkros_pp,
        cols_elegidas=dict_insumos["Ventas_Muebles_PPago_Neve"]["cols_necesarias"],
    )

    maestra_activos_sap_reg = funcion_select_cols(
        df=maestra_activos_sap, cols_elegidas=["Cliente", "Regional"]
    )
    maestra_activos_sap_reg.dropna(subset="Cliente")


    # Driver estados.
    dict_estados_meses = Lectura_simple_excel(
        path=dict_drivers["path_drivers"],
        nom_insumo=dict_drivers["Estado_acumulado"]["nom_base"],
    )
    list_meses = dict_drivers["Estado_acumulado"]["nom_hojas"]
    list_series_cli_est = [
        dict_estados_meses[cada_mes]["Concatenar Cliente y Estrategia"]
        for cada_mes in list_meses
    ]
    concat_ser_cli_est = PBT.Remove_duplicates(
        PBT.concatenate_dataframes(dataframes=list_series_cli_est)
    )
    dict_nuvos_noms = {}
    for cada_mes in list_meses:
        dict_estados_meses[cada_mes] = dict_estados_meses[cada_mes].rename(
            columns={"Estado Mes": cada_mes}
        )
        concat_ser_cli_est = PBT.pd_left_merge(
            base_left=concat_ser_cli_est,
            base_right=dict_estados_meses[cada_mes][
                ["Concatenar Cliente y Estrategia", cada_mes]
            ],
            key="Concatenar Cliente y Estrategia",
        )
        dict_nuvos_noms[cada_mes] = f"Estatus_Venta $ {cada_mes}"

    concat_ser_cli_est = concat_ser_cli_est.fillna("NR")

    concat_ser_cli_est_sin_index = PBT.Eliminar_columnas(
        df=concat_ser_cli_est, columnas_a_eliminar="index"
    )

    concat_ser_cli_est_sin_index["Conteo_meses"] = concat_ser_cli_est_sin_index.apply(
        lambda row: (row != "NR").sum(), axis=1
    )

    concat_ser_cli_est_sin_index.loc[:, "Conteo_meses"] = concat_ser_cli_est_sin_index[
        "Conteo_meses"
    ].fillna(1)

    concat_ser_cli_est_sin_index_rename = PBT.Renombrar_columnas_con_diccionario(
        base=concat_ser_cli_est_sin_index, cols_to_rename=dict_nuvos_noms
    )

    return (
        universo_directa,
        universo_indirecta,
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
        concat_ser_cli_est_sin_index_rename,
    )
