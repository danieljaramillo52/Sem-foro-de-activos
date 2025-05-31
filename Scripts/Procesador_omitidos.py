
OMITIR = "Omitir"
NOMBRE_COLUMNA = "Nombre Columna"
VALOR_DEL_CAMPO = "Valor del campo"
CODIGO_BARRAS = "Cód. Barras"


def aplicar_logica_omitidos(driver_activos_a_omitir, base_final_merge, TF):
    """
    Agrega la columna 'Omitir' a `base_final_merge` marcando con 'x' aquellas filas
    que coinciden con reglas definidas en `driver_activos_a_omitir`.

    Usa la función existente:
    TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra_extendido

    Args:
        driver_activos_a_omitir (pd.DataFrame): Tabla con condiciones de omisión.
        base_final_merge (pd.DataFrame): DataFrame principal a modificar.
        TF (module/class): Módulo que contiene la función `PandasBaseTransformer`.

    Returns:
        pd.DataFrame: DataFrame con la columna 'Omitir' modificada.
    """

    # Agrupar el driver y convertirlo a diccionario
    dict_activos_omitir = (
        driver_activos_a_omitir
        .groupby(NOMBRE_COLUMNA)[VALOR_DEL_CAMPO]
        .apply(list)
        .to_dict()
    )

    # Crear diccionarios de mapeo por columna
    dict_col_omitir_mapeo = {
        clave: {valor: "x" for valor in lista}
        for clave, lista in dict_activos_omitir.items()
    }

    # Inicializar columna "Omitir"
    base_final_merge.loc[:,OMITIR] = ""

    base_final_replace = base_final_merge.copy()

    # Aplicar reemplazos por cada columna
    for cada_col, dict_reemplazos in dict_col_omitir_mapeo.items():
        
        buscar_fila = True if cada_col == CODIGO_BARRAS else False

        base_final_replace = TF.PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra_extendido(
            df=base_final_replace,
            nom_columna_a_reemplazar=OMITIR,
            nom_columna_de_referencia=cada_col,
            mapeo=dict_reemplazos,
            buscar_en_toda_la_fila=buscar_fila
        )

    return base_final_replace
