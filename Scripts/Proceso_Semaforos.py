from Transformation_Functions import PandasBaseTransformer
from pandas import DataFrame, Series, Timestamp, isnull, concat
from typing import Type
import datetime


def Proceso_semaforo_activos(
    base_insumo_semaforos: DataFrame,
    driver_topes_acum: DataFrame,
    concat_ser_cli_est: DataFrame,
    config: dict,
    mes_actual: str,
):
    """
    Procesa la base de insumo para semáforos, aplicando transformaciones específicas y
    lógica de negocio utilizando funciones de pandas y PyArrow según se configure.

    Args:
        base_insumo_semaforos (pd.DataFrame): DataFrame de pandas que contiene la base de datos
            de insumo para el proceso de semáforo de activos.
        config (dict): Diccionario que contiene configuraciones específicas y parámetros necesarios
            para el proceso de semáforo, incluidos nombres de columnas, valores específicos, y más.

    Returns:
        pd.DataFrame: Un DataFrame de pandas que contiene los resultados del proceso de semáforo,
            incluidas las transformaciones aplicadas y la lógica de negocio implementada.
    """

    nuevas_cols = config["Insumos"]["Ventas_Muebles_PPago_Neve"]["nuevas_cols"]
    fillna_vtas_estatus = config["Insumos"]["Ventas_Muebles_PPago_Neve"][
        "fillna_vtas_estatus"
    ]

    #def contar_meses_consecutivos_rojos(
    #    df: DataFrame, columnas_estatus: list
    #) -> DataFrame:
    #    """
    #    Cuenta los meses consecutivos con estatus Rojo para cada fila.
    #    Se detiene de contar si encuentra un estatus que no es Rojo.
#
    #    Args:
    #        df (DataFrame): DataFrame con los datos de venta y estatus.
    #        columnas_estatus (list): Lista de columnas de estatus en el orden cronológico (del más antiguo al más reciente).
#
    #    Returns:
    #        DataFrame: DataFrame con una nueva columna 'TIEMPO EN ROJO' que tiene el conteo de meses consecutivos con estatus Rojo.
    #    """
#
    #    # Asumiendo que la columna 'TIEMPO EN ROJO' ya existe, si no, la crea e inicializa con cero.
    #    if "TIEMPO EN ROJO" not in df.columns:
    #        df["TIEMPO EN ROJO"] = 0
#
    #    # Recorre las filas del DataFrame
    #    for index, row in df.iterrows():
    #        # Contador para los meses consecutivos en rojo
    #        meses_consecutivos_rojos = 0
    #        # Recorre las columnas de estatus en orden inverso
    #        for col_estatus in columnas_estatus:
    #            # Si encuentra un estatus rojo, incrementa el contador
    #            if row[col_estatus] == "ROJO":
    #                meses_consecutivos_rojos += 1
    #            # Si encuentra un estatus que no es rojo, detiene el conteo
    #            else:
    #                break
    #        # Asigna el contador a la columna 'TIEMPO EN ROJO'
    #        df.at[index, "TIEMPO EN ROJO"] = meses_consecutivos_rojos
#
    #    return df

    def imputar_valor(row):
        if isnull(row["Cod Actual"]):
            return row["Cliente"]
        else:
            pass

    def asignar_estatus_por_topes(
        df,
        columnas_ventas,
        columna_top_rojo,
        columna_top_verde,
        driver_topes_acum,
        concat_ser_cli_est,
    ):
        """
        Asigna un estatus a cada fila basado en el valor de las columnas de ventas comparado
        con dos columnas de topes (rojo y verde), considerando un caso especial para "Venta Acum.".

        Args:
            df (pd.DataFrame): DataFrame que contiene las columnas de ventas y topes.
            columnas_ventas (list): Lista de nombres de las columnas de ventas a evaluar.
            columna_top_rojo (str): Nombre de la columna que contiene el tope rojo.
            columna_top_verde (str): Nombre de la columna que contiene el tope verde.

        Returns:
            pd.DataFrame: DataFrame original con una nueva columna de estatus para cada columna de ventas.
        """
        df_copy = df.copy()
        driver_topes_acum_copy = driver_topes_acum.copy()

        CONCATENADA_TEMPORAL = "Concatenada_temporal"
        TOPE_ROJO = "TOPE ROJO"
        TOPE_VERDE = "TOPE VERDE"
        TIPO_AJUST = "Tipo_ajust"
        CLIENTE_ESTRATEGIA = "Cliente / Estrategia"

        # Filtrar columna actual y venta Acum:
        cols_actual_acum = [
            col
            for col in columnas_ventas
            if (mes_actual in col) or (col == "Venta Acum.")
        ]

        for col_venta in cols_actual_acum:
            # Caso especial para "Venta Acum."
            if col_venta == "Venta Acum.":
                # Copiar el DataFrame y crear la columna TIPO_AJUST
                df_copy[TIPO_AJUST] = ""
                df[TIPO_AJUST] = ""

                # Reemplazar valores de TIPO_AJUST basado en "Tipo Activo"
                dict_reemplazos = config["dict_reemplazos_finales"]["Tipo Activo"][
                    "dict_reemplazos"
                ]
                df_copy = PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
                    df=df_copy,
                    nom_columna_a_reemplazar=TIPO_AJUST,
                    nom_columna_de_referencia="Tipo Activo",
                    mapeo=dict_reemplazos,
                )
                df = PandasBaseTransformer.Reemplazar_columna_en_funcion_de_otra(
                    df=df,
                    nom_columna_a_reemplazar=TIPO_AJUST,
                    nom_columna_de_referencia="Tipo Activo",
                    mapeo=dict_reemplazos,
                )

                # Concatenar columnas "Cliente" y TIPO_AJUST en CONCATENADA_TEMPORAL
                df_copy = PandasBaseTransformer.concatenar_columnas_pd(
                    dataframe=df_copy,
                    cols_elegidas=["Cliente", TIPO_AJUST],
                    nueva_columna=CONCATENADA_TEMPORAL,
                )
                df = PandasBaseTransformer.concatenar_columnas_pd(
                    dataframe=df,
                    cols_elegidas=["Cliente", TIPO_AJUST],
                    nueva_columna=CONCATENADA_TEMPORAL,
                )

                # Realizar el primer merge
                df_copy = PandasBaseTransformer.pd_left_merge_two_keys(
                    base_left=df_copy,
                    base_right=driver_topes_acum_copy,
                    left_key=CONCATENADA_TEMPORAL,
                    right_key=CLIENTE_ESTRATEGIA,
                )

                # Realizar el segundo merge
                driver_topes_acum_copy = PandasBaseTransformer.pd_left_merge_two_keys(
                    base_left=driver_topes_acum_copy,
                    base_right=df_copy[
                        [CONCATENADA_TEMPORAL, columna_top_rojo, columna_top_verde]
                    ],
                    left_key=CLIENTE_ESTRATEGIA,
                    right_key=CONCATENADA_TEMPORAL,
                )

                # Realizar ajustes en los topes de TOPE_ROJO y TOPE_VERDE
                df_copy[columna_top_rojo] = df_copy[columna_top_rojo].fillna(
                    0
                ) + df_copy[TOPE_ROJO].fillna(0).astype(float)
                df_copy[columna_top_verde] = df_copy[columna_top_verde].fillna(
                    0
                ) + df_copy[TOPE_VERDE].fillna(0).astype(float)

                faltantes = df_copy[df_copy[CLIENTE_ESTRATEGIA].isnull()][
                    [
                        CONCATENADA_TEMPORAL,
                        TOPE_ROJO,
                        TOPE_VERDE,
                        columna_top_rojo,
                        columna_top_verde,
                    ]
                ]
                faltantes[[TOPE_ROJO, TOPE_VERDE]] = faltantes[
                    [TOPE_ROJO, TOPE_VERDE]
                ].fillna(0)

                faltantes_rename = faltantes.rename(
                    columns={CONCATENADA_TEMPORAL: CLIENTE_ESTRATEGIA}
                )

                # Agregar los registros faltantes a la izquierda
                driver_topes_acum_copy_completo = concat(
                    [driver_topes_acum_copy, faltantes_rename], ignore_index=True
                )
                # Actualizar topes en "driver_topes_acum_copy"
                driver_topes_acum_copy_completo[
                    [columna_top_verde, columna_top_rojo]
                ] = driver_topes_acum_copy_completo[
                    [columna_top_verde, columna_top_rojo]
                ].fillna(
                    0
                )
                driver_topes_acum_copy_completo[[TOPE_VERDE, TOPE_ROJO]] = (
                    driver_topes_acum_copy_completo[[TOPE_VERDE, TOPE_ROJO]].astype(
                        float
                    )
                )
                driver_topes_acum_copy_completo[
                    TOPE_VERDE
                ] += driver_topes_acum_copy_completo[columna_top_verde]
                driver_topes_acum_copy_completo[
                    TOPE_ROJO
                ] += driver_topes_acum_copy_completo[columna_top_rojo]
                # Variables finales
                top_rojo = df_copy[columna_top_rojo]
                top_verde = df_copy[columna_top_verde]

                nueva_col_estatus = f"Estatus_{col_venta}"

                # Inicializar la nueva columna de estatus como vacía
                df_copy[nueva_col_estatus] = None

                # Asignar estatus basado en los topes
                df_copy.loc[df_copy[col_venta] >= top_verde, nueva_col_estatus] = (
                    "VERDE"
                )
                df_copy.loc[
                    (df_copy[col_venta] >= top_rojo) & (df_copy[col_venta] < top_verde),
                    nueva_col_estatus,
                ] = "AMARILLO"
                df_copy.loc[df_copy[col_venta] < top_rojo, nueva_col_estatus] = "ROJO"
            else:
                top_rojo = df_copy[columna_top_rojo]
                top_verde = df_copy[columna_top_verde]

                nueva_col_estatus = f"Estatus_{col_venta}"

                # Inicializar la nueva columna de estatus como vacía
                df_copy[nueva_col_estatus] = None

                # Asignar estatus basado en los topes
                df_copy.loc[df_copy[col_venta] >= top_verde, nueva_col_estatus] = (
                    "VERDE"
                )
                df_copy.loc[
                    (df_copy[col_venta] >= top_rojo) & (df_copy[col_venta] < top_verde),
                    nueva_col_estatus,
                ] = "AMARILLO"
                df_copy.loc[df_copy[col_venta] < top_rojo, nueva_col_estatus] = "ROJO"

        # Eliminar columna temporal.
        df_copy_delete_cols = PandasBaseTransformer.Eliminar_columnas(
            df=df_copy,
            columnas_a_eliminar=[
                CLIENTE_ESTRATEGIA,
                TOPE_VERDE,
                TOPE_ROJO,
                TIPO_AJUST,
                columna_top_verde,
                columna_top_rojo,
            ],
        )
        driver_topes_acum_copy_delete_cols = PandasBaseTransformer.Eliminar_columnas(
            df=driver_topes_acum_copy_completo,
            columnas_a_eliminar=[
                CONCATENADA_TEMPORAL,
                columna_top_rojo,
                columna_top_verde,
            ],
        )
        # Merge para restaurar los topes iniciales del mes.
        df_restaurado = df[
            [CONCATENADA_TEMPORAL, columna_top_verde, columna_top_rojo]
        ].drop_duplicates(subset=CONCATENADA_TEMPORAL)

        df_copy_delete_cols = PandasBaseTransformer.pd_left_merge(
            base_left=df_copy_delete_cols,
            base_right=df_restaurado,
            key=CONCATENADA_TEMPORAL,
        )
        df_copy_delete_cols

        cols_not_actual_acum = [
            col
            for col in concat_ser_cli_est
            if (mes_actual not in col) and (col != "Venta Acum.")
        ]

        concat_ser_cli_est_select = concat_ser_cli_est[cols_not_actual_acum]

        df_copy_delete_cols_replace = (
            PandasBaseTransformer.Reemplazar_valores_con_dict_pd(
                df=df_copy_delete_cols,
                columna="Tipo Activo",
                diccionario_mapeo=dict_reemplazos,
            )
        )
        df_copy_delete_cols_replace = PandasBaseTransformer.concatenar_columnas_pd(
            dataframe=df_copy_delete_cols_replace,
            cols_elegidas=["Cliente", "Tipo Activo"],
            nueva_columna="Concatenar Cliente y Estrategia",
        )

        df_copy_delete_cols_replace_merge = PandasBaseTransformer.pd_left_merge(
            base_left=df_copy_delete_cols_replace,
            base_right=concat_ser_cli_est_select,
            key="Concatenar Cliente y Estrategia",
        )

        return df_copy_delete_cols_replace_merge, driver_topes_acum_copy_delete_cols

    #def contar_condiciones_automaticamente(df):
    #    # Inicializar la nueva columna para el conteo
    #    df["Conteo_Condicional"] = 0
#
    #    # Encontrar los pares de columnas correspondientes a cada mes.
    #    # Asumiendo que las columnas siguen el formato 'Estatus_Venta $ {mes}' y 'Venta $ {mes}'
    #    columnas_estatus = [
    #        col for col in df.columns if col.startswith("Estatus_Venta $")
    #    ]
    #    meses = [col.split("$ ")[1] for col in columnas_estatus]
#
    #    # Crear una máscara para identificar las filas que no deben seguir siendo contadas.
    #    # Inicialmente, todas las filas pueden ser contadas.
    #    mask = Series(True, index=df.index)
#
    #    for mes in meses:
    #        col_estatus = f"Estatus_Venta $ {mes}"
    #        col_venta = f"Venta $ {mes}"
#
    #        # Asegúrate de que ambas columnas, estatus y venta, existen en el DataFrame
    #        if col_estatus in df.columns and col_venta in df.columns:
    #            # Aplicar la condición: si estatus no es 'NR' y venta es '0' o menos, sumar 1 al conteo.
    #            condicion = (df[col_estatus] != "NR") & (df[col_venta] <= 0) & mask
    #            # Actualizar la máscara para excluir filas con estatus 'ROJO'
    #            mask &= df[col_estatus] != "ROJO"
    #            # Sumar 1 al conteo solo en las filas que cumplen la condición y no han sido excluidas
    #            df["Conteo_Condicional"] += condicion.astype(int)
#
    #    return df

    def encontrar_fecha_mas_antigua(fecha_cadena):
        # Separar las fechas usando la coma como delimitador y eliminar espacios en blanco
        fechas = [fecha.strip() for fecha in fecha_cadena.split(",")]
        # Convertir las cadenas de fechas en objetos datetime, ignorando las cadenas vacías y las fechas incorrectas
        fechas_datetime = []
        for fecha in fechas:
            if fecha:
                try:
                    fecha_datetime = datetime.datetime.strptime(fecha, "%Y-%m-%d")
                    fechas_datetime.append(fecha_datetime)
                except ValueError:
                    # Si la conversión falla, pasa a la siguiente fecha sin imprimir nada.
                    pass
        # Devolver la fecha más antigua si hay al menos una fecha válida
        return min(fechas_datetime) if fechas_datetime else None

    # Copia para trabajar
    base_insumo_semaforos_copy = base_insumo_semaforos.copy()

    # Aplicar la función a cada fila de la columna 'Fe.suministro'
    base_insumo_semaforos_copy[nuevas_cols["Fecha Instalación Ajustada"]] = (
        base_insumo_semaforos_copy["Fe.suministro"].apply(encontrar_fecha_mas_antigua)
    )

    # Vamos a sumar las columnas numericas.
    # Lista_cols_base
    cols_base = list(base_insumo_semaforos_copy.columns)

    # Cols_numericas
    cols_numericas = [
        col
        for col in cols_base
        if config["dict_constantes"]["Venta $"] in col
        or config["dict_constantes"]["TOP_"] in col
    ]

    # Cols_a_sumar
    cols_to_sum = [
        col for col in cols_base if config["dict_constantes"]["Venta $"] in col
    ]

    # Cambiar tipo de dato de cols_numericas para hacer operaciones.
    base_insumo_semaforos_copy[cols_numericas] = base_insumo_semaforos_copy[
        cols_numericas
    ].astype(float)

    # Tomemos la venta acumulada.
    base_insumo_semaforos_copy[nuevas_cols["Venta Acum."]] = base_insumo_semaforos_copy[
        cols_to_sum
    ].sum(axis=1)

    # Agregar la columna venta acumulada a las cols_to_sum
    cols_to_sum.append(nuevas_cols["Venta Acum."])

    # Estatus de las ventas por mes.
    base_insumo_semaforos_copy_estatus, driver_topes_acum_copy_delete_cols = (
        asignar_estatus_por_topes(
            base_insumo_semaforos_copy,
            columnas_ventas=cols_to_sum,
            columna_top_rojo="TOP_ROJO",
            columna_top_verde="TOP_VERDE",
            driver_topes_acum=driver_topes_acum,
            concat_ser_cli_est=concat_ser_cli_est,
        )
    )

    # Asumiendo que el año actual se determina automáticamente
    anio_actual = Timestamp.now().year
    m_actual = Timestamp.now().month

    # Identificar las columnas de Estatus_Venta por mes
    columnas_mes = [
        col
        for col in base_insumo_semaforos_copy_estatus.columns
        if col.startswith("Venta $")
    ]

    columnas_estatus = [
        col
        for col in base_insumo_semaforos_copy_estatus.columns
        if col.startswith("Estatus_Venta $")
    ]

    dict_rellenar_cols = {
        fillna_vtas_estatus[0]: columnas_mes,
        fillna_vtas_estatus[1]: columnas_estatus,
    }

    for cada_valor, columnas in dict_rellenar_cols.items():
        base_insumo_semaforos_copy_estatus[columnas] = (
            base_insumo_semaforos_copy_estatus[columnas].fillna(cada_valor)
        )

    #base_insumo_semaforos_copy_estatus = contar_meses_consecutivos_rojos(
    #    df=base_insumo_semaforos_copy_estatus, columnas_estatus=columnas_estatus
    #)

    # Crear columnas constante año_actual.
    año_actual = config["Año_act"]
    base_insumo_semaforos_copy_estatus[año_actual] = config["año_actual"]
    base_insumo_semaforos_copy_estatus["Mes_act"] = mes_actual

    # Crear columna constante Cliente inactivo.
    # Crear columna constante Cliente inactivo inicialmente vacía.

    base_insumo_semaforos_copy_estatus[
        config["dict_constantes"]["Cliente Inactivo"]
    ] = " "

    # Clientes inactivos: marcar con el nombre del cliente si 'Cod Actual' está vacío.
    # Asumiendo que 'Cod Actual' es el nombre correcto de la columna.

    base_insumo_semaforos_copy_estatus[
        config["dict_constantes"]["Cliente Inactivo"]
    ] = base_insumo_semaforos_copy_estatus.apply(imputar_valor, axis=1)

    # Agregar la columna tiempo venta.
    # Aplicar la función
    #base_insumo_semaforos_copy_estatus = contar_condiciones_automaticamente(
    #    df=base_insumo_semaforos_copy_estatus
    #)

    base_insumo_semaforos_copy_estatus["MESES_DISPONIBLES"] = len(columnas_estatus)

    # Agregar columna pendiente de venta.
    def calcular_pendiente(row, mes_actual, estatus):
        if row[f"Estatus_Venta $ {mes_actual}"] == estatus:
            if estatus != "VERDE":
                return abs(row[f"Venta $ {mes_actual}"] - row["TOP_VERDE"])
            else:
                return 0
        return 0

    # Iniciar columna en 0 antes de sumar
    base_insumo_semaforos_copy_estatus["PENDIENTE DE VENTA"] = 0

    # Sumar los pendientes de ROJO y AMARILLO en la misma columna
    for est in ["ROJO", "AMARILLO"]:
        base_insumo_semaforos_copy_estatus[
            "PENDIENTE DE VENTA"
        ] += base_insumo_semaforos_copy_estatus.apply(
            calcular_pendiente, axis=1, args=(mes_actual, est)
        )

    # Calcular pendiente para VERDE por separado
    base_insumo_semaforos_copy_estatus["PENDIENTE DE META"] = 0

    base_insumo_semaforos_copy_estatus.loc[
        base_insumo_semaforos_copy_estatus[f"Estatus_Venta $ {mes_actual}"]
        == "AMARILLO",
        "PENDIENTE DE META",
    ] = base_insumo_semaforos_copy_estatus["PENDIENTE DE VENTA"]

    base_insumo_semaforos_copy_estatus["Fecha Instalación Ajustada"] = (
        base_insumo_semaforos_copy_estatus["Fecha Instalación Ajustada"]
        .astype(str)
        .str.split()
        .str[0]
    )

    base_insumo_semaforos_copy_estatus = base_insumo_semaforos_copy_estatus.drop(
        columns=["key_vtas"]
    )

    # Agregar columnas faltantes de vtas.
    # Columnas posibles de estado del config.
    cols_faltantes_posibles = config["base_final"]["cols_faltantes_posibles"]

    for cada_col_venta in cols_faltantes_posibles.values():
        if (cada_col_venta) not in list(base_insumo_semaforos_copy_estatus.columns):
            base_insumo_semaforos_copy_estatus[cada_col_venta] = 0
        else:
            pass

    # Rerodenar columnas finales.
    orden_final_cols = config["base_final"]["orden_final_cols"]

    base_insumo_semaforos_rename = (
        PandasBaseTransformer.Renombrar_columnas_con_diccionario(
            base=base_insumo_semaforos_copy_estatus, cols_to_rename=orden_final_cols
        )
    )
    mes = str(datetime.datetime.now().month)
    driver_topes_acum_copy_delete_cols.to_excel(
        f"Resultados/Historico_topes_{mes}.xlsx", index=False
    )
    # Retornar la base con los conceptos de vtas.
    return base_insumo_semaforos_rename
