import sys
import pandas as pd
import numpy as np
import pyarrow as pa
from loguru import logger
from typing import List, Any
from functools import reduce
from General_Functions import Registro_tiempo


class PyArrowColumnTransformer:

    @Registro_tiempo
    @staticmethod
    def remove_first_n_characters(
        table: pa.Table, column_name: str, n: int
    ) -> pa.Table:
        """
        Elimina los primeros 'n' caracteres de cada cadena en la columna especificada de una tabla PyArrow.

        Parameters:
        table (pa.Table): La tabla PyArrow a modificar.
        column_name (str): El nombre de la columna a modificar.
        n (int): Número de caracteres a eliminar desde el inicio de cada cadena.

        Returns:
        pa.Table: Una nueva tabla PyArrow con la columna modificada.

        Example:
        >>> table = pa.Table.from_pydict({'Nombre': ['Alice', 'Bob', 'Charlie']})
        >>> modified_table = PyArrowTableModifier.remove_first_n_characters(table, 'Nombre', 2)
        >>> modified_table.column('Nombre').to_pylist()
        ['ice', 'b', 'arlie']
        """
        try:
            pattern = "^.{" + str(n) + "}"
            modified_column = pa.compute.replace_substring_regex(
                table[column_name], pattern=pattern, replacement=""
            )
            return table.set_column(
                table.schema.get_field_index(column_name), column_name, modified_column
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Error al eliminar los primeros {n} caracteres: {e}")
        except pa.lib.ArrowInvalid as e:
            logger.error(f"Error de PyArrow: {e}")

    @Registro_tiempo
    @staticmethod
    def remove_non_numeric_characters(table: pa.Table, column_name: str) -> pa.Table:
        """
        Elimina todos los caracteres no numéricos de las cadenas en la columna especificada de una tabla PyArrow.

        Parameters:
        table (pa.Table): La tabla PyArrow a modificar.
        column_name (str): El nombre de la columna a modificar.

        Returns:
        pa.Table: Una nueva tabla PyArrow con la columna modificada.

        Example:
        >>> table = pa.table({'Texto': ['Hola123', '456Mundo', 'Python789']})
        >>> modified_table = remove_non_numeric_characters(table, 'Texto')
        >>> print(modified_table.column('Texto').to_pylist())
        ['123', '456', '789']
        """
        # Define el patrón para coincidir con cualquier cosa que no sea un dígito
        pattern = "[^0-9]"
        # Reemplaza los caracteres que coinciden con el patrón por una cadena vacía
        modified_column = pa.compute.replace_substring_regex(
            table[column_name], pattern=pattern, replacement=""
        )
        # Devuelve la tabla con la columna modificada
        return table.set_column(
            table.schema.get_field_index(column_name), column_name, modified_column
        )

    @Registro_tiempo
    @staticmethod
    def remove_last_n_characters(table: pa.Table, column_name: str, n: int) -> pa.Table:
        """
        Elimina los últimos 'n' caracteres de cada cadena en la columna especificada de una tabla PyArrow.

        Parameters:
        table (pa.Table): La tabla PyArrow a modificar.
        column_name (str): El nombre de la columna a modificar.
        n (int): Número de caracteres a eliminar desde el final de cada cadena.

        Returns:
        pa.Table: Una nueva tabla PyArrow con la columna modificada.

        Example:
        >>> table = pa.Table.from_pydict({'Nombre': ['Alice', 'Bob', 'Charlie']})
        >>> modified_table = PyArrowTableModifier.remove_last_n_characters(table, 'Nombre', 2)
        >>> modified_table.column('Nombre').to_pylist()
        ['Al', 'B', 'Charl']
        """
        try:
            pattern = ".{" + str(n) + "}$"
            modified_column = pa.compute.replace_substring_regex(
                table[column_name], pattern=pattern, replacement=""
            )
            return table.set_column(
                table.schema.get_field_index(column_name), column_name, modified_column
            )
        except (ValueError, TypeError) as e:
            logger.error(f"Error al eliminar los últimos {n} caracteres: {e}")
        except pa.lib.ArrowInvalid as e:
            logger.error(f"Error de PyArrow: {e}")

    @Registro_tiempo
    @staticmethod
    def reemplazar_valores_con_diccionario_pa(
        tabla: pa.Table, nombre_columna: str, diccionario_de_mapeo: dict
    ) -> pa.Table:
        """
        Reemplaza valores en una columna específica de una tabla Arrow utilizando un diccionario de mapeo.

        Args:
            tabla (pa.Table): La tabla Arrow de entrada.
            nombre_columna (str): El nombre de la columna que se va a reemplazar.
            diccionario_de_mapeo (dict): Un diccionario que mapea valores antiguos a nuevos valores.

        Returns:
            pa.Table: Una nueva tabla Arrow con los valores de la columna especificada reemplazados según el diccionario_de_mapeo.

        Raises:
            ValueError: Si nombre_columna no existe en el esquema de la tabla.
            TypeError: Si diccionario_de_mapeo no es un diccionario o si tabla no es una tabla Arrow.

        Ejemplo:
            tabla = pa.table({'A': [1, 2, 3, 4, 5]})
            mapeo = {2: 10, 4: 20}
            tabla_resultante = reemplazar_valores_con_diccionario(tabla, 'A', mapeo)
        """
        try:
            # Verificar si la columna existe en el esquema de la tabla
            if nombre_columna not in tabla.column_names:
                raise ValueError(
                    f"Columna '{nombre_columna}' no encontrada en el esquema de la tabla."
                )

            keys_array = pa.array(list(diccionario_de_mapeo.keys()))
            values_array = pa.array(list(diccionario_de_mapeo.values()))

            # Crear una máscara booleana
            mascara = pa.compute.is_in(tabla[nombre_columna], value_set=keys_array)

            # Crear un array de valores reemplazados
            valores_reemplazados = pa.compute.take(
                values_array,
                pa.compute.index_in(tabla[nombre_columna], value_set=keys_array),
            )

            # Usar if_else para elegir entre el valor original y el valor reemplazado
            columna_final = pa.compute.if_else(
                mascara, valores_reemplazados, tabla[nombre_columna]
            )

            # Reemplazar la columna en la tabla
            return tabla.set_column(
                tabla.schema.get_field_index(nombre_columna),
                nombre_columna,
                columna_final,
            )

        except ValueError as ve:
            logger.error(ve)
            raise ve
        except TypeError as te:
            logger.error(te)
            raise te

    @staticmethod
    def Join_combine_pyarrow(
        table_left: pa.Table, table_right: pa.Table, join_key: str
    ):
        """
        Realiza un join entre dos tablas de PyArrow, combina los chunks de la tabla resultante,
        y ordena la tabla por una columna especificada.

        Parámetros:
        - table_left (pa.Table): La tabla de PyArrow del lado izquierdo para el join.
        - table_right (pa.Table): La tabla de PyArrow del lado derecho para el join.
        - join_key (str): El nombre de la columna en ambas tablas por la cual realizar el join.

        Retorna:
        - pa.Table: Una nueva tabla de PyArrow resultante del join, que ha sido combinada en chunks
                    y ordenada por la columna especificada en `join_key`.
        """
        # Realiza el join entre las dos tablas en la columna especificada
        joined_table = table_left.join(table_right, keys=join_key)

        # Combina los chunks de la tabla resultante para optimizar el rendimiento
        combined_table = joined_table.combine_chunks()

        return combined_table

    @staticmethod
    def Join_combine_pyarrow_two_keys(
        table_left: pa.Table,
        table_right: pa.Table,
        join_key_left: str,
        join_key_right: str,
    ):
        """
        Realiza un join entre dos tablas de PyArrow utilizando claves de unión potencialmente distintas para
        cada tabla, combina los chunks de la tabla resultante, y devuelve la tabla combinada.

        Parámetros:
        - table_left (pa.Table): La tabla de PyArrow del lado izquierdo para el join.
        - table_right (pa.Table): La tabla de PyArrow del lado derecho para el join.
        - join_key_left (str): El nombre de la columna en la tabla izquierda por la cual realizar el join.
        - join_key_right (str): El nombre de la columna en la tabla derecha por la cual realizar el join.

        Retorna:
        - pa.Table: Una nueva tabla de PyArrow resultante del join, que ha sido combinada en chunks.
        """
        # Realiza el join entre las dos tablas usando las claves especificadas
        joined_table = table_left.join(
            table_right, keys=[(join_key_left, join_key_right)]
        )

        # Combina los chunks de la tabla resultante para optimizar el rendimiento
        combined_table = joined_table.combine_chunks()

        return combined_table

    @staticmethod
    def sort_pyarrow_table(table, sort_key):
        """
        Ordena una tabla de PyArrow por una columna especificada.

        Parámetros:
        - table (pa.Table): La tabla de PyArrow a ordenar.
        - sort_key (str): El nombre de la columna por la cual ordenar la tabla.

        Retorna:
        - pa.Table: Una nueva tabla de PyArrow ordenada por la columna especificada.
        """
        # Verifica si la clave de ordenación es una lista de claves o una única clave
        # y prepara el argumento para el método sort_by adecuadamente
        if isinstance(sort_key, str):
            sort_criteria = sort_key
        elif isinstance(sort_key, list):
            sort_criteria = [
                (key, "ascending") for key in sort_key
            ]  # Asume orden ascendente por defecto
        else:
            raise ValueError(
                "sort_key debe ser una cadena de texto o una lista de cadenas de texto"
            )

        # Ordena la tabla por la columna especificada
        sorted_table = table.sort_by(sort_criteria)

        return sorted_table

    @Registro_tiempo
    @staticmethod
    def cambiar_tipo_dato_columnas_pa(
        tabla: pa.Table, columnas: list, nuevo_tipo
    ) -> pa.Table:
        """
        Cambia el tipo de dato de múltiples columnas en una tabla PyArrow.

        Parameters:
        tabla (pa.Table): La tabla PyArrow original.
        columnas (list): Lista de nombres de columnas cuyos tipos se van a cambiar.
        nuevo_tipo: El nuevo tipo de dato al que se van a cambiar las columnas.

        Returns:
        pa.Table: Una nueva tabla PyArrow con los tipos de columna actualizados.

        Example:
        >>> data = {'columna1': [1, 2, 3], 'columna2': [4, 5, 6], 'columna3': ['a', 'b', 'c']}
        >>> tabla_original = pa.Table.from_pydict(data)
        >>> tabla_modificada = cambiar_tipo_columnas(tabla_original, ['columna1', 'columna2'], pa.float64())
        >>> print(tabla_modificada.schema)
        """
        tabla_modificada = tabla

        for columna in columnas:
            if columna in tabla_modificada.column_names:
                logger.info(
                    f"Cambiando el tipo de la columna '{columna}' a {nuevo_tipo}."
                )
                columna_casteada = pa.compute.cast(
                    tabla_modificada[columna], nuevo_tipo
                )
                tabla_modificada = tabla_modificada.set_column(
                    tabla_modificada.schema.get_field_index(columna),
                    columna,
                    columna_casteada,
                )
            else:
                logger.error(f"La columna '{columna}' no se encuentra en la tabla.")
                raise KeyError(f"La columna '{columna}' no se encuentra en la tabla.")

        logger.success("Cambio de tipo de columna(s) completado con éxito.")
        return tabla_modificada

    class OpAritmeticasPa:
        @Registro_tiempo
        @staticmethod
        def sumar_columnas_pa(tabla: pa.Table, columnas_a_sumar: list) -> pa.Array:
            """
            Suma las columnas especificadas de una tabla PyArrow.

            Parameters:
            tabla (pa.Table): La tabla PyArrow a modificar.
            columnas_a_sumar (list of str): Lista de nombres de las columnas que se van a sumar.

            Returns:
            pa.Array: Un array con la suma de las columnas especificadas.

            Raises:
            KeyError: Si alguna de las columnas especificadas no existe en la tabla.
            TypeError: Si alguna de las columnas especificadas no es de tipo numérico.
            """
            try:
                suma = None
                for col in columnas_a_sumar:
                    if suma is None:
                        suma = tabla[col]
                    else:
                        suma = pa.compute.add(suma, tabla[col])

                return suma
            except Exception as e:
                logger.exception("Error al sumar las columnas: {}", e)
                raise

    @staticmethod
    def columns_to_dict_pa(
        table: pa.Table, key_column_name: str, value_column_name: str
    ):
        """
        Convierte dos columnas de una tabla PyArrow directamente en un diccionario,
        con controles de errores utilizando try-except para verificar la existencia de las columnas
        y el tipo de los nombres de las columnas.

        Args:
            table (pa.Table): La tabla de PyArrow que contiene las columnas.

            key_column_name (str): El nombre de la columna que se utilizará como clave del diccionario.

            value_column_name (str): El nombre de la columna que se utilizará como valor del diccionario.

        Returns:
            dict: Un diccionario construido directamente a partir de las columnas especificadas.

        throw:
        - ValueError: Si los nombres de las columnas no son strings o las columnas no existen en la tabla.
        """
        try:
            # Verifica que los nombres de las columnas sean strings
            if not isinstance(key_column_name, str) or not isinstance(
                value_column_name, str
            ):
                raise TypeError("Los nombres de las columnas deben ser strings.")

            # Intenta acceder a las columnas para verificar su existencia
            key_array = table.column(key_column_name)
            value_array = table.column(value_column_name)

        except KeyError:
            # Lanzado por table.column si el nombre de la columna no existe
            raise ValueError(
                f"Una o ambas columnas especificadas ('{key_column_name}' o '{value_column_name}') no existen en la tabla."
            )
        except TypeError as e:
            # Captura errores de tipo, como pasar un nombre de columna no string
            raise ValueError(str(e))

        # Construye el diccionario directamente si no hay errores
        result_dict = {
            key.as_py(): value.as_py() for key, value in zip(key_array, value_array)
        }

        return result_dict

    @Registro_tiempo
    @staticmethod
    def Seleccionar_columnas_pa(tabla: pa.Table, columnas: list) -> pa.Table:
        """Selecciona columnas específicas de una tabla PyArrow.

        Esta función toma una tabla PyArrow y una lista de nombres de columnas,
        y devuelve una nueva tabla que contiene solo las columnas especificadas.

        Parameters:
        tabla (pa.Table): La tabla PyArrow de la que seleccionar columnas.
        columnas (list): Lista de nombres de columnas a seleccionar.

        Returns:
        pa.Table: Una nueva tabla PyArrow que contiene solo las columnas seleccionadas.

        Raises:
        KeyError: Si alguna de las columnas especificadas no existe en la tabla."""

        try:
            columnas_existentes = set(tabla.column_names)
            columnas_solicitadas = set(columnas)

            if not columnas_solicitadas.issubset(columnas_existentes):
                columnas_faltantes = columnas_solicitadas - columnas_existentes
                raise KeyError(
                    f"Las columnas {columnas_faltantes} no existen en la tabla. No puede ser seleccionada."
                )

            return tabla.select(columnas)

        except KeyError as e:
            logger.error(f"Error al seleccionar columnas: {e}")
            raise

    @staticmethod
    def Eliminar_columnas_pa(
        tabla: pa.Table, columnas_a_eliminar: str | list
    ) -> pa.Table:
        """
        Elimina una o varias columnas de una tabla PyArrow.

        Args:
            tabla (pa.Table): La tabla de PyArrow de la cual eliminar las columnas.
            columnas_a_eliminar (Union[str, List[str]]): Nombre(s) de la(s) columna(s) a eliminar. Puede ser un string único o una lista de strings.

        Returns:
            pa.Table: Una nueva tabla de PyArrow sin las columnas especificadas.
        """
        # Convertir el nombre de la columna a eliminar en una lista si es un string único
        if isinstance(columnas_a_eliminar, str):
            columnas_a_eliminar = [columnas_a_eliminar]

        # Comprobar que todas las columnas a eliminar existen en la tabla
        columnas_existentes = tabla.column_names
        for columna in columnas_a_eliminar:
            if columna not in columnas_existentes:
                raise ValueError(f"La columna '{columna}' no existe en la tabla.")

        # Eliminar las columnas especificadas y retornar la nueva tabla
        tabla_modificada = tabla.drop(columnas_a_eliminar)
        return tabla_modificada
    

    @Registro_tiempo
    @staticmethod
    def Concatenar_tablas_pa(tablas: List[pa.Table]) -> pa.Table:
        """
        Concatena verticalmente las tablas de PyArrow en la lista.

        Args:
            tablas (list): Una lista de tablas de PyArrow.

        Returns:
            pa.Table: La tabla resultante después de la concatenación.

        Raises:
            TypeError: Si algún elemento de la lista no es una tabla de PyArrow.

        """
        try:
            # Verificar que cada elemento sea una tabla de PyArrow
            for tabla in tablas:
                if not isinstance(tabla, pa.Table):
                    raise TypeError("Cada elemento debe ser una tabla de PyArrow.")

            # Concatenar verticalmente las tablas
            tabla_concatenada = pa.concat_tables(tablas)

            # Registrar mensaje informativo
            logger.info("Tablas concatenadas exitosamente.")

            return tabla_concatenada

        except TypeError as e:
            # Registrar mensaje de error crítico
            logger.critical(f"Error al concatenar tablas: {e}")
            raise

    @staticmethod
    def Renombrar_cols_con_dict_pa(tabla: pa.Table, dict_renombrar: dict) -> pa.Table:
        """
        Renombra las columnas de una tabla PyArrow basándose en un diccionario proporcionado.

        Parameters:
        tabla (pa.Table): La tabla PyArrow que se va a modificar.
        dict_renombrar (dict): Un diccionario donde las claves son los nombres actuales de las columnas y los valores son los nuevos nombres.

        Returns:
        pa.Table: Una nueva tabla PyArrow con las columnas renombradas.

        Raises:
        KeyError: Si alguna clave del diccionario no corresponde a una columna en la tabla.

        Example:
        >>> data = {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
        >>> original_table = pa.Table.from_pydict(data)
        >>> rename_dict = {'col1': 'new_col1', 'col2': 'new_col2'}
        >>> renamed_table = renombrar_columnas_tabla(original_table, rename_dict)
        >>> print(renamed_table)
        """
        try:
            # Verificar que todas las claves del diccionario son nombres de columnas existentes
            columnas_existentes = set(tabla.column_names)
            columnas_renombrar = set(dict_renombrar.keys())
            if not columnas_renombrar.issubset(columnas_existentes):
                columnas_faltantes = columnas_renombrar - columnas_existentes
                raise KeyError(
                    f"Las siguientes columnas a renombrar no existen en la tabla: {columnas_faltantes}"
                )

            # Crear la lista de nuevos nombres manteniendo el orden original
            new_column_names = [
                dict_renombrar.get(name, name) for name in tabla.column_names
            ]

            # Renombrar las columnas de la tabla
            table_renamed = tabla.rename_columns(new_column_names)
            logger.info("Las columnas han sido renombradas exitosamente.")

            return table_renamed

        except KeyError as e:
            logger.error(f"Error al renombrar las columnas: {e}")
            raise
        except Exception as e:
            logger.exception(f"Error inesperado al renombrar las columnas: {e}")
            raise

    @staticmethod
    def obtener_tipo_pa(tipo_str):
        """
        Devuelve el tipo de dato de PyArrow correspondiente al string proporcionado.

        Args:
            - tipo_str (str): Una cadena que describe el tipo de dato.
            Los valores admitidos son 'string', 'int8', 'int16', 'int32', 'int64',
            'uint8', 'uint16', 'uint32', 'uint64', 'float', 'double', 'bool', y otros
            soportados por PyArrow.

        Retruns:
            - pyarrow.DataType: El tipo de dato de PyArrow correspondiente a la descripción.

        Lanza:
        - ValueError: Si 'tipo_str' no corresponde a un tipo de dato soportado.
        """
        tipos = {
            "string": pa.string(),
            "int8": pa.int8(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "uint8": pa.uint8(),
            "uint16": pa.uint16(),
            "uint32": pa.uint32(),
            "uint64": pa.uint64(),
            "float": pa.float32(),
            "double": pa.float64(),
            "bool": pa.bool_(),
            # Aquí puedes añadir más tipos de datos de PyArrow según sea necesario
        }

        try:
            return tipos[tipo_str]
        except KeyError:
            raise ValueError(f"Tipo de dato '{tipo_str}' no soportado.")

    @staticmethod
    def crear_columna_constante_pa(
        num_filas: int, valor_constante, tipo_dato: pa.DataType
    ) -> pa.Array:
        """
        Crea un array de PyArrow con un valor constante.

        Parámetros:
        num_filas (int): Número de filas (elementos) en el array.
        valor_constante: El valor constante para llenar el array. Puede ser de cualquier tipo.
        tipo_dato (pa.DataType): Tipo de dato de PyArrow para el array. Debe ser un tipo de dato válido de PyArrow.

        Devoluciones:
        pa.Array: Un array de PyArrow lleno con el valor constante.

        Excepa.computeiones:
        TypeError: Si el tipo de dato no es compatible con el valor constante.
        ValueError: Si el número de filas es no positivo.

        Ejemplo:
        >>> array_constante = crear_columna_constante(5, 100, pa.int64())
        >>> print(array_constante)
        """
        try:
            if num_filas <= 0:
                raise ValueError("El número de filas debe ser positivo.")

            return pa.array([valor_constante] * num_filas, type=tipo_dato)
        except TypeError as error_de_tipo:
            logger.error(
                f"Se produjo un error de tipo al crear el array: {error_de_tipo}"
            )
            raise
        except ValueError as error_de_valor:
            logger.error(f"Se produjo un error de valor: {error_de_valor}")
            raise
        except Exception as error_general:
            logger.exception(f"Se produjo un error inesperado: {error_general}")
            raise

    @staticmethod
    def agregar_nueva_columna_pa(
        tabla: pa.Table, array_resultado: pa.Array, nombre_nueva_columna: str
    ) -> pa.Table:
        """
        Agrega un array como una nueva columna a una tabla PyArrow.

        Parameters:
        tabla (pa.Table): La tabla PyArrow original.
        array_resultado (pa.Array): Array que se agregará como nueva columna.
        nombre_nueva_columna (str): El nombre de la nueva columna.

        Returns:
        pa.Table: Una nueva tabla PyArrow con la nueva columna agregada.

        Example:
        >>> data = {'columna1': [1, 2, 3], 'columna2': [4, 5, 6]}
        >>> tabla_original = pa.Table.from_pydict(data)
        >>> suma_columnas = sumar_columnas(tabla_original, ['columna1', 'columna2'])
        >>> tabla_modificada = agregar_nueva_columna(tabla_original, suma_columnas, 'suma_total')
        >>> print(tabla_modificada)
        """
        try:
            logger.success(f"Columna {nombre_nueva_columna} agregada exitosamente.")
            return tabla.append_column(nombre_nueva_columna, array_resultado)
        except Exception as e:
            logger.exception("Error al agregar la nueva columna: {}", e)
            raise

    @Registro_tiempo
    @staticmethod
    def Group_and_sum_columns_pa(
        table, group_column: list | str, sum_columns: list
    ) -> pa.Table:
        """
        Agrupa una tabla PyArrow por una columna y suma las columnas numéricas especificadas.

        Args:
            table (pa.Table): La tabla PyArrow a agrupar.
            group_column Union [list, str]: El nombre de la columna por la cual agrupar,
            o la lista de columnas por las cuales se quiere agrupar.
            sum_columns (list): Lista de nombres de columnas numéricas a sumar.

        Returns:
            pa.Table: Tabla agrupada con las sumas calculadas.
        """
        # Agrupar por la columna deseada y sumar las columnas numéricas
        grouped_table = table.group_by(group_column).aggregate(
            [(col, "sum") for col in sum_columns]
        )

        return grouped_table

    @Registro_tiempo
    @staticmethod
    def duplicar_columna_n_veces_pa(
        tabla: pa.Table, nombre_columna: str, nombres_nuevas_columnas: list
    ) -> pa.Table:
        """
        Duplica una columna específica de una tabla PyArrow y agrega las duplicaciones con  nuevos nombres.

        Parameters:
        tabla (pa.Table): La tabla PyArrow a modificar.
        nombre_columna (str): El nombre de la columna que se va a duplicar.
        nombres_nuevas_columnas (list of str): Lista de nombres para las nuevas columnas    duplicadas.

        Returns:
        pa.Table: Una nueva tabla PyArrow con las columnas duplicadas agregadas.

        Raises:
        KeyError: Si la columna especificada no existe en la tabla.
        ValueError: Si no se proporcionan nombres para las columnas duplicadas.
        ValueError: Si el número de nombres proporcionados no es adecuado para el número de     duplicaciones.

        Example:
        >>> data = {'columna1': [1, 2, 3], 'columna2': [4, 5, 6]}
        >>> tabla_original = pa.Table.from_pydict(data)
        >>> nombres_nuevas_columnas = ['columna1_copia1', 'columna1_copia2']
        >>> tabla_modificada = duplicar_columna(tabla_original, 'columna1',     nombres_nuevas_columnas)
        >>> print(tabla_modificada)
        """
        if nombre_columna not in tabla.column_names:
            logger.error(f"La columna '{nombre_columna}' no se encuentra en la tabla.")
            raise KeyError(
                f"La columna '{nombre_columna}' no se encuentra en la tabla."
            )

        if len(nombres_nuevas_columnas) == 0:
            logger.error("No se proporcionaron nombres para las columnas duplicadas.")
            raise ValueError(
                "Es necesario proporcionar nombres para las columnas duplicadas."
            )

        try:
            for nuevo_nombre in nombres_nuevas_columnas:
                tabla = tabla.append_column(nuevo_nombre, tabla[nombre_columna])
            logger.success("Columna duplicada y agregada con éxito.")
            return tabla
        except Exception as e:
            logger.exception("Error al duplicar la columna: {}", e)
            raise

    class ColumnDependencyReplacer:
        def __init__(
            self,
            tabla: pa.Table,
            dict_mapeo: dict,
            columna_base: str,
            columna_reemplazo: str,
        ):
            """
            Inicializa la instancia de ColumnDependencyReplacer.

            Args:
                tabla (pa.Table): La tabla de PyArrow donde se realizarán los reemplazos.

                dict_mapeo (dict): Diccionario usado para mapear los valores de la columna_base a nuevos valores.

                columna_base (str): Nombre de la columna cuyos valores se mapearán según dict_mapeo.

                columna_reemplazo (str): Nombre de la columna donde se colocarán los valores reemplazados.
            """
            if not isinstance(tabla, pa.Table):
                raise ValueError(
                    "El argumento 'tabla' debe ser una instancia de pa.Table."
                )

            self.tabla = tabla
            self.dict_mapeo = dict_mapeo
            self.columna_base = columna_base
            self.columna_reemplazo = columna_reemplazo

        def dict_a_pyarrow_arrays(self):
            """
            Convierte las claves y valores del diccionario de mapeo en arrays de PyArrow.

            Returns:
                tuple: Dos arrays de PyArrow, uno con las claves y otro con los valores del diccionario.
            """
            keys_array = pa.array(list(self.dict_mapeo.keys()))
            values_array = pa.array(list(self.dict_mapeo.values()))

            return keys_array, values_array

        def obtener_cols_base_replace_table_pa(self):
            """Obtiene una columna especifica de una tabla de pyarrow.

            Returns:
                Retorna una tupla con las columnas requeridas.
            """
            # Obtiene la columna base de la tabla.
            source_column = self.tabla.column(self.columna_base)

            # Obtiene la columna a reemplazar.
            replace_column = self.tabla.column(self.columna_reemplazo)

            return source_column, replace_column

        @Registro_tiempo
        def get_replaced_values(self):
            """
            Genera un array de PyArrow con los valores reemplazados o originales según el mapeo del diccionario.

            Returns:
                pa.Array: Array de PyArrow con los valores reemplazados o originales para la columna especificada.
            """
            # Pasamos el arrays de llaves y valores.
            keys_array, values_array = self.dict_a_pyarrow_arrays()

            # Pasamos las columnas base y a reemplazar
            source_column, replace_column = self.obtener_cols_base_replace_table_pa()

            try:

                # Crea una máscara booleana para saber qué valores están en el diccionario.
                mask = pa.compute.is_in(source_column, value_set=keys_array)

                # Mapea los índices donde existen claves del diccionario en la columna base.
                mapped_indices = pa.compute.index_in(
                    source_column, value_set=keys_array
                )

                # Utiliza la máscara para seleccionar los nuevos valores solo donde la máscara es True.
                new_values = pa.compute.take(values_array, mapped_indices)

                # Reemplaza los valores en la columna de reemplazo manteniendo los originales donde la máscara es False.
                replaced_values = pa.compute.if_else(mask, new_values, replace_column)

                return replaced_values

            except KeyError as e:
                logger.error(f"Error al acceder a la columna: {e}")
                raise
            except Exception as e:
                logger.error(f"Error al reemplazar los valores: {e}")
                raise

        def reemplazar_columna_pa(self):
            """
            Reemplaza la columna especificada en la tabla de PyArrow con los valores mapeados.

            Returns:
                pa.Table: Nueva tabla de PyArrow con la columna reemplazada.
            """
            replaced_values = self.get_replaced_values()
            try:
                if self.columna_reemplazo in self.tabla.column_names:
                    indice_columna_reemplazo = self.tabla.schema.get_field_index(
                        self.columna_reemplazo
                    )
                    tabla_modificada = self.tabla.set_column(
                        indice_columna_reemplazo,
                        self.columna_reemplazo,
                        replaced_values,
                    )
                else:
                    tabla_modificada = self.tabla.append_column(
                        self.columna_reemplazo, replaced_values
                    )
                logger.info(
                    f"Columna '{self.columna_reemplazo}' reemplazada exitosamente."
                )
                return tabla_modificada
            except Exception as e:
                logger.error(f"Error al reemplazar la columna: {e}")
                raise

    class TableColumnConcatenator:

        @staticmethod
        def concatenar_cols_seleccionadas(table: pa.Table, column_names: list):
            try:
                selected_cols = (
                    PyArrowColumnTransformer.TableColumnConcatenator.select_columns(
                        table, column_names
                    )
                )
                string_columns = (
                    PyArrowColumnTransformer.TableColumnConcatenator.convert_to_strings(
                        selected_cols
                    )
                )
                concatenated_column = PyArrowColumnTransformer.TableColumnConcatenator.concatenate_strings(
                    string_columns
                )
            except Exception as e:
                logger.error(
                    f"Error en la función 'concatenar_cols_seleccionadas': {e}"
                )
                raise
            return concatenated_column

        @staticmethod
        def select_columns(table, column_names):
            try:
                selected_columns = [table[col] for col in column_names]
            except KeyError as e:
                logger.error(f"Error al seleccionar columnas: {e}")
                raise ValueError(
                    "Una o más columnas especificadas no existen en la tabla."
                )
            return selected_columns

        @staticmethod
        def convert_to_strings(columns):
            try:
                cols_convertidas = [
                    pa.compute.cast(col, pa.string()) for col in columns
                ]
            except (
                pa.ArrowInvalid,
                pa.ArrowNotImplementedError,
                pa.ArrowInvalidValueError,
            ) as e:
                logger.error(f"Error al convertir columnas a tipo string: {e}")
                raise TypeError(
                    "No se pudo convertir una o más columnas a tipo de datos string."
                )
            return cols_convertidas

        @staticmethod
        def concatenate_strings(columns):
            try:
                concatenated_column = pa.compute.binary_join_element_wise(*columns, "")
            except ValueError as e:
                logger.error(f"Error al concatenar columnas: {e}")
                raise ValueError(
                    "Las columnas no tienen la misma longitud y no se puedenconcatenar."
                )
            return concatenated_column

    @Registro_tiempo
    def Transform_pyarrow_a_pandas(table: pa.Table) -> pd.DataFrame:
        """
        Función que toma una tabla de PyArrow y la transforma en un DataFrame de Pandas.

        Args:
          table (pa.Table): Tabla de PyArrow a ser transformada.

        Returns:
          pd.DataFrame: DataFrame de Pandas resultante de la transformación.
        """
        try:
            # Verificar si la entrada es una tabla de PyArrow
            if not isinstance(table, pa.Table):
                raise TypeError("El argumento 'table' debe ser una tabla de PyArrow.")
            else:
                df = table.to_pandas()
        except Exception as e:
            raise ValueError(
                f"Error al convertir la tabla PyArrow a DataFrame: {e}"
            ) from e

        return df

    def Cambiar_tipo_dato_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, type_data: type
    ) -> pd.DataFrame:
        """
        Función que toma un DataFrame, una lista de sus columnas para hacer un cambio en el tipo de dato de las mismas.

        Args:
            base (pd.DataFrame): DataFrame que es la base del cambio.
            list_columns (list): Columnas a modificar su tipo de dato.
            type_data (type): Tipo de dato al que se cambiarán las columnas (ejemplo: str, int, float).

        Returns:
            pd.DataFrame: Copia del DataFrame con los cambios.
        """
        try:
            # Verificar que el DataFrame tenga las columnas especificadas
            for columna in list_columns:
                if columna not in base.columns:
                    raise KeyError(f"La columna '{columna}' no existe en el DataFrame.")

            # Cambiar el tipo de dato de las columnas
            base_copy = (
                base.copy()
            )  # Crear una copia para evitar problemas de SettingWithCopyWarning

            for columna in list_columns:
                if type_data == int:
                    # Convertir primero a numérico (por si viene como string tipo '1583166.0'), luego a int
                    base_copy[columna] = pd.to_numeric(base_copy[columna], errors="coerce").fillna(0).astype(int)
                else:
                    base_copy[columna] = base_copy[columna].astype(type_data)

            return base_copy

        except Exception as e:
            logger.critical(f"Error en Cambiar_tipo_dato_multiples_columnas: {e}")
            raise


    @staticmethod
    def Group_by_pa_whit_pd(
        tabla: pa.Table, group_col: str | list, sum_cols: str | list
    ):
        """
        Agrupa y suma columnas específicas en una tabla de PyArrow usando Pandas.

        Esta función convierte una tabla de PyArrow a un DataFrame de Pandas,
        realiza una operación de agrupación por una columna dada y suma las columnas
        especificadas. Luego, convierte el DataFrame de Pandas resultante de vuelta
        a una tabla de PyArrow y la retorna.

        Parameters:
        - table (pa.Table): La tabla de PyArrow que se va a procesar.
        - group_col (str|list): El nombre de la columna o columnas por la cuales se agruparán los datos.
        - sum_cols (list of str): Una lista de nombres de columnas cuyos valores se sumarán.

        Returns:
        - pa.Table: Una nueva tabla de PyArrow con los datos agrupados y sumados."""

        # Convertir tabla PyArrow a DataFrame de Pandas
        df = tabla.to_pandas()

        # Realizar la agrupación y sumar las columnas especificadas.
        df_agrupado = PandasBaseTransformer.Group_by_and_sum_cols_pd(
            df=df, group_col=group_col, sum_col=sum_cols
        )

        df_agrupado = PandasBaseTransformer.Cambiar_tipo_dato_multiples_columnas_pd(
            df_agrupado, list_columns=df_agrupado.columns, type_data=str
        )

        # Convertir el DataFrame de Pandas de vuelta a una tabla de PyArrow
        table_resultado = pa.Table.from_pandas(df_agrupado)

        return table_resultado

    def LLenar_nulos_con_otra_columna_pa(tabla, col_fuente, col_destino):
        """
        Llena los valores nulos en una columna de una tabla de PyArrow con los valores de otra columna.

        Parámetros:
        - tabla (pa.Table): La tabla de PyArrow que se va a procesar.
        - col_fuente (str): El nombre de la columna cuyos valores se usarán para rellenar nulos.
        - col_destino (str): El nombre de la columna donde se quieren rellenar los nulos.

        Retorna:
        - pa.Table: Una nueva tabla de PyArrow con los valores nulos reemplazados.
        """
        columna_fuente = tabla[col_fuente]
        columna_destino = tabla[col_destino]

        # Utilizar `coalesce` para llenar los valores nulos en la columna destino con los valores de la columna fuente
        columna_destino_llenada = pa.compute.coalesce(columna_destino, columna_fuente)

    class PyArrowTablefilter:

        def __init__(self, tabla):
            if not isinstance(tabla, pa.Table):
                raise ValueError(
                    "El argumento 'tabla' debe ser una instancia de pa.Table."
                )
            self.tabla = tabla

        def Mascara_is_in_pa(self, columna: str, valores: list):
            """
            Crea una máscara booleana basada en si los valores de una columna específica están  contenidos en una lista dada.

            Esta función genera una máscara booleana para filtrar las filas de una tabla PyArrow,   donde cada elemento de la máscara
            es True si el valor correspondiente en la columna especificada está presente en la  lista de 'valores' proporcionada,
            y False en caso contrario. Esta operación es útil para seleccionar subconjuntos de  datos basados en múltiples criterios.

            Args:
                Columna (str): Nombre de la columna en la tabla PyArrow sobre la cual se desea aplicar el filtro.

                Valores (lsit): Lista de valores que se buscan en la columna especificada. Cada    elemento en esta lista se compara con los valores de la columna, y si hay una coincidencia, la máscara    en esa posición se establece en True.

            Return:
                Un pyarrow.Array de tipo booleano que representa la máscara de filtrado.   Puede usarse para filtrar la tabla original y seleccionar solo las filas que cumplen con el criterio  especificado.
            """

            if not isinstance(valores, list):
                raise ValueError("El argumento 'valores' debe ser una lista.")
            try:
                mask = pa.compute.is_in(
                    self.tabla[columna], value_set=pa.array(valores)
                )
                return mask
            except KeyError:
                raise ValueError(f"La columna '{columna}' no existe en la tabla.")
            except Exception as e:
                raise RuntimeError(f"Error al crear la máscara: {e}")

        @staticmethod
        def Invertir_mascara_pa(mask):
            """
            Invierte una máscara booleana dada.

            Esta función toma una máscara booleana como entrada y devuelve una nueva máscara booleana donde todos los valores True se convierten en False, y viceversa. Esta operación es útil para cambiar el criterio de filtrado de los datos en operaciones que involucran tablas
            o arrays de PyArrow.

            Args:
                mask (pyarrow.Array de tipo booleano). La máscara booleana a ser invertida.

            Return:
                inverted_mask (pyarrow.Array de tipo booleano). La máscara booleana invertida.
            """
            # Invertir la mascara.
            if not isinstance(mask, pa.lib.ChunkedArray):
                raise ValueError("El argumento 'mask' debe ser de tipo pa.Array.")
            try:
                inverted_mask = pa.compute.invert(mask)
                return inverted_mask
            except Exception as e:
                raise RuntimeError(f"Error al invertir la máscara: {e}")

        def mask_equivalente_pa(self, columna, valor):
            """
            Crea una máscara booleana que indica si los valores en una columna específica no son iguales a un valor dado.

            Esta función utiliza la operación de comparación 'equal' de PyArrow para comparar   cada elemento en la columna especificada
            con el valor proporcionado. Retorna una máscara booleana donde cada posición    corresponde a la comparación de un elemento
            de la columna, siendo True si el elemento es igual al valor y False en caso  contrario.

            Args:
                columna (str): Nombre de la columna en la tabla PyArrow sobre la cual se realizará la comparación.

                valor (Union[int, str]) : Valor con el que se compararán los elementos de la columna.

            Return:
                mask (pa.Array) Máscara booleana de PyArrow que representa el resultado de la comparación.
            """
            try:
                mask = pa.compute.equal(self.tabla[columna], valor)
                return mask
            except KeyError:
                raise ValueError(f"La columna '{columna}' no existe en la tabla.")
            except Exception as e:
                raise RuntimeError(f"Error al crear la máscara de equivalencia: {e}")

        def mask_no_equivalente_pa(self, columna: str, valor: str) -> pa.Array:
            """
            Crea una máscara booleana que indica si los valores en una columna específica no son iguales a un valor dado.

            Esta función utiliza la operación de comparación 'not_equal' de PyArrow para comparar   cada elemento en la columna especificada
            con el valor proporcionado. Retorna una máscara booleana donde cada posición    corresponde a la comparación de un elemento
            de la columna, siendo True si el elemento no es igual al valor y False en caso  contrario.

            Args:
                columna (str): Nombre de la columna en la tabla PyArrow sobre la cual se realizará la comparación.

                valor (str) : Valor con el que se compararán los elementos de la columna.

            Return:
                mask (pa.Array) Máscara booleana de PyArrow que representa el resultado de la comparación.
            """
            try:
                mask = pa.compute.not_equal(self.tabla[columna], valor)
                return mask
            except KeyError:
                raise ValueError(f"La columna '{columna}' no existe en la tabla.")
            except Exception as e:
                raise RuntimeError(f"Error al crear la máscara de no equivalencia: {e}")

        def mask_filter_null_rows(self, column_name: str) -> pa.Table:
            """
            Filtra las filas de una tabla de PyArrow basándose en los valores no nulos de una columna específica.

            Parámetros:
            - column_name (str): El nombre de la columna a evaluar para el filtrado.

            Retorna:
            - mask_non_null (pa.Array): Una mascara para tabla de PyArrow que contiene solo filas con valores no nulos en la columna especificada.
            """
            # Crear un filtro de booleanos que sea True para filas nulas en la columna especificada
            mask_non_null = pa.compute.is_null(self.tabla[column_name])

            return mask_non_null

        def mask_filter_not_null_rows(self, column_name: str) -> pa.Table:
            """
            Filtra las filas de una tabla de PyArrow basándose en los valores no nulos de una columna específica.

            Parámetros:
            - column_name (str): El nombre de la columna a evaluar para el filtrado.

            Retorna:
            - mask_non_null (pa.Array): Una mascara para tabla de PyArrow que contiene solo filas con valores no nulos en la columna especificada.
            """
            # Crear un filtro de booleanos que sea True para filas nulas en la columna especificada
            mask_null = pa.compute.is_null(self.tabla[column_name])

            mask_non_null = (
                PyArrowColumnTransformer.PyArrowTablefilter.Invertir_mascara_pa(
                    mask=mask_null
                )
            )

            return mask_non_null

        def mask_filter_rows_starting_with(
            self, columna: str, start_string: str
        ) -> pa.Table:
            """
            Filtra las filas de una tabla de PyArrow basándose en si los valores de una columna específica
            comienzan con una cadena de caracteres determinada.

            Args:
                - column (str): El nombre de la columna a evaluar para el filtrado.
                - start_string (str): La cadena de caracteres con la que deben comenzar los valores de la columna.

            Returns:
                - mask_start_with_condition (pa.Array): Una mascara de filtrado que contiene los valores que cumple con la condición
            """

            # Crear una expresión que evalúe si los valores de la columna comienzan con la cadena especificada
            columna = self.tabla.column(columna)

            mask_start_with_condition = pa.compute.starts_with(columna, start_string)

            return mask_start_with_condition

        def mask_filter_filas_sin_letras(self, columna: str) -> pa.Array:
            """Crea una máscara booleana para una tabla de PyArrow donde la máscara es True si la columna especificada no contiene letras y False en caso contrario.

            Parámetros:
            - tabla (pa.Table): La tabla de PyArrow sobre la cual se aplicará la máscara.
            - nombre_columna (str): El nombre de la columna en la que se verificarán valores sin letras.

            Retorna:
            - pa.Array: Una array de PyArrow de tipo booleano que representa la máscara de filtrado.
            """

            # La expresión regular para coincidir con strings que no contienen letras
            regex_pattern = "^[^a-zA-Z]*$"

            # Crear la máscara utilizando match_substring_regex
            mask = pa.compute.match_substring_regex(self.tabla[columna], regex_pattern)
            return mask

        @staticmethod
        def Combinar_mask_and_pa(*masks) -> pa.Array:
            """
            Combina múltiples condiciones/máscaras booleanas utilizando el operador lógico AND.

            Args:
                mask (pa.Array()) :Máscaras booleanas individuales como argumentos variables.

            Return:
                condicion_combinada (pa.Array()) : Una máscara booleana combinada que es la operación AND de todas las máscaras de entrada.
            """
            if not masks:
                raise ValueError("Se debe proporcionar al menos una máscara.")
            try:
                condicion_combinada = masks[0]
                for mascara in masks[1:]:
                    condicion_combinada = pa.compute.and_(condicion_combinada, mascara)
                return condicion_combinada
            except Exception as e:
                raise RuntimeError(f"Error al combinar máscaras con AND: {e}")

        def Filtrar_tabla_pa(self, mask):
            """Filtra la tabla de acuerdo a una Mascara que actua como condicion de filtrado
            Args:
                tabla (pa.Table): tabla de pyarrow a filtrar.
                mask : condición de filtrado

            Returns:
                tabla_filtrada (pa.Table)  Tabla de pyarrow a la que se le aplicó el filtro
                correspondiente.
            """
            if not isinstance(mask, pa.lib.ChunkedArray):
                raise ValueError("El argumento 'mask' debe ser de tipo pa.Array.")
            try:
                tabla_filtrada = self.tabla.filter(mask)
                return tabla_filtrada
            except Exception as e:
                raise RuntimeError(f"Error al filtrar la tabla: {e}")

    @staticmethod
    def LLenar_valores_nulos_con_otra_columna_pa(
        tabla: pa.Table, col_fuente: str, col_destino
    ):
        """
        Llena los valores nulos en una columna de una tabla de PyArrow con los valores de otra columna
        y mantiene el nombre original de la columna destino.
        Parámetros:
        - tabla (pa.Table): La tabla de PyArrow que se va a procesar.
        - col_fuente (str): El nombre de la columna cuyos valores se usarán para rellenar nulos.
        - col_destino (str): El nombre de la columna donde se quieren rellenar los nulos.
        Retorna:
        - pa.Table: Una nueva tabla de PyArrow con los valores nulos reemplazados en la columna destino.
        """
        # Utilizar `coalesce` para llenar los valores nulos en la columna destino con los valores de la columna fuente
        columna_fuente = tabla.column(col_fuente)
        columna_destino = tabla.column(col_destino)
        columna_destino_llenada = pa.compute.coalesce(columna_destino, columna_fuente)

        # Construir una nueva tabla con las columnas originales, reemplazando la columna destino
        columnas = [
            (
                tabla.column(i)
                if tabla.field(i).name != col_destino
                else columna_destino_llenada
            )
            for i in range(tabla.num_columns)
        ]

        tabla_nueva = pa.Table.from_arrays(columnas, schema=tabla.schema)
        return tabla_nueva


import pandas as pd
import pyarrow as pa
from General_Functions import Registro_tiempo


class PandasBaseTransformer:

    @staticmethod
    def Remove_duplicates(df):
        """
        Elimina filas duplicadas de un DataFrame y restablece el índice.

        Args:
            df (pandas.DataFrame): El DataFrame del cual eliminar duplicados y resetear el índice.

        Returns:
            pandas.DataFrame: Un nuevo DataFrame sin duplicados y con el índice reseteado.
        """
        # Eliminar duplicados
        df = df.drop_duplicates()

        # Restablecer el índice
        df = df.reset_index(drop=True)

        return df

    @staticmethod
    def Cambiar_tipo_dato_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, type_data: type
    ) -> pd.DataFrame:
        """
        Función que toma un DataFrame, una lista de sus columnas para hacer un cambio en el tipo de dato de las mismas.

        Args:
            base (pd.DataFrame): DataFrame que es la base del cambio.
            list_columns (list): Columnas a modificar su tipo de dato.
            type_data (type): Tipo de dato al que se cambiarán las columnas (ejemplo: str, int, float).

        Returns:
            pd.DataFrame: Copia del DataFrame con los cambios.
        """
        try:
            # Verificar que el DataFrame tenga las columnas especificadas
            for columna in list_columns:
                if columna not in base.columns:
                    raise KeyError(f"La columna '{columna}' no existe en el DataFrame.")

            # Cambiar el tipo de dato de las columnas
            base_copy = (
                base.copy()
            )  # Crear una copia para evitar problemas de SettingWithCopyWarning

            for columna in list_columns:
                if type_data == int:
                    # Convertir primero a numérico (por si viene como string tipo '1583166.0'), luego a int
                    base_copy[columna] = pd.to_numeric(base_copy[columna], errors="coerce").fillna(0).astype(int)
                else:
                    base_copy[columna] = base_copy[columna].astype(type_data)

            return base_copy

        except Exception as e:
            logger.critical(f"Error en Cambiar_tipo_dato_multiples_columnas: {e}")
            raise


    @staticmethod
    def Renombrar_columnas_con_diccionario(
        base: pd.DataFrame, cols_to_rename: dict
    ) -> pd.DataFrame:
        """Funcion que toma un diccionario con keys ( nombres actuales ) y values (nuevos nombres) para remplazar nombres de columnas en un dataframe.
        Args:
            base: dataframe al cual se le harán los remplazos
            cols_to_rename: diccionario con nombres antiguos y nuevos
        Result:
            base_renombrada: Base con las columnas renombradas.
        """
        base_renombrada = None

        try:
            base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
            logger.success("Proceso de renombrar columnas satisfactorio: ")
        except Exception:
            logger.critical("Proceso de renombrar columnas fallido.")
            raise Exception

        return base_renombrada

    def concatenate_dataframes(dataframes: list) -> pd.DataFrame:
        """
        Concatena dos DataFrames de pandas.

        Args:
            df1 (pd.DataFrame): Primer DataFrame.
            df2 (pd.DataFrame): Segundo DataFrame.

        Returns:
            pd.DataFrame: DataFrame resultante después de la concatenación.
        """
        try:
            # Concatenar por filas (verticalmente)
            df_concatenado = pd.concat(dataframes, ignore_index=True)

            # Registrar un mensaje informativo
            logger.info("DataFrames concatenados exitosamente.")

            return df_concatenado
        except Exception as e:
            # Registrar un mensaje de error
            logger.error(f"Error al concatenar DataFrames: {str(e)}")
            return None

    @staticmethod
    @Registro_tiempo
    def Group_by_and_sum_cols_pd(df=pd.DataFrame, group_col=list, sum_col=list):
        """
        Agrupa un DataFrame por una columna y calcula la suma de otra columna.

        Args:
            df (pandas.DataFrame): El DataFrame que se va a agrupar y sumar.
            group_col (list or str): El nombre de la columna o lista de nombres de columnas por la cual se va a agrupar.
            sum_col (list or str): El nombre de la columna o lista de nombres de columnas que se va a sumar.

        Returns:
            pandas.DataFrame: El DataFrame con las filas agrupadas y la suma calculada.
        """

        try:
            if isinstance(group_col, str):
                group_col = [group_col]

            if isinstance(sum_col, str):
                sum_col = [sum_col]

            result_df = df.groupby(group_col, as_index=False)[sum_col].sum()

            # Registro de éxito
            logger.info(f"Agrupación y suma realizadas con éxito en las columnas.")

        except Exception as e:
            # Registro de error crítico
            logger.critical(
                f"Error al realizar la agrupación y suma en las columnas. {e}"
            )
            result_df = None

        return result_df

    @Registro_tiempo
    def Transform_dfs_pandas_a_pyarrow(df: pd.DataFrame) -> pa.Table:
        """
        Función que toma un DataFrame de pandas y lo transforma en una tabla de PyArrow,
        asegurándose de que no se transfiera ningún MultiIndex como una columna.

        Args:
        df (pd.DataFrame): DataFrame de pandas a ser transformado.

        Returns:
        pa.Table: Tabla bidimensional de datos de PyArrow.
        """
        try:
            # Verificar si la entrada es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Resetear el índice si es MultiIndex para evitar columnas adicionales en PyArrow
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Convertir el DataFrame de pandas a una tabla de PyArrow
            table = pa.Table.from_pandas(df, preserve_index=False)
        except Exception as e:
            raise ValueError(f"Error al convertir el DataFrame a PyArrow: {e}") from e

        return table
    
    @staticmethod
    def eliminar_last_caracteres(df, columnas):
        """
        Elimina los últimos dos caracteres de una o varias columnas en un DataFrame.

        Parámetros:
        df (pd.DataFrame): DataFrame que contiene las columnas a modificar.
        columnas (str o list): Nombre de la columna o lista de nombres de columnas en las que se eliminarán los últimos dos caracteres.

        Retorna:
        pd.DataFrame: DataFrame con las columnas modificadas.
        """
        if isinstance(columnas, str):  # Si es una sola columna, convertir a lista
            columnas = [columnas]

        columnas_invalidas = [col for col in columnas if col not in df.columns]
        
        if columnas_invalidas:
            raise ValueError(f"Las siguientes columnas no existen en el DataFrame: {columnas_invalidas}")

        df[columnas] = df[columnas].astype(str).apply(lambda x: x.str[:-2])
        
        return df
    
    @staticmethod
    def Seleccionar_columnas_pd(
        df: pd.DataFrame, cols_elegidas: List[str]
    ) -> pd.DataFrame | None:
        """
        Filtra y retorna las columnas especificadas del DataFrame.

        Parámetros:
        dataframe (pd.DataFrame): DataFrame del cual se filtrarán las columnas.
        cols_elegidas (list): Lista de nombres de las columnas a incluir en el DataFrame filtrado.

        Retorna:
        pd.DataFrame: DataFrame con las columnas filtradas.
        """
        try:
            # Verificar si dataframe es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    "El argumento 'dataframe' debe ser un DataFrame de pandas."
                )

            # Filtrar las columnas especificadas
            df_filtrado = df[cols_elegidas]

            # Registrar el proceso
            logger.info(f"Columnas filtradas: {', '.join(cols_elegidas)}")

            return df_filtrado

        except KeyError as ke:
            logger.critical(
                f"Error: Columnas especificadas no encontradas en el DataFrame: {str(ke)}",
                exc_info=True,
            )
        except Exception as e:
            logger.critical(
                f"Error inesperado al filtrar columnas: {str(e)}", exc_info=True
            )
            

    @Registro_tiempo
    def Crear_diccionario_desde_dataframe(
        df: pd.DataFrame, col_clave: str, col_valor: str
    ) -> dict:
        """
        Crea un diccionario a partir de un DataFrame utilizando dos columnas especificadas.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.
            col_clave (str): El nombre de la columna que se utilizará como clave en el diccionario.
            col_valor (str): El nombre de la columna que se utilizará como valor en el diccionario.

        Returns:
            dict: Un diccionario creado a partir de las columnas especificadas.
        """
        try:
            # Verificar si las columnas existen en el DataFrame
            if col_clave not in df.columns or col_valor not in df.columns:
                raise ValueError(
                    "Las columnas especificadas no existen en el DataFrame."
                )

            # Crear el diccionario a partir de las columnas especificadas
            resultado_dict = df.set_index(col_clave)[col_valor].to_dict()

            return resultado_dict

        except ValueError as ve:
            # Registrar un mensaje crítico si hay un error
            logger.critical(f"Error: {ve}")
            raise ve

    @staticmethod
    def Filtrar_por_valores_pd(
        df: pd.DataFrame, columna: str, valores_filtrar: List[str | int]
    ) -> pd.DataFrame:
        """
        Filtra el DataFrame basándose en los valores de una columna específica.

        Args:
            columna (pd.Series): Columna del DataFrame a filtrar.
            valores_filtrar (List[Union[str, int]]): Lista de valores a utilizar para filtrar la columna.

        Returns:
            pd.DataFrame: DataFrame filtrado basándose en los valores especificados.
        """
        try:
            if isinstance(valores_filtrar, str):
                valores_filtrar = [valores_filtrar]

            # Filtrar el DataFrame basándose en los valores de la columna
            df_filtrado = df[df[columna].isin(valores_filtrar)]

            return df_filtrado

        except Exception as e:
            logger.critical(f"Error inesperado al filtrar por valores: {str(e)}")
            return None


    @staticmethod
    def convert_series_to_dataframe(data: pd.DataFrame | pd.Series) -> pd.DataFrame:
        """Convierte una Serie en un DataFrame si es necesario.

        Args:
            data (pd.DataFrame or pd.Series): DataFrame o Serie a procesar.

        Returns:
            pd.DataFrame: Si es una Serie, se convierte en un DataFrame. Si ya es un DataFrame, se retorna tal cual.

        Raises:
            ValueError: Si una Serie no tiene un nombre asignado.
        """
        # Si ya es un DataFrame, devolverlo tal cual
        if isinstance(data, pd.DataFrame):
            return data
        
        # Si es una Serie, convertirla a DataFrame
        if isinstance(data, pd.Series):
            if data.name is None:
                raise ValueError("La Serie proporcionada no tiene un nombre asignado.")
            col_nombre = data.name
            data = data.reset_index()
            data.columns = ["index", col_nombre]  # Renombrar columnas: índice y nombre de la serie
            return data
        
        # Si no es ni DataFrame ni Series, lanzar un error
        raise TypeError("El argumento debe ser un DataFrame o una Serie.")

    @staticmethod
    def pd_left_merge(
        base_left: pd.DataFrame | pd.Series,
        base_right: pd.DataFrame | pd.Series,
        key: str | list[str],
    ) -> pd.DataFrame:
        """Realiza un left join entre un DataFrame y otro DataFrame o Series.

        Args:
            base_left (pd.DataFrame or pd.Series): Base para el join (se convierte en DataFrame si es Series).
            base_right (pd.DataFrame or pd.Series): Datos complementarios (se convierte en DataFrame si es Series).
            key (str or list of str): Llave o llaves para realizar el merge.

        Returns:
            pd.DataFrame: Resultado del merge.
        """
        # Convertir Series a DataFrame usando la función auxiliar
        base_left = PandasBaseTransformer.convert_series_to_dataframe(base_left)
        base_right = PandasBaseTransformer.convert_series_to_dataframe(base_right)

        # Convertir llave única a lista
        if isinstance(key, str):
            key = [key]

        # Validar que todas las llaves estén presentes en ambos DataFrames
        for col in key:
            if col not in base_left.columns:
                raise KeyError(f"La columna '{col}' no existe en base_left.")
            if col not in base_right.columns:
                raise KeyError(f"La columna '{col}' no existe en base_right.")

        # Realizar el merge
        try:
            base = pd.merge(left=base_left, right=base_right, how="left", on=key)
            logger.success("Proceso de merge satisfactorio")
        except pd.errors.MergeError as e:
            logger.critical(f"Proceso de merge fallido: {e}")
            raise e

        return base


    
    @staticmethod
    def pd_left_merge_two_keys(
        base_left: pd.DataFrame,
        base_right: pd.DataFrame,
        left_key: list[str] | str,
        right_key: list[str] | str,
    ) -> pd.DataFrame:
        """Función que retorna el left join de dos dataframe de pandas.

        Args:
            base_left (pd.DataFrame): Dataframe que será la base del join.
            base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
            left_key (str or list of str): Llave o llaves mediante las cuales se va a realizar el merge.
            right_key (str or list of str): Llave o llaves en el DataFrame derecho para realizar el merge.

        Returns:
            pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
        """

        # Validar que base_left y base_right sean DataFrames de pandas
        if not isinstance(base_left, pd.DataFrame):
            raise ValueError("El argumento base_left no es un DataFrame de pandas")
        if not isinstance(base_right, pd.DataFrame):
            raise ValueError("El argumento base_right no es un DataFrame de pandas")

        # Convertir a lista si solo se pasa una llave como string
        if isinstance(left_key, str):
            left_key = [left_key]
        if isinstance(right_key, str):
            right_key = [right_key]

        # Validar que existan las columnas en ambos DataFrames
        for col in left_key:
            if col not in base_left.columns:
                raise KeyError(f"La columna '{col}' no existe en base_left")
        for col in right_key:
            if col not in base_right.columns:
                raise KeyError(f"La columna '{col}' no existe en base_right")

        base = None
        try:
            base = pd.merge(
                left=base_left,
                right=base_right,
                how="left",
                left_on=left_key,
                right_on=right_key,
            )
            logger.success("Proceso de merge satisfactorio")
        except pd.errors.MergeError as e:
            logger.critical(f"Proceso de merge fallido: {e}")
            raise e

        return base


    @staticmethod
    def merge_dfs_on_column(df_list: List[pd.DataFrame], key: str):
        """
        Fusiona una lista de DataFrames en uno solo, utilizando una columna específica
        como clave para el merge. Si la lista está vacía, devuelve None.

        Parámetros:
        - df_list (list of pd.DataFrame): Lista de DataFrames para fusionar.
        - key (str): Nombre de la columna en la que se basará el merge.

        Retorna:
        - pd.DataFrame: DataFrame resultante de la fusión de todos los DataFrames de la lista.
        """

        # Realiza la fusión (merge) sucesiva de todos los DataFrames en la lista
        df_merged = reduce(
            lambda left, right: pd.merge(left, right, on=key, how="left"),
            df_list,
        )

        return df_merged

    def Eliminar_filas_con_cadena(df: pd.DataFrame, columna: str, cadena: str):
        """
        Elimina todas las filas que contengan una palabra específica en una columna del DataFrame.

        Args:
            - df_name (str): Nombre del DataFrame.
            - columna (str): Nombre de la columna en la que se realizará la búsqueda.
            - cadena (str): Palabra específica que se utilizará como criterio de eliminación.

        Returns:
            pd.DataFrame: Nuevo DataFrame sin las filas que contienen la palabra especificada.
        """
        try:
            # Eliminar filas que contengan la palabra en la columna especificada
            df_filtrado = df[
                ~df[columna].str.contains(rf"\b{cadena}\b", case=False, regex=True)
            ]

            # Registrar información sobre las filas eliminadas
            logger.info(
                f"Filas que contienen '{cadena}' en la columna '{columna}' eliminadas con éxito."
            )

            return df_filtrado

        except KeyError as ke:
            # Registrar un error específico si la columna no existe
            logger.critical(f"Error al eliminar filas: {str(ke)}")
            # Propagar la excepción para que el usuario sea consciente del problema
            raise ke
        
    @staticmethod
    def concatenar_columnas_pd(
        dataframe: pd.DataFrame, cols_elegidas: List[str], nueva_columna: str
    ) -> pd.DataFrame | None:
        """
        Concatena las columnas especificadas y agrega el resultado como una nueva columna al DataFrame.

        Parámetros:
        dataframe (pd.DataFrame): DataFrame del cual se concatenarán las columnas.
        cols_elegidas (list): Lista de nombres de las columnas a concatenar.
        nueva_columna (str): Nombre de la nueva columna que contendrá el resultado de la concatenación.

        Retorna:
        pd.DataFrame: DataFrame con la nueva columna agregada.
        """
        try:
            # Verificar si dataframe es un DataFrame de pandas
            if not isinstance(dataframe, pd.DataFrame):
                raise TypeError(
                    "El argumento 'dataframe' debe ser un DataFrame de pandas."
                )

            # Verificar si las columnas especificadas existen en el DataFrame
            for col in cols_elegidas:
                if col not in dataframe.columns:
                    raise KeyError(f"La columna '{col}' no existe en el DataFrame.")

            # Concatenar las columnas especificadas y agregar el resultado como una nueva columna
            dataframe[nueva_columna] = (
                dataframe[cols_elegidas].fillna("").agg("".join, axis=1)
            )

            # Registrar el proceso
            logger.info(
                f"Columnas '{', '.join(cols_elegidas)}' concatenadas y almacenadas en '{nueva_columna}'."
            )

            return dataframe

        except Exception as e:
            logger.critical(f"Error inesperado al concatenar columnas: {str(e)}")
            return None

    def Remplazar_nulos_multiples_columnas_pd(
        base: pd.DataFrame, list_columns: list, value: str
    ) -> pd.DataFrame:
        base_modificada = None
        """Funcion que toma un dataframe, una lista de sus columnas para hacer un 
        cambio en los datos nulos de las mismas.
        Args:
            base: Dataframe a base del cambio.
            list_columns: Columnas a modificar su tipo de dato.
            Value: valor del dato: (Notar, solo del tipo str.) 
        Returns: 
            base_modificada (copia de la base con los cambios.)
        """
        try:
            base.loc[:, list_columns] = base[list_columns].fillna(value)
            base_modificada = base
            logger.success("cambio tipo de dato satisfactorio: ")

        except Exception:
            logger.critical("cambio tipo de dato fallido.")
            raise Exception

        return base_modificada

    def Renombrar_columnas_con_diccionario(
        base: pd.DataFrame, cols_to_rename: dict
    ) -> pd.DataFrame:
        """Funcion que toma un diccionario con keys ( nombres actuales ) y values (nuevos nombres) para remplazar nombres de columnas en un dataframe.
        Args:
            base: dataframe al cual se le harán los remplazos
            cols_to_rename: diccionario con nombres antiguos y nuevos
        Result:
            base_renombrada: Base con las columnas renombradas.
        """
        base_renombrada = None

        try:
            base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
            logger.success("Proceso de renombrar columnas satisfactorio: ")
        except Exception:
            logger.critical("Proceso de renombrar columnas fallido.")
            raise Exception

        return base_renombrada

    def duplicar_columnas_pd(df: pd.DataFrame, mapeo_columnas: dict):
        """
        Duplica múltiples columnas en un DataFrame de pandas, asignándoles nuevos nombres.

        Parámetros:
        - df (pandas.DataFrame): El DataFrame original.
        - mapeo_columnas (dict): Un diccionario que mapea los nombres de las columnas existentes
                                a los nuevos nombres. Las claves son los nombres de las columnas
                                existentes y los valores son los nuevos nombres de columna.

        Retorna:
        - Un nuevo DataFrame con las columnas duplicadas añadidas.
        """
        # Duplicar las columnas especificadas utilizando un bucle
        for columna_original, columna_nueva in mapeo_columnas.items():
            df[columna_nueva] = df[columna_original]

        return df
    
    @staticmethod
    def Reemplazar_columna_en_funcion_de_otra(
        df: pd.DataFrame,
        nom_columna_a_reemplazar: str,
        nom_columna_de_referencia: str,
        mapeo: dict,
    ) -> pd.DataFrame:
        """
        Reemplaza los valores en una columna en función de los valores en otra columna en un DataFrame.

        Args:
            df (pandas.DataFrame): El DataFrame en el que se realizarán los reemplazos.
            columna_a_reemplazar (str): El nombre de la columna que se reemplazará.
            columna_de_referencia (str): El nombre de la columna que se utilizará como referencia para el reemplazo.
            mapeo (dict): Un diccionario que mapea los valores de la columna de referencia a los nuevos valores.

        Returns:
            pandas.DataFrame: El DataFrame actualizado con los valores reemplazados en la columna indicada.
        """
        try:
            logger.info(
                f"Inicio de remplazamiento de datos en {nom_columna_a_reemplazar}"
            )
            df[nom_columna_a_reemplazar] = np.where(
                df[nom_columna_de_referencia].isin(mapeo.keys()),
                df[nom_columna_de_referencia].map(mapeo),
                df[nom_columna_a_reemplazar],
            )
            logger.success(
                f"Proceso de remplazamiento en {nom_columna_a_reemplazar} exitoso"
            )
        except Exception as e:
            logger.critical(
                f"Proceso de remplazamiento de datos en {nom_columna_a_reemplazar} fallido."
            )
            raise e

        return df

    def Reemplazar_valores_con_dict_pd(
        df: pd.DataFrame, columna: str, diccionario_mapeo: dict
    ):
        """
        Reemplaza los valores en la columna especificada de un DataFrame según un diccionario de mapeo.

        Args:
        - df (pd.DataFrame): El DataFrame a modificar.
        - columna (str): El nombre de la columna que se va a reemplazar.
        - diccionario_mapeo (dict): Un diccionario que define la relación de mapeo de valores antiguos a nuevos.

        Returns:
        - pd.DataFrame: El DataFrame modificado con los valores de la columna especificada reemplazados.

        - TypeError: Si 'df' no es un DataFrame de pandas o 'diccionario_mapeo' no es un diccionario.
        - KeyError: Si la 'columna' especificada no se encuentra en el DataFrame.

        """
        try:
            # Verificar si la entrada es un DataFrame de pandas
            if not isinstance(df, pd.DataFrame):
                raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

            # Verificar si la columna especificada existe en el DataFrame
            if columna not in df.columns:
                raise KeyError(f"Columna '{columna}' no encontrada en el DataFrame.")

            # Verificar si el diccionario de mapeo es un diccionario
            if not isinstance(diccionario_mapeo, dict):
                raise TypeError("'diccionario_mapeo' debe ser un diccionario.")

            # Realizar el reemplazo según el diccionario de mapeo
            df.loc[:, columna] = df[columna].replace(diccionario_mapeo)
            
            # Registrar mensaje de éxito
            logger.success(
                f"Valores de la columna '{columna}' reemplazados según el diccionario de mapeo."
            )

            return df

        except Exception as e:
            # Registrar mensaje crítico con detalles del tipo de error
            logger.critical(
                f"Error durante el reemplazo de valores en la columna. Tipo de error: {type(e).__name__}. Detalles: {str(e)}"
            )
            return None

    def Eliminar_columnas(df: pd.DataFrame, columnas_a_eliminar: list) -> pd.DataFrame:
        """
        Elimina las columnas especificadas de un DataFrame de pandas.

        Args:
            - df (pd.DataFrame): El DataFrame de pandas original.
            - columnas_a_eliminar (list): Lista de nombres de columnas a eliminar.

        Returns:
            pd.DataFrame: Un nuevo DataFrame sin las columnas especificadas.
        """
        try:
            # Eliminar las columnas del DataFrame
            df_resultado = df.drop(columns=columnas_a_eliminar)

            # Registrar información sobre las columnas eliminadas
            logger.info(f"Columnas {columnas_a_eliminar} eliminadas con éxito.")

            return df_resultado
        except Exception as e:
            # Registrar un error crítico si ocurre una excepción
            logger.critical(f"Error al eliminar columnas: {str(e)}")
            # Propagar la excepción para que el usuario sea consciente del problema
            raise e

def reemplazar_cero_por_si_no(df: pd.DataFrame , columnas:str|List):
    """
    Reemplaza los valores '0' por 'No' y cualquier otro valor por 'Si' en una o varias columnas de un DataFrame.

    Parámetros:
    df (pd.DataFrame): DataFrame que contiene las columnas a modificar.
    columnas (str o list): Nombre de la columna o lista de columnas a modificar.

    Retorna:
    pd.DataFrame: DataFrame con las columnas modificadas.
    """
    if isinstance(columnas, str):  # Si es una sola columna, convertir a lista
        columnas = [columnas]

    columnas_invalidas = [col for col in columnas if col not in df.columns]
    
    if columnas_invalidas:
        raise ValueError(f"Las siguientes columnas no existen en el DataFrame: {columnas_invalidas}")

    df[columnas] = df[columnas].astype(str).apply(lambda x: x.map(lambda v: "No" if v == "0" else "Sí"))
    
    return df


def marcar_cliente_inactivo(df, columna="Cliente Inactivo"):
    """
    Reemplaza los valores no vacíos por 'x' en la columna 'Cliente inactivo',
    dejando los valores vacíos sin cambios.

    Parámetros:
    df (pd.DataFrame): DataFrame que contiene la columna a modificar.
    columna (str, opcional): Nombre de la columna a modificar. Por defecto, "Cliente inactivo".

    Retorna:
    pd.DataFrame: DataFrame con la columna modificada.
    """
    if columna not in df.columns:
        raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

    df[columna] = df[columna].apply(lambda x: 'x' if pd.notna(x) and x != '' else x)

    return df