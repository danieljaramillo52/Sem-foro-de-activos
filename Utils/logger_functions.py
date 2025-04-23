import os
from loguru import logger

def setup_logging():
    """Configura el logger para guardar los registros en un archivo externo.

    Esta función configura el logger de Loguru para guardar los registros en un archivo
    llamado 'logs.log' dentro de la carpeta 'logs_folder'. Si la carpeta no existe, se
    creará automáticamente.

    Returns:
        logger: El objeto logger configurado para registrar en el archivo externo.
    """
    # Obtén la ruta absoluta de la carpeta logs_folder en el directorio principal
    logs_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs_folder")
    os.makedirs(logs_folder, exist_ok=True)  # Crea la carpeta si no existe

    # Configuración del manejador para guardar en un archivo externo en la carpeta logs_folder
    log_file = os.path.join(logs_folder, "logs.log")

    # Configura el logger para que solo guarde los registros de la sesión actual y sobrescriba el archivo en cada ejecución
    # El parámetro "rotation" se establece en "500 MB" para rotar el archivo cuando alcance 500 MB de tamaño.
    # El parámetro "mode" se establece en "w" para sobrescribir el archivo en cada ejecución.
    logger.add(log_file, rotation="500 MB", mode="w")

    return logger

# Llamamos a la función setup_logging() para obtener el logger configurado
setup_logs = setup_logging()

"""resultado_final = PV.Procesar_ventas(
    Pandas_Functions=TF.PandasBaseTransformer,  # La instancia de tu clase con métodos de transformación
    config=config,  # El diccionario de configuración
    base_activos_clientes=base_activos_clientes,  # DataFrame de activos y clientes
    vts_snkros=vts_snkros,  # DataFrame de ventas snorkels
    vtas_pp=vtas_pp,  # DataFrame de ventas de puntos de pago
    vts_nevrs=vts_nevrs,  # DataFrame de ventas de neveras
)"""