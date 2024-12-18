from sqlalchemy import create_engine
import pandas as pd

# Configuración del engine
engine = create_engine("postgresql+psycopg2://postgres:SsQFip8CV8mkLkp@localhost:5432/classicmodels")


def leer_tabla(tabla, engine):
    """
    Lee una tabla desde la base de datos y la convierte en un DataFrame

    Args:
        tabla (str): Nombre de la tabla en la base de datos.
        engine: Motor de conexión SQLAlchemy.

    Returns:
        pd.DataFrame: DataFrame con los datos de la tabla.
    """
    try:
        # Construye la consulta SQL
        query = f"SELECT * FROM {tabla}"
        # Lee la tabla utilizando pandas
        df = pd.read_sql(query, engine)
        # Devuelve el DataFrame
        return df
    except Exception as e:
        print(f"Error al leer la tabla {tabla}: {e}")
        return None  


def filtrar_fecha(df, columna, fechaInicio, fechaFinal):
    """
    Filtrar un DataFrame por un rango de fechas.

    Args:
        df (pd.DataFrame): DataFrame a filtrar
        columna (str): nombre de la columna 
        fechaInicio (str): fecha de inicio en formato 'YYYY-MM-DD'
        fechaFinal (str): fecha de fin en formato 'YYYY-MM-DD'
    
    Returns: 
        pd.DataFrame: El dataFrame filtrado por el rango de fechas.
    """
    try:
        # convertir la columna de fecha a formato datetime para poder trabajar con las fechas
        df[columna] = pd.to_datetime(df[columna])
        # filtrar las filas por el rango de fechas que ingresen en la función 
        return df[(df[columna] >= pd.to_datetime(fechaInicio)) & (df[columna] <= pd.to_datetime(fechaFinal))] 
    except Exception as e:
        print(f"Error al leer la tabla {df}: {e}")
        return None   


def generar_reportes(df, filas, columnas, valores, funcion, fill_value = 0):
    """_summary_

    Args:
        df (pd.DataFrame): DataFrame a pivotar
        filas (str): nombre de la columna que se utilizará como filas.
        columnas (str): nombre de la columna que se utilizará como columna.
        valores (str): nombre de la columna que se utilizará como valores.
        funcion (funcion): Función agregadora a aplicar sobre los valores
        
    Returns: 
        pd.DataFrame: El DataFrame pivotado
    """
    try:
        reporte = pd.pivot_table(
        df,
        index = filas,
        columns = columnas,
        values = valores, 
        aggfunc = funcion,
        fill_value = fill_value  #rellenar con 0 donde falten datos
        )
        return reporte
    except Exception as e:
        print(f"Error al leer la tabla {df}: {e}")
        return None   


def guardar_postgre(df, nombreTabla, engine, if_exists='replace'):
    """
    Guarda un DataFrame en forma de tabla en PostgreSQL

    Args:
        df (pd.DataFrame): El DataFrame a guardar
        nombreTabla (str): El nombre en la tabla en la base de datos
        engine (sqlalchemy.engine.base.Engine): El engine de conexión a PostgreSQL
        if_exists (str): Puede ser 'replace'(default), 'append' o 'fail'

    """
    try:
        df.to_sql(nombreTabla, engine, if_exists = if_exists, index = False)
        print(f"Guardado con éxito en PostgreSQL en la tabla {nombreTabla}")
    except Exception as e:
        print(f"Error al leer la tabla {df}: {e}")
        return None  