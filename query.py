import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
import pandas as pd

def listador_de_databases(ip:str , user:str , password:str):
    """
    Listar las bases de datos de un servidor MySQL

    Args:
        ip (str): ip del servidor de base de datos
        user (str): usuario de la base de datos
        password (str): contraseña del usuario
    """

    # Crear motor de conexión para mysql 5.5
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{ip}/')

    # Crear conexión
    connection = engine.connect()

    # Realizar consulta
    query = text("SHOW DATABASES")
    databases = connection.execute(query)

    # Guardar bases de datos en una lista
    databases = [database[0] for database in databases]
    
    # Eliminar las bases 'master' 'information_schema' 'performance_schema' 'mysql' de la lista
    borrar = ['master', 'information_schema', 'performance_schema', 'mysql']
    for i in borrar:
        databases.remove(i)


    return databases

def get_connection(ip:str , database:str , user:str , password:str):
    """
    Crear una conexión a una base de datos MySQL


    Args:
        ip (str): ip del servidor de base de datos
        database (str): base de datos a la que se desea conectar
        user (str): usuario de la base de datos
        password (str): contraseña del usuario
    """

    # Crear motor de conexión para mysql 5.5
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{ip}/{database}')

    # Crear conexión
    connection = engine.connect()
    
    return connection

def query_pago(connection:Connection, desde:str, hasta:str):
    """
    Realizar una consulta a la base de datos

    Args:
        connection (Connection): conexión a la base de datos
        desde (str): fecha de inicio de la consulta
        hasta (str): fecha de fin de la consulta
    """

    # Crear de pagos
    query = text(f"""
    SELECT cmp_pago_medio.*, cmp_pago.*, cmp_proveedor.razon_social
    FROM cmp_pago_medio
    LEFT JOIN cmp_pago ON cmp_pago_medio.pago = cmp_pago.numero 
    LEFT JOIN cmp_proveedor ON cmp_pago.proveedor = cmp_proveedor.id
    where cmp_pago.fecha BETWEEN :desde AND :hasta;""")

    # Realizar consulta y guardar en un DataFrame
    tabla = pd.read_sql_query(query , connection, params={'desde': desde, 'hasta': hasta})

    return tabla


def obtener_pagos_masivos(ip:str , user:str , password:str , desde:str , hasta:str):
    """
    Función para obtener los pagos de todas las bases de datos de un servidor MySQL

    Args:
        ip (str): ip del servidor de base de datos
        user (str): usuario de la base de datos
        password (str): contraseña del usuario
        desde (str): periodo desde el cual se desea obtener los pagos (formato 'YYYY-MM-DD')
        hasta (str): periodo hasta el cual se desea obtener los pagos (formato 'YYYY-MM-DD')
    """
    
    bases = listador_de_databases(ip, user, password)
    for i in bases:
        try:
            conección = get_connection(ip , i , user , password)
            datos = query_pago(conección, desde, hasta)
            datos.to_excel(f'resultados/datos - {i} - {desde} - {hasta}.xlsx', index=False)
        except:
            print(f'Error en la base de datos {i}')
            
    # Cerar conexión
    conección.close()
        
