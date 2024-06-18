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
    borrar = ['master', 'information_schema', 'performance_schema', 'mysql', 'sys_nacional']
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
    Realizar una consulta a la base de datos de pagos

    Args:
        connection (Connection): conexión a la base de datos
        desde (str): fecha de inicio de la consulta
        hasta (str): fecha de fin de la consulta
    """

    # Crear query de pagos
    query = text(f"""
    SELECT cmp_pago_medio.*, cmp_pago.*, iva_persona.razon_social
    FROM cmp_pago_medio
    LEFT JOIN cmp_pago ON cmp_pago_medio.pago = cmp_pago.numero 
    LEFT JOIN iva_persona ON cmp_pago.proveedor = iva_persona.id
    where cmp_pago.fecha BETWEEN :desde AND :hasta
    ;""")

    # Realizar consulta y guardar en un DataFrame
    tabla = pd.read_sql_query(query , connection, params={'desde': desde, 'hasta': hasta})

    return tabla


def query_cobro(connection:Connection, desde:str, hasta:str):
    """
    Realizar una consulta a la base de datos de cobros

    Args:
        connection (Connection): conexión a la base de datos
        desde (str): fecha de inicio de la consulta
        hasta (str): fecha de fin de la consulta
    """
    
    # crear query de cobros
    query = text(f"""
    SELECT vta_cobro.*, vta_cobro_medio.*, iva_persona.razon_social
    FROM vta_cobro
    LEFT JOIN vta_cobro_medio ON vta_cobro.numero = vta_cobro_medio.cliente
    LEFT JOIN iva_persona ON vta_cobro.cliente = iva_persona.id
    where vta_cobro.fecha BETWEEN :desde AND :hasta
    ;""")
    
    # Realizar consulta y guardar en un DataFrame
    tabla = pd.read_sql_query(query , connection, params={'desde': desde, 'hasta': hasta})
    
    return tabla


def query_IVA(connection:Connection, desde:str, hasta:str):
    """
    Realizar una consulta a la base de datos de comprobantes de IVA

    Args:
        connection (Connection): conexión a la base de datos
        desde (str): fecha de inicio de la consulta
        hasta (str): fecha de fin de la consulta
    """

    # Crear query de IVA
    query = text(f"""
    SELECT iva_comprobante.modulo, iva_comprobante.tipo, comprobante, persona, iva_persona.razon_social, fecha, periodo , total
    FROM iva_comprobante
    LEFT JOIN iva_persona ON iva_comprobante.persona = iva_persona.id
    where fecha BETWEEN :desde AND :hasta
    ;""")

    # Realizar consulta y guardar en un DataFrame
    tabla = pd.read_sql_query(query , connection, params={'desde': desde, 'hasta': hasta})
    
    # multiplicar por -1 los valores de la columna total si el tipo es 'NC' 'NCE'
    #tabla.loc[tabla['tipo'].str.contains('NC|NCE'), 'total'] = tabla.loc[tabla['tipo'].str.contains('NC|NCE'), 'total'] * -1
    tabla.loc[tabla['tipo'].isin(['NC', 'NCE']), 'total'] = tabla.loc[tabla['tipo'].isin(['NC', 'NCE']), 'total'] * -1
    
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
            pago = query_pago(conección, desde, hasta)
            iva = query_IVA(conección, desde, hasta)
            cobro = query_cobro(conección, desde, hasta)
            
            # realizar tablas dinámicas
            
            iva_td_periodo = pd.pivot_table(iva, values='total', index=['modulo','razon_social','persona'], columns=['periodo'], aggfunc='sum', fill_value=0)
            # mostrar en la tavla dinámica los indices en todas las filas
            iva_td_periodo = iva_td_periodo.reset_index()
            
            iva_td_resumen = pd.pivot_table(iva, values='total', index=['modulo','razon_social','persona'], aggfunc='sum', fill_value=0)
            # mostrar en la tavla dinámica los indices en todas las filas
            iva_td_resumen = iva_td_resumen.reset_index()
            
            # Exportar a un mismo excel
            with pd.ExcelWriter(f'resultados/datos - {i} - {desde} - {hasta}.xlsx') as writer:
                iva.to_excel(writer, sheet_name='IVA', index=False)
                pago.to_excel(writer, sheet_name='Pagos', index=False)
                cobro.to_excel(writer, sheet_name='Cobros', index=False)
                iva_td_periodo.to_excel(writer, sheet_name='IVA_TD_periodo', index=False)
                iva_td_resumen.to_excel(writer, sheet_name='IVA_TD_resumen', index=False)
            print(f'Base de datos {i} exportada correctamente')
        except:
            print(f'Error en la base de datos {i}')
            
    # Cerar conexión
    conección.close()
        
