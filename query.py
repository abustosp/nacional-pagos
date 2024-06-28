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
    # SELECT cmp_pago_medio.*, cmp_pago.*, iva_persona.razon_social
    SELECT cmp_pago.numero, cmp_pago.proveedor, iva_persona.razon_social, cmp_pago.fecha, cmp_pago.caja, cmp_pago.cuenta, cmp_pago.tarjeta, cmp_pago.cheque, cmp_pago.cheque_3ro, cmp_pago.certificado, cmp_pago.ctacte, cmp_pago.total, cmp_pago_medio.modalidad, cmp_pago_medio.caja, cmp_pago_medio.cuenta, cmp_pago_medio.tarjeta, cmp_pago_medio.cheque, cmp_pago_medio.cheque_3ro, cmp_pago_medio.certificado, cmp_pago_medio.pago_a_cuenta
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
    #SELECT vta_cobro.*, vta_cobro_medio.*, iva_persona.razon_social
    SELECT vta_cobro.numero, vta_cobro.cliente, iva_persona.razon_social, vta_cobro.fecha, vta_cobro.caja, vta_cobro.cuenta, vta_cobro.tarjeta, vta_cobro.cheque, vta_cobro.cheque_3ro, vta_cobro.certificado, vta_cobro.ctacte, vta_cobro.total, vta_cobro_medio.modalidad, vta_cobro_medio.caja, vta_cobro_medio.cuenta, vta_cobro_medio.tarjeta, vta_cobro_medio.cheque, vta_cobro_medio.cheque_3ro, vta_cobro_medio.certificado, vta_cobro_medio.cobro_a_cuenta
    FROM vta_cobro
    LEFT JOIN vta_cobro_medio ON vta_cobro.numero = vta_cobro_medio.cobro
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
            with pd.ExcelWriter(f'resultados/reporte - {desde} - {hasta} - {i}.xlsx') as writer:
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


def numero_cmpvta(host:str, user:str, password:str, database:str , tabla:str):
    """
    Crear conexión a una base de datos para extraer todos los 'numero' de la tabla cmp_pago o vta_cobro

    Args:
        host (str): ip del servidor de base de datos
        user (str): usuario de la base de datos
        password (str): contraseña del usuario
        database (str): base de datos a la que se desea conectar
        tabla (str): nombre de la tabla de la cual se desea extraer los 'numero' (cmp_pago o vta_cobro)
    """
    
    # crear conexión 
    con = get_connection(host, database, user, password)
    
    # crear query
    query = text(f"""
    SELECT numero
    FROM {tabla};
    """)
    
    # realizar consulta
    tabla = pd.read_sql_query(query , con)
    
    # cerrar conexión
    con.close()
    
    # Separar los valores de la columna 'numero' en 3 columnas 'tipo' 'punto' 'comprobante' que se obtienen de la separación por ' '
    tabla['tipo'] = tabla['numero'].str.split(' ').str[0]
    tabla['punto'] = tabla['numero'].str.split(' ').str[1]
    tabla['comprobante'] = tabla['numero'].str.split(' ').str[2]
    
    del tabla['numero']
    
    maximo = int(tabla['comprobante'].max())
    sig = maximo + 1
