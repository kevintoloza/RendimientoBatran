
from logging.handlers import QueueListener
import cx_Oracle
import mysql.connector
from datetime import datetime
import logging
import pytz
import time
import RendimientoBatranParams as db
import RendimientoQuery as queries
from pyzabbix import ZabbixMetric, ZabbixSender

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,filename='Rendimiento_Batran_BA.log')

def consultar_mysql(consultar):
    try:
        cnx = mysql.connector.connect(**db.mysql_args)
        cur = cnx.cursor()
        cur.execute(consultar)
        resultado = cur.fetchall()
        cur.close()
        cnx.close()
    except mysql.connector.Error as err:
        print("Error obteniendo datos:", err)
        return None
    else:
        return tuple(resultado)

def local_time():
    tzone = pytz.timezone('America/El_Salvador')
    tz_dtime = tzone.localize(datetime.now())
    return tz_dtime

def delete_mysql(query):
    try:
        conn = mysql.connector.connect(**db.mysql_args)
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.close()
    except mysql.connector.Error as err:
        logging.info(datetime.now())
        logging.info("Error: ", err)
        logging.info("Sentencia delete: ", query)
        return None
    else:
        return None

def consultar_oracle(query):
    try:
        cnx = cx_Oracle.connect(user=db.user, password=db.pw, dsn=db.dsn, encoding=db.encoding)
        cursor = cnx.cursor()
        cursor.execute(query)
        resultado = cursor.fetchall()
        cursor.close()
        cnx.close()
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        logging.info("Oracle-Error-Code:", error.code)
        logging.info("Oracle-Error-Message:", error.message)
        return None
    else:
        return tuple(resultado)

def constructor_insert(data):
    values = ''
    try:
        for row in data:
            values += str(row)+','
        return values
    except Exception as error:
        logging.info("Error sentencia sql;", error)

def insertar_mysql_bis(query,parametro):
    try:
        conn = mysql.connector.connect(**db.mysql_args)
        cur = conn.cursor()
        if parametro == 'ProcesadoBase': 
            cur.execute(queries.INSERTAR_PROCESO_BASE.format(datos=query[:-1]))
        if parametro == 'sesionesBatran': 
            cur.execute(queries.INSERTAR_SESIONES_BASE.format(datos=query[:-1]))  
        if parametro == 'rendimientoBatran': 
            cur.execute(queries.INSERTAR_RENDIMIENTO_BASE.format(datos=query[:-1]))   
        if parametro == 'estadisticasBatran': 
            cur.execute(queries.INSERTAR_ESTADISTICAS_BASE.format(datos=query[:-1])) 
        if parametro == 'crecimientoBatran': 
            cur.execute(queries.INSERTAR_CRECIMIENTO_BASE.format(datos=query[:-1])) 
        if parametro == 'pgasagBatran': 
            cur.execute(queries.INSERTAR_PGA_SGA_BASE.format(datos=query[:-1])) 
        if parametro == 'infosgaBatran': 
            cur.execute(queries.INSERTAR_PGA_SGA_INFO_BASE.format(datos=query[:-1]))
        if parametro == 'discoBatran': 
            cur.execute(queries.INSERTAR_DISCO_BASE.format(datos=query[:-1]))
        if parametro == 'discoioBatran': 
            cur.execute(queries.INSERTAR_DISCOIO_BASE.format(datos=query[:-1]))
        if parametro == 'bloqueoBatran': 
            cur.execute(queries.INSERTAR_BLOQUEO_BASE.format(datos=query[:-1]))
        cur.close()
        conn.close()
    except mysql.connector.Error as err:
        logging.info(datetime.now())
        logging.info("Error: ", err)
        logging.info("Sentencia insert: ", query)
        return None
    else:
        return None

def Ejecutar(query,parametro,delete):
    rendimientosqueries = consultar_oracle(query)
    if rendimientosqueries !=():
        rendimiento_bis = constructor_insert(rendimientosqueries)
        insertar_mysql_bis(rendimiento_bis,parametro)
        delete_mysql(delete)


if __name__ == '__main__':
    while True:
        dtime = local_time()
        Ejecutar(queries.ProcesadoBaseBatran,'ProcesadoBase',queries.DELETE_PROCESO_BASE_INTERVAL)
        Ejecutar(queries.SESIONES_BATRAN,'sesionesBatran',queries.DELETE_SESIONES_BASE_INTERVAL)
        Ejecutar(queries.RendimientoQueryBatran.format(fecha=dtime.strftime('%Y-%m-%d')),'rendimientoBatran',queries.DELETE_RENDIMIENTO_BASE_INTERVAL)
        Ejecutar(queries.EstadisticasBatran,'estadisticasBatran',queries.DELETE_ESTADISTICAS_BASE_INTERVAL)
        Ejecutar(queries.CrecimientoBaseBatran,'crecimientoBatran',queries.DELETE_CRECIMIENTO_BASE_INTERVAL)
        Ejecutar(queries.PgaBatranSga,'pgasagBatran',queries.DELETE_PGASGA_BASE_INTERVAL)
        Ejecutar(queries.SgaBatranInfo,'infosgaBatran',queries.DELETE_INFOSGA_BASE_INTERVAL)
        Ejecutar(queries.DiscoBatran,'discoBatran',queries.DELETE_DISCO_BASE_INTERVAL)
        Ejecutar(queries.DiscoBatranIO,'discoioBatran',queries.DELETE_DISCO_IO_BASE_INTERVAL)
        Ejecutar(queries.BloqueoBatran,'bloqueoBatran',queries.DELETE_BLOQUEO_BASE_INTERVAL)
        
        time.sleep(60)