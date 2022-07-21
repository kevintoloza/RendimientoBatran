# -*- coding: utf-8 -*-

#Muestra información sobre el uso de recursos globales para algunos de los recursos del sistema
ProcesadoBaseBatran = """
select RESOURCE_NAME,CURRENT_UTILIZATION,MAX_UTILIZATION,LIMIT_VALUE
from gv$resource_limit
where RESOURCE_NAME in ('processes','sessions')
"""
#Proporciona estadísticas sobre sentencias SQL que están en la memoria, analizadas y listas para ejecutarse
RendimientoQueryBatran = """
select
SA.PLSQL_EXEC_TIME,sa.SQL_ID,COALESCE(sa.sql_text,'null') as sql_text,
sa.cpu_time, sa.executions, sa.ROWS_PROCESSED,
COALESCE(sa.optimizer_cost,0) as optimizer_cost ,au.USERNAME parseuser,
to_char(sa.LAST_ACTIVE_TIME,'YYYY-MM-DD HH24:MI:SS') as LAST_ACTIVE_TIME
from   v$sqlarea sa, ALL_USERS au
where (parsing_user_id != 0)
AND (au.user_id(+)=sa.parsing_user_id)
and sa.cpu_time >= 20000
and to_char(sa.LAST_ACTIVE_TIME,'YYYY-MM-DD') = '{fecha}'
order by sa.cpu_time
"""

#Estadisticas de base
EstadisticasBatran = """
select COALESCE(PNAME,'null'),PID,COALESCE(USERNAME,'null'),PROGRAM,COALESCE(BACKGROUND,'normal'),
PGA_USED_MEM,PGA_MAX_MEM,PGA_ALLOC_MEM
from SYS.V_$PROCESS
"""
#Describe los archivos de la base de datos
CrecimientoBaseBatran = """
select df.tablespace_name as "TABLE_SPACE",totalusedspace as "USED_MB",
(df.totalspace - tu.totalusedspace) as "FREE_MB",df.totalspace as "TOTAL_MB",
round(100 * ( (df.totalspace - tu.totalusedspace)/ df.totalspace))  as "PCT_FREE"
from (select tablespace_name,round(sum(bytes) / 1048576) TotalSpace
from dba_data_files
group by tablespace_name) df,
(select round(sum(bytes)/(1024*1024)) totalusedspace, tablespace_name
from dba_segments
group by tablespace_name)
tu where df.tablespace_name = tu.tablespace_name
"""

#Muestra estadísticas de uso de memoria
PgaBatranSga= """
select name, value
from SYS.V_$PGASTAT
where name in ('total PGA inuse','total PGA allocated','maximum PGA allocated','total freeable PGA memory')
"""
#Muestra información de tamaño sobre el SGA
SgaBatranInfo= """
select name, bytes
from SYS.V_$SGAINFO
where name in 
('Redo Buffers','Buffer Cache Size','Shared Pool Size','Large Pool Size','Java Pool Size',
'Shared IO Pool Size','Granule Size','Maximum SGA Size')
"""

#Msuestra información sobre estadísticas a nivel de segmento
DiscoBatran= """
SELECT *
FROM (SELECT
    statistic_name
    ,owner
    ,object_type
    ,object_name
    ,value as valor
    FROM v$segment_statistics
    WHERE statistic_name IN
    ('physical reads', 'physical writes', 'logical reads',
        'physical reads direct', 'physical writes direct')
ORDER BY valor DESC)
WHERE rownum < 20
"""
#Las estadísticas que se muestran V$SQLnormalmente se actualizan al final de la ejecución de la consulta
DiscoBatranIO= """
SELECT *
FROM
(SELECT
    parsing_schema_name
    ,direct_writes
    ,sql_text as text
    ,SQL_ID
    ,disk_reads
FROM v$sql
ORDER BY disk_reads DESC)
WHERE rownum < 20
"""
#Numera los bloqueos actualmente en poder de la base de datos de Oracle y las solicitudes pendientes de bloqueo o pestillo
BloqueoBatran= """
SELECT
    COALESCE(decode(L.TYPE,'TM','TABLE','TX','Record(s)'),'2') TYPE_LOCK,
    decode(L.REQUEST,0,'NO','YES') WAIT,
    S.OSUSER OSUSER_LOCKER,
    l.block,
    S.PROCESS PROCESS_LOCKER,
    COALESCE(S.USERNAME,'3') DBUSER_LOCKER,
    O.OBJECT_NAME OBJECT_NAME,
    O.OBJECT_TYPE OBJECT_TYPE,
    concat(' ',s.PROGRAM) PROGRAM,
    O.OWNER OWNER
FROM v$lock l,dba_objects o,v$session s
WHERE
    l.ID1 = o.OBJECT_ID
    AND s.SID =l.SID
"""
#Esta vista enumera la información de sesión para cada sesión actual.
SESIONES_BATRAN= """
select COALESCE(username,'null'), machine,
program,status
from v$session
"""
######################################################################################################
INSERTAR_RENDIMIENTO_BASE = """
INSERT INTO sigeco.RENDIMIENTO_QUERY_BATRAN_BIS(PLSQL_EXEC_TIME,SQL_ID,SQL_TEXT,CPU_TIME,EXECUTIONS,ROWS_PROCESSED,OPTIMIZER_COST,USER_NAME,LAST_ACTIVE_TIME) 
values {datos}
"""

INTERVALOS_DIAS = """ CALL TRANSACCIONES_MES_BATRAN_RENDIMIENTO"""

INSERTAR_PROCESO_BASE = """
INSERT INTO sigeco.PROCESADO_BASE_BATRAN_BIS(RESOURCE_NAME,CURRENT_UTILIZATION,MAX_UTILIZATION,LIMIT_VALUE) 
values {datos}
"""

INSERTAR_SESIONES_BASE = """
INSERT INTO sigeco.SESIONES_BASE_BATRAN_BIS(USERNAME,MACHINE,PROGRAM,STATUS)
values {datos}
"""

INSERTAR_ESTADISTICAS_BASE = """
INSERT INTO sigeco.ESTADISTICAS_BATRAN_BIS(PNAME,PID,USERNAME,PROGRAM,BACKGROUND,PGA_USED_MEM,PGA_MAX_MEM,PGA_ALLOC_MEM)
values {datos}
"""

INSERTAR_CRECIMIENTO_BASE = """
INSERT INTO sigeco.CRECIMIENTO_BASE_BATRAN_BIS(TABLE_SPACE,USED_MB,FREE_MB,TOTAL_MB,PCT_FREE)
values {datos}
"""

INSERTAR_PGA_SGA_BASE = """
INSERT INTO sigeco.PGA_SGA_BATRAN_BIS(NOMBRE,VALOR)
values {datos}
"""

INSERTAR_PGA_SGA_INFO_BASE = """
INSERT INTO sigeco.INFO_SGA_BATRAN_BIS(NOMBRE,VALOR)
values {datos}
"""

INSERTAR_DISCO_BASE = """
INSERT INTO sigeco.DISCO_BATRAN_BIS(STATISTIC_NAME,OWNER,OBJECT_TYPE,OBJECT_NAME,VALOR)
values {datos}
"""

INSERTAR_DISCOIO_BASE = """
INSERT INTO sigeco.DISCO_IO_BATRAN_BIS(PARSING_SCHEMA_NAME,DIRECT_WRITES,TEXT,SQL_ID,DISK_READS)
values {datos}
"""

INSERTAR_BLOQUEO_BASE = """
INSERT INTO sigeco.BLOQUEO_BATRAN_BIS(TYPE_LOCK,WAIT,OSUSER_LOCKER,block,PROCESS_LOCKER,DBUSER_LOCKER,OBJECT_NAME,OBJECT_TYPE,PROGRAM,OWNER)
values {datos}
"""

########################################################################################################
DELETE_PROCESO_BASE_INTERVAL="""
DELETE FROM sigeco.PROCESADO_BASE_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.PROCESADO_BASE_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_SESIONES_BASE_INTERVAL="""
DELETE FROM sigeco.SESIONES_BASE_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.SESIONES_BASE_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_RENDIMIENTO_BASE_INTERVAL="""
DELETE FROM sigeco.RENDIMIENTO_QUERY_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.RENDIMIENTO_QUERY_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_ESTADISTICAS_BASE_INTERVAL="""
DELETE FROM sigeco.ESTADISTICAS_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.ESTADISTICAS_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_CRECIMIENTO_BASE_INTERVAL="""
DELETE FROM sigeco.CRECIMIENTO_BASE_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.CRECIMIENTO_BASE_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_PGASGA_BASE_INTERVAL="""
DELETE FROM sigeco.PGA_SGA_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.PGA_SGA_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""
DELETE_INFOSGA_BASE_INTERVAL="""
DELETE FROM sigeco.INFO_SGA_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.INFO_SGA_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_DISCO_BASE_INTERVAL="""
DELETE FROM sigeco.DISCO_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.DISCO_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_DISCO_IO_BASE_INTERVAL="""
DELETE FROM sigeco.DISCO_IO_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.DISCO_IO_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""

DELETE_BLOQUEO_BASE_INTERVAL="""
DELETE FROM sigeco.BLOQUEO_BATRAN_BIS
WHERE clock <= ((SELECT max(clock) as clock
FROM sigeco.BLOQUEO_BATRAN_BIS WHERE clock >= UNIX_TIMESTAMP(DATE(NOW()))) - interval 3 minute)
"""