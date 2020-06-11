import pgdb

hostname = 'localhost'
username = 'justo'
password = 'justo'
database = 'tas_v12'
database13 = 'tas_13'

archivo_12 = open('v12.txt', 'r')
v12_salida = open('v12_salida.txt', 'w')

archivo_13 = open('v13.txt', 'r')
v13_salida = open('v13_salida.txt', 'w')



def doQuery( conn, ver ) :
    if ver == 13:
        archivo_lectura = archivo_13
        archivo_escritura = v13_salida
    else:
        archivo_lectura = archivo_12
        archivo_escritura = v12_salida

    lista = []
    lista_python = {}
    for linea in archivo_lectura.readlines():
        linea = linea[:-1]
        lista_python[linea] = []
        cur = conn.cursor()
        cur.execute( "SELECT column_name FROM information_schema.columns where table_name = '%s'" % linea )
        for nombre in cur.fetchall():
            lista_python[linea].append(nombre.column_name)

    archivo_escritura.writelines('"' + str(lista_python) + '"')


myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
doQuery( myConnection, 12)

myConnection13 = pgdb.connect( host=hostname, user=username, password=password, database=database13 )
doQuery( myConnection13, 13)

myConnection.close()