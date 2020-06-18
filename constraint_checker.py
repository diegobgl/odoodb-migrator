import pgdb

hostname = 'localhost'
username = 'justo'
password = 'justo'
database = 'tas_12'
database13 = 'tas_13'

archivo_12 = open('v12.txt', 'r')
v12_salida = open('./output/v12_constraint_salida.txt', 'w')

archivo_13 = open('v13.txt', 'r')
v13_salida = open('./output/v13_constraint_salida.txt', 'w')



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
        cur.execute( "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS where table_name = '%s';" % linea )
        for constraint in cur.fetchall():
            lista_python[linea].append(constraint.constraint_name)
        lista_python[linea].append(len(lista_python[linea]))
    
    lista_python = sorted(lista_python.items(), key=lambda e: e[1][-1])
    for objeto in lista_python:
        if(ver == 12):
            print(objeto[0])
    archivo_escritura.writelines('"' + str(lista_python) + '"')


myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
doQuery( myConnection, 12)

myConnection13 = pgdb.connect( host=hostname, user=username, password=password, database=database13 )
doQuery( myConnection13, 13)

myConnection.close()