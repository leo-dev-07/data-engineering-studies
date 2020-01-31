#%%
import crud as dbCrud
import pyodbc as odb

driver1 = r'ODBC Driver 17 for SQL Server'
driver2 = r'SQL Server'
bd = dbCrud.CRUDmssql(SERVER='USER-PC' ,DATABASE='TESTE_PYTHON',DRIVER=driver1 ,USERNAME='User',PASSWORD='')
conn = bd.mssqConnection()
#%%
#CRIA TABELAS
bd.criaTabela('RESULTADO_1',conn)
#bd.criaTabela2('RESULTADO_2',conn)

#%%
import time
import os
import string

path = r'C:\Users\user\Documents\git_projects\data-engineering-studies\planilhas'

nome_bd_tab= 'RESULTADO_1'#TEM QUE SER NESSA ORDEM!
timer = 0

#Loop principal
for nome_arq in os.listdir(path):
    try:
        print(nome_arq)
        bd.procedimentoFinal(path+"\\"+nome_arq , nome_bd_tab ,conn)
        time.sleep(2)
    except os.error as err:
        print(err)
        #time.sleep(15)

    except odb.Error as err:
        print(err)
        #time.sleep(15)

#%%
bd.testa()
#%%

