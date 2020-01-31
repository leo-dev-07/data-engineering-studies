#%%
import matplotlib.pyplot as plt
import pandas as pd
import crud as dbCrud


driver1 = r'ODBC Driver 17 for SQL Server'
driver2 = r'SQL Server'
bd_mssql = dbCrud.CRUDmssql(SERVER='USER-PC' ,DATABASE='TESTE_PYTHON',DRIVER=driver1 ,USERNAME='User',PASSWORD='')
conn = bd_mssql.mssqConnection()

#%%
venda= pd.read_sql_query('SELECT salesman, sum(value_day) FROM RESULTADO_1  group by salesman',conn)

prod = pd.read_sql_query('SELECT product, sum(value_day) FROM RESULTADO_1  group by product',conn)

loja = pd.read_sql_query('SELECT store, sum(value_day) FROM RESULTADO_1  group by store',conn)

cliente = pd.read_sql_query('SELECT customer, sum(value_day) FROM RESULTADO_1  group by customer',conn)

venda.plot(kind='bar',x='salesman',title='Valor por venda')
prod.plot(kind='bar',x='product',title='Produto x Valor das compras')
loja.plot(kind='bar',x='store',title='Loja x Valor das compras')
cliente.plot(kind='bar',x='customer',title='Cliente x Valor das compras')
plt.show()
#df.plot(kind='bar', x='idx', y='value_day')
#plt.show()

#%%
import numpy as np

dados = pd.read_sql_query('SELECT * FROM RESULTADO_1',conn)
tot_vendas_por_vendedor = dados.groupby('salesman').agg({'value_day':np.sum})
qt_vendas_por_vendedor = dados.groupby('salesman').agg({'idx':pd.Series.count})
med_vendas_por_vendedor = dados.groupby('salesman').agg({'value_day':np.mean})

tot_vendas_por_vendedor.plot(kind='barh',title='Tot Vendas por vendedor')
qt_vendas_por_vendedor.plot(kind='pie',title='Qt Vendas por vendedor',subplots=True)
med_vendas_por_vendedor.plot(kind='barh', title='Med Vendas por vendedor')

plt.show()
