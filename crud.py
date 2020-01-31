#%%
import pandas as pd
import pyodbc as odb
import datetime as datetime

#Classe que define operações  de escrita em banco de dados SQL Server
class CRUDmssql:
    # Cria uma conexão com um bando dedados e retorna um cursor
    def __init__(self, SERVER, DATABASE, DRIVER, USERNAME, PASSWORD):
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        # A máquina local usa este driver ODBC abaixo
        self.DRIVER = DRIVER
        self.USERNAME = USERNAME
        self.PASSWORD = PASSWORD

    def testa(self):
        print('Processo de carregamento de dados finalizado.\n')

    #Cria uma função de conexão
    def mssqConnection(self):
        try:
            # string_conexao = r'{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={ODBC+Driver+13+for+SQL+Server}?trusted_connection=yes;'
            string_conexao = 'Driver=' + self.DRIVER + ';Server=' + self.SERVER + ';Database=' + self.DATABASE + ';UID= ' + self.USERNAME + '; Trusted_Connection=yes;'

            # DATABASE_CONNECTION= r'mssql+pyodbc://'+string_conexao
            conn = odb.connect(string_conexao)
            print('Conexão feita!')
            return conn

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)
            print('Problema mssqConnection function.')

    # Cria Tabela
    def criaTabela(self, nome, conn):
        # Tenta executar uma SQL para criar uma tabela. Caso contrário, ele retorna valor 0 e mostra um print
        try:
            conn = conn.cursor()
            conn.execute(
                "CREATE TABLE [{}] (idx INT PRIMARY KEY IDENTITY(1,1) , data_ VARCHAR(22), product VARCHAR (11), customer VARCHAR (11), store VARCHAR (11), value_day INT, salesman VARCHAR(11), ini_vigencia VARCHAR(11), fim_vigencia VARCHAR(11));".format(
                    nome))
            conn.commit()
            print('Tabela criada.')
            return 1

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)
            print('Problema')
            return 0

    #Cria tabela 2
    def criaTabela2(self, nome, conn):
        # Tenta executar uma SQL para criar uma tabela. Caso contrário, ele retorna valor 0 e mostra um print
        try:
            conn = conn.cursor()
            conn.execute(
                "CREATE TABLE [{}] (ano VARCHAR(10), mes INT, variavel VARCHAR (5), serie INT, valor float);".format(nome))
            conn.commit()
            print('Tabela criada.')
            return 1

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)
            print('Problema')
            return 0

    #Busca o útlimo registro no banco de dados com base no índice da tabela
    def buscaUltimoRegistro(self, nome_tab, conn):
        try:
            query = "SELECT TOP 1 * FROM [{}] order by idx desc;".format(nome_tab)
            df = pd.read_sql_query(query, conn)
            print('ÚLTIMO REGISTRO BUSCADO')
            return df

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)
            print('Último registro não foi buscado.')

    #Atualiza tabela de rotina de vigência
    def atualizaTabela(self,nome_tab, *args,conn):
        try:
            param_nome_tab = "UPDATE [" + nome_tab + "] "
            set = "SET "
            where_param = " WHERE "
            q_mark = '?'

            # Formatação dos nomes das colunas para query
            for col in args[0]:
                set += col + ' = ' + q_mark + ","

            # Formatação dos valores para query coloquando question marks
            for value in args[1]:
                where_param += value + ' = ' + q_mark + ' AND '

            set = set[:-1]
            where_param = where_param[:-5]
            query = param_nome_tab + set + " " + where_param + ";"
            print(query)
            conn.execute(query,args[2])
            print('Atualização da tabela de vigencia feita.')
            conn.commit()

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)
            print('Não foi possivel fazer a atualização.')

    def aplicaVig(self, val, df,conn):
        for i, row in df.iterrows():
            #if (row['product'] == val['product']) and (row['value_day'] != val['value_day']) and (row['data_'] != val['data_']):
            query = "SELECT * FROM [RESULTADO_1] WHERE product ='"+val['product']+"' ;"
            query_bd = pd.read_sql_query(query, conn)
            print('aplicaVig ->Query Executada:  QUERY: \n',query)
            print('aplicaVig -> : QUERY: \n',query_bd)

            try:
                query_bd = query_bd.loc[-1]
                print('aplicaVig -> Última posição:  \n',query_bd)
                ini_vig = query_bd.ini_vigencia.values[-1]
                values = query_bd.idx.values[-1]
                print(query_bd.ini_vigencia.values[-1])
                print(query_bd.idx.values[-1])

                self.atualizaTabela('RESULTADO_1', ['fim_vigencia'], ['idx'],
                                    [ini_vig, int(str(values),10)], conn= conn)
            except KeyError:
                print('aplicaVig -> PASSOU:  \n',query_bd.values)
                ini_vig = query_bd.ini_vigencia.values[0]
                values = query_bd.idx.values[0]
                self.atualizaTabela('RESULTADO_1', ['fim_vigencia'], ['idx'],
                                    [ini_vig,int(str(values),10)], conn=conn)
                print(query_bd.ini_vigencia.values[0])
                print(query_bd.idx.values[0])
                pass

        print('-> Vigência finalizada aplicaVig<-\n')

    # Função que aplica à uma tabela a regra de vigência e retorna uma tabela com as leis aplicadas
    def rotinaVigencia(self, nome_tab, tab, conn):
        print('A ROTINA COMEÇOU\n')
        ind = 0  # Contador para ter acesso à índices diferentes do atual no loop.

        tab = tab.rename(columns={'Data': 'data_', 'Product': 'product', 'Customer': 'customer', 'Store': 'store',
                                  'Value / day': 'value_day', 'Salesman': 'salesman'})

        print('TABELA QUE SOFRERÁ ALTERAÇÕES DE ROTINA DE VIGÊNCIA')
        print('Quantidade de colunas',len(tab.columns))
        print(tab)
        tab['ini_vigencia'] = 'xxx'
        tab['fim_vigencia'] = 'xxx'

        try:
            #Linhas da tabela que será carregada
            for i, row in tab.iterrows():
                df_bd = pd.read_sql_query('SELECT * FROM RESULTADO_1', conn)
                if df_bd.empty:
                    row['value_day'] = int(str(row['value_day']), 10)
                    row['ini_vigencia'] = row['data_']
                    row['fim_vigencia'] = '31/12/9999'
                    self.carregaLinha(nome_tab, tab.columns.tolist(), row.tolist(), conn=conn)
                    pass
                print('CASO ->> NÃO <<- TENHA O VALOR NA TABELA:\n', (df_bd.isin(row) is False))
                #configuração da linha
                row['value_day'] = int(str(row['value_day']), 10)
                row['ini_vigencia'] = row['data_']
                row['fim_vigencia'] = '31/12/9999'

                #if key not in df_bd or (key in df_bd and (tab.loc[ind, 'value_day'] != df_bd[df_bd.date_ == '31/12/9999'].value_day)):
                print('KEY:',list(set(row)))

                #Caso não exista o valor na tabela
                if (any(df_bd.isin(row)) == True) or (row['value_day'] in df_bd.values):
                    #self.carregaLinha(nome_tab, tab.columns.tolist(), row.tolist(), conn=conn)
                    ind += 1
                    df_bd = pd.read_sql_query('SELECT * FROM RESULTADO_1', conn)
                    print(df_bd)

                #CASO A CHAVE SEJA A MESMA E O VALOR SEJA DIFERENTE
                if (any(df_bd.isin(row)) == False) or (row['value_day'] not in df_bd.values) or (row['product'] not in df_bd.values) :
                    print('CASO ->> TENHA <<- O VALOR NA TABELA:\n',(row.isin(row) is False))
                    self.carregaLinha(nome_tab, tab.columns.tolist(), row.tolist(), conn=conn)
                    self.aplicaVig(row, df_bd,conn)

                    ind += 1
                    pass

        except odb.Error as ex:
            # em caso de algum erro (acesso a índice inexistente e etc) ele retorna os dados do jeito que foram alterados e apra o loop.
            sqlstate = ex.args
            print(sqlstate)

        except EOFError as err:
            print(err)

            #except ValueError as err:
            #    print(err)
            #    print('Não foi possível acessar a posição de algum índice durante a atribuição de valores às colunas, pois não foi necessário')
        print('APLICAÇÃO DA VIGÊNCIA FINALIZADA')

    def rotina2(self,nome_tab, tab, conn):
        print('A ROTINA COMEÇOU')
        ind = 0  # Contador para ter acesso à índices diferentes do atual no loop.

        tab = tab.rename(columns={'Data': 'data_', 'Product': 'product', 'Customer': 'customer', 'Store': 'store',
                                 'Value / day': 'value_day', 'Salesman': 'salesman'})

        tab['ini_vigencia'] = pd.Series(tab['data_'])
        tab['fim_vigencia'] = '31/12/9999'

        df_bd = pd.read_sql_query('SELECT * FROM ['+nome_tab+']', conn)

        final_tab=pd.DataFrame(columns=df_bd.columns)
        final_tab.drop('idx', inplace=True, axis=1)

        #Condição inicial
        if not df_bd.empty:
            df_bd.drop('idx', inplace=True,axis=1)
            new_tab = pd.concat([df_bd, tab], ignore_index=True, sort='False', axis=0)
            new_tab.drop_duplicates('value_day',keep='first',inplace=True)
        else:
            #tab.drop_duplicates('value_day',keep='first',inplace=True)
            tab.drop_duplicates('value_day',keep='first',inplace=True)
            new_tab = tab
        df_list=[]

        nomes = list(set(new_tab['product'].values))

        for nome in nomes:
            var = new_tab[new_tab['product'] == nome]
            print(var)
            df_list.append(var)

        final_list =[]

        #Lista de dataframes distintos
        for dist_df in df_list:
            dist_df.reset_index(inplace=True)

            for i, row in dist_df.iterrows():
                final_list.append(self.logicaVigencia(dist_df))
                pass

        final_tab = pd.concat(final_list,ignore_index=True, sort=False)

        print('FINAL_TAB 127839718269381\n',final_tab)

        final_tab.drop('index', inplace=True, axis=1)
        nomes = list(set(new_tab['product'].values))

        for nome in nomes:
            var = new_tab[new_tab['product'] == nome]
            print(var)
            if var.reset_index(inplace=True) is None:
                df_list.append(var)
            else:
                df_list.append(var.reset_index(inplace=True).drop('index',inplace=True, axis=1))

        for df_l in df_list:
            #linha dataframe que será alterado
            for i,row in df_l.iterrows():
                for j,row_j in final_tab.iterrows():
                    print((row['product'] == row_j['product']) and (row['value_day'] != row_j['value_day']) and (row['ini_vigencia'] != row_j['ini_vigencia']) and (row['fim_vigencia'] != row_j['fim_vigencia']) )
                    if ((row['product'] == row_j['product']) and (row['value_day'] != row_j['value_day']) and (row['ini_vigencia'] != row_j['ini_vigencia']) and (row['fim_vigencia'] != row_j['fim_vigencia']) ):
                        print(list(set(row)))
                        print(row)
                        try:
                            df_l.drop(i,inplace=True,axis=0)
                        except:
                            pass

        final_tab = pd.concat(df_list, ignore_index=True, sort='False', axis=0)
        final_tab.drop_duplicates('value_day', keep='first', inplace=True)
        #final_tab = final_tab[(final_tab['value_day'] != final_tab.value_day)]
        final_tab.sort_values(by='data_',inplace=True, axis=0)
        final_tab.drop('index', inplace=True, axis=1)

        for sample,row in final_tab.iterrows():
            self.carregaLinha(nome_tab, final_tab.columns.tolist(), list(row.values), conn=conn)


    #Muda o nome das colunas
    def logicaVigencia(self, df):
        # aplica vigencia
        for i, row in df.iterrows():
            if any(df[(df['product'] == row['product']) & (df['value_day'] != row['value_day']) & (df['fim_vigencia'] == '31/12/9999')]) in df:
                print('PASSOU!')
                pass
            else:
                try:
                    df.loc[i, 'fim_vigencia'] = df.loc[i + 1, 'ini_vigencia']
                    df.loc[i + 1, 'fim_vigencia'] = '31/12/9999'
                except:
                    pass
        return df

    def mudaNomeColunas(self, df, **kwargs):
        for nome, novo_nome in kwargs.items():
            n_1 = nome
            n_2 = novo_nome
            df = df.rename(columns={n_1: n_2})
        return df

    # Carrega dados para o banco
    def carregaDf(self,nome_tab ,dados ,conn):
        print('CARREGANDO DADOS PARA O BANCO DE DADOS!')

        df_x = self.buscaUltimoRegistro(nome_tab, conn)
        df_x.drop('idx',axis=1,inplace=True)
        new_df = pd.concat([df_x, dados], axis=0, join='outer', ignore_index=True, sort=False)

        print(new_df)

        try:
            for index, dados in new_df.iterrows():
                print(dados.tolist())
                print(new_df.columns.tolist())

                self.carregaLinha(nome_tab, new_df.columns.tolist(), dados.tolist(), conn=conn)

                print('CARREGAMENTO DE DADOS COMPLETO')

        except odb.Error as ex:
            sqlstate = ex.args
            print(sqlstate)

    #Carrega planilha CSV
    def carregaPlanilhaCSV(self, path, **kwargs):
        # dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        plan = pd.read_csv(path, sep=';')
        plan['Data'] = plan['Data'].apply(lambda x: str(x))
        plan = self.mudaNomeColunas(plan,**kwargs)
        return plan

    #Converte as datas para Inglês
    def converteDataParaIngles(self, i):
        i = str(i)
        i = i.replace('/', '-')
        i = i.split('-')
        temp = i[0]
        print(i)
        i[0] = i[2]
        i[2] = temp
        date_string = i[0] + '-' + i[1] + '-' + i[2]
        return date_string

    #Rotina final
    def procedimentoFinal(self, nome_arq, nome_tab,conn,**kwargs):
        print('PROCEDIMENTO - 1')
        tab_bd = self.carregaPlanilhaCSV(nome_arq,**kwargs)
        print('PROCEDIMENTO - 2')
        self.rotinaVigencia(nome_tab = nome_tab, tab = tab_bd, conn=conn)

    # Converte para a precisão numérica do banco de dados e retorna uma tupla (o PyODBC só aceita incerção de dados via tuplas)
    def convertePrecisao(self, *args):
        new_data = []
        for arg in args:
            if type(arg) == int:
                arg = str(arg)
                arg = int(arg, 10)
                new_data.append(arg)
            elif type(arg) == float:
                arg = str(arg)
                arg = float(arg, 64)
                new_data.append(arg)
            else:
                new_data.append(arg)
        return new_data

    #Carrega uma única linha
    def carregaLinha(self, nome_tab, *args, conn):
        # Preparação da query
        param_nome_tab = "INSERT INTO [" + nome_tab + "] ("
        values_param = " VALUES ("
        q_mark = '?'
        prc = "%s"

        # Formatação dos nomes das colunas para query
        for col in args[0]:
            param_nome_tab += col + ","

        # Formatação dos valores para query coloquando question marks
        for value in args[1]:
            values_param += q_mark + ','

        param_nome_tab = param_nome_tab[:-1]
        values_param = values_param[:-1]
        query = param_nome_tab + ")" + values_param + ");"

        # Preparação dos valores para serem inseridos na tabela
        col_val = self.convertePrecisao(args[1])

        try:
            connstr = conn.cursor()
            connstr.execute(query, col_val[0])
            conn.commit()
            print('Linha carregada.')
            return 1

        except IndexError as irr:
            connstr = conn.cursor()
            connstr.execute(query, col_val)
            conn.commit()
            print('Linha carregada.')
            print(irr.args)
            return 1

        except odb.Error as ex:
            sqlstate = ex.args
            print('Problema ao inserir linha')
            print(sqlstate)


