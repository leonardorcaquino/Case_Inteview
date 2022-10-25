import pandas as pd

path1 = r'GL 012021.csv'
path2 = r'GL 022021.csv'
path3= r'GL 032021.csv'

# Função para tratamento de cada arquivo GL 
def tratamento(caminho):
    df = pd.read_csv(caminho,sep=';')

    # Tratamento da coluna data
    df2 = pd.Series(df['DATA'], name='Date')
    df2 = df2[(df2.str.contains('/')) & (df2.str.contains('-') == False) ]
    ###################################################


    # Tratamento da coluna 'LOTE/SUB/DOC/LINHA' 
    df3 = pd.Series(df['LOTE/SUB/DOC/LINHA'], name='ExternalReference')
    df3 = df3.dropna()
    df3 = df3[((df3.str.contains('D')) | (df3.str.contains('-'))) == False ]
    ###################################################

    # Tratamento da coluna 'HISTORICO' 
    df4 = df[['HISTORICO','FILIAL DE ORIGEM']]
    df4 = df4[df4['HISTORICO'].notnull()]
    df4['FILIAL DE ORIGEM'] = df4['FILIAL DE ORIGEM'].fillna('2')
    df4 = df4[df4['HISTORICO'] !='HISTORICO' ]
    indexs = df4.index
    col1 = df4['HISTORICO'].values
    col2 = df4['FILIAL DE ORIGEM'].values
    corrigido = []
    for i in range(df4.shape[0]-1):
        valor =  str(col1[i])
        if str(col2[i]) == '1' and str(col2[i+1]) == '2':
            valor = str(col1[i]) + str(col1[i+1])
        corrigido.append([valor,str(col1[i]),str(col2[i])])
    corrigido.append([str(col1[-1]),str(col1[-1]),str(col2[-1])])
    df4 = pd.DataFrame(corrigido, columns=['EntryDescription','HISTORIC', 'FILIAL DE ORIGE'])
    df4 = df4.set_index(indexs)
    ###################################################


    # Tratamento da coluna NUMEROS DAS CONTAS
    df5 = pd.Series(df['DATA'], name='AccountNumber')
    df5 = df5[df5.str.contains('CONTA - ') == True].apply(lambda x : x[7:16])
    ###################################################


    # Tratamento da coluna DESCRIÇÃO DAS CONTAS
    df0 = pd.Series(df['DATA'], name='AccountNumber1')
    df0 = df0[df0.str.contains('CONTA - ') == True]
    aux = df0.apply(lambda x : x[7:16])
    aux2 = df0.apply(lambda x : x[18:])
    aux2 = aux2.str.replace("-","")
    aux2 = aux2.str.strip()
    df_aux=pd.concat([aux,aux2],axis=1)
    df_aux.columns = ['AccountNumber1', 'Description']
    ###################################################

    # Tratamento da coluna SALDO ANTERIOR
    df8 = pd.Series(df['LOTE/SUB/DOC/LINHA'], name='PreviousBalance')
    df8 = df8[df8.str.contains('SALDO ANTERIOR:') == True]
    df8 = df8.str.replace(" ","")
    df8 = df8.str.replace("SALDOANTERIOR:","")
    ###################################################


    # Juntando as colunas tratadas na tabela original df
    df = pd.merge(df, df8,how='left', left_index=True, right_index=True)
    df = pd.merge(df, df5,how='left', left_index=True, right_index=True)
    df = pd.merge(df, df2,how='left', left_index=True, right_index=True)
    df = pd.merge(df, df3,how='left', left_index=True, right_index=True)
    df = pd.merge(df, df4,how='left', left_index=True, right_index=True)
    ###################################################

    # Fazendo preenchimento para baixo das informações
    df['PreviousBalance'] = df['PreviousBalance'].fillna(method="ffill")
    df['AccountNumber'] = df['AccountNumber'].fillna(method="ffill")
    df['Date'] = df['Date'].fillna(method="ffill")
    df['ExternalReference'] = df['ExternalReference'].fillna(method="ffill")
    ###################################################

    df = pd.merge(df, df2,how='inner', left_index=True, right_index=True)

    # Retirando algumas colunas desnecessárias no momento
    df.drop(['DATA', 'LOTE/SUB/DOC/LINHA', 'HISTORICO','HISTORIC', 'FILIAL DE ORIGE','Date_y', 'ITEM CONTA', 'DIVISAO'], axis=1, inplace=True)
    ###################################################

    # Renomeando colunas
    df.columns = ['CounterpartAccount', 
                    'InternalReference ', 
                    'CostCenter',
                    'Debit', 
                    'Credit', 
                    'PreviousBalance', 
                    'AccountNumber', 
                    'Date',
                    'ExternalReference',
                    'EntryDescription']
    ###################################################
                    
    
    df = df.merge(df_aux, left_on='AccountNumber', right_on='AccountNumber1')

    
    # Retirando algumas colunas desnecessárias no momento
    df.drop(['AccountNumber1'], axis=1, inplace=True)
    ###################################################

    # Renomeando colunas
    df.columns = ['CounterpartAccount', 
                    'InternalReference ', 
                    'CostCenter',
                    'Debit', 
                    'Credit', 
                    'PreviousBalance', 
                    'AccountNumber', 
                    'Date',
                    'ExternalReference',
                    'EntryDescription',
                    'AccountDescription']
    ###################################################

    # Reordenando colunas
    df = df[[ 
                    'AccountNumber', 
                    'AccountDescription',
                    'CostCenter',
                    'CounterpartAccount',
                    'EntryDescription',
                    'ExternalReference',
                    'InternalReference ', 
                    'Date',
                    'Credit', 
                    'Debit', 
                    'PreviousBalance', 
                    ]]
    ###################################################

    # Substituindo NAN valores por "0" na colunas Credito e debito e fazendo filtro de linhas desnecessárias
    df['Credit'] = df['Credit'].fillna("0")
    df['Debit'] = df['Debit'].fillna("0")
    df = df[(df['Credit'] != str(0)) | (df['Debit'] != str(0))  ]
    ###################################################

    # Fazendo conversão de tipo nas colunas data, credito e débito e cálculo de Balance
    df['Date'] = pd.to_datetime(df['Date'])
    df['Debit'] = df['Debit'].str.replace(",",".")
    df['Debit'] = df['Debit'].astype(float)
    df['Credit'] = df['Credit'].str.replace(",",".")
    df['Credit'] = df['Credit'].astype(float)
    df['Balance'] = df['Debit'] - df['Credit']
    df['PreviousBalance']=df['PreviousBalance'].apply(lambda x: "-"+ str(x[:-1]) if str(x[-1:]) == 'D' else str(x[:-1]))
    ###################################################
    
    return df


# Concatenando(fazendo um UNION) as colunas tratadas pela função tratamento
df = pd.concat([
                tratamento(path1),
                tratamento(path2),
                tratamento(path3)
                ])
###################################################

df.drop_duplicates()

# Tratamento de Mcur
mcur = pd.read_csv(r'mcur.csv',sep=";")
mcur.drop(['cur_to', 'ex_rate', 'f_from', 'data_version'], axis=1, inplace=True)
mcur.columns = ['ExchangeType', 
                'Currency', 
                'Date1',
                'Tax'
                ]

mcur['Date1']=mcur['Date1'].apply(lambda x: str(x) + "-01")
mcur['Date1'] = pd.to_datetime(mcur['Date1'])
###################################################

# Tratamento de Dim Account
dim = pd.read_csv(r'DIM_Account.csv',sep=";")
dim = dim[dim['ACCOUNT'].str.contains('not') == False]
###################################################

# Fazendo JOIN com a tabela Dim Account e identificando em qual a sua Categoria de conversão 
df['AccountNumber'] = df['AccountNumber'].astype('int32')
dim['ACCOUNT'] = dim['ACCOUNT'].astype('int32')
df = pd.merge(df, dim,how='left', left_on='AccountNumber', right_on='ACCOUNT')
df.loc[df['Category'] == 'BS', 'Ex_rate'] = 'END' 
df.loc[df['Category'] != 'BS', 'Ex_rate'] = 'AVG' 
###################################################


# Fazendo JOIN com a tabela Mcur para pegar o valor da taxa de conversão
df['DateFormat'] = df['Date'].dt.strftime('%Y-%m')
mcur['DateFormatM'] = mcur['Date1'].dt.strftime('%Y-%m')
df = pd.merge(
    df,
    mcur,
    left_on=['Ex_rate','DateFormat'],
    right_on=['ExchangeType','DateFormatM']
)
###################################################

# Cálculo dos valores de credtito, debito e Balance nas respectivas taxas USD
df = df.assign(Credit_T = lambda x : x['Credit']* x['Tax'])
df = df.assign(Debit_T = lambda x : x['Debit']* x['Tax'])
df = df.assign(Balance_T = lambda x : x['Balance']* x['Tax'])
###################################################

# Retirando as colunas desnecessárias
df.drop([   'Credit', 
            'Debit',
            'Balance',
            'ACCOUNT', 
            'MFR', 
            'Managerial', 
            'Acctype', 
            'Category',
            'Ex_rate', 
            'DateFormat', 
            'ExchangeType',
            'Date1',
            'Tax', 
            'DateFormatM'], axis=1, inplace=True)
###################################################

# Renomeando colunas
df.columns = [
                'AccountNumber', 
                'AccountDescription', 
                'CostCenter',
                'CounterpartAccount', 
                'EntryDescription', 
                'ExternalReference',
                'InternalReference ',
                'Date',
                'PreviousBalance',
                'Currency', 
                'Credit',
                'Debit', 
                'Balance'
            ]
###################################################

# Reordenando colunas
df = df[[
                'AccountNumber', 
                'AccountDescription', 
                'CostCenter',
                'CounterpartAccount', 
                'EntryDescription', 
                'ExternalReference',
                'InternalReference ',
                'Date',
                'Balance',
                'Credit',
                'Debit', 
                'Currency', 
                'PreviousBalance'
            ]]
###################################################
# Mudando "." por "," para no arquivo de saida estar no formato BR
# df['PreviousBalance'] = df['PreviousBalance'].str.replace(",",".")

df['Credit']  = df['Credit'].astype(str)
df['Debit']   = df['Debit'].astype(str)
df['Balance'] = df['Balance'].astype(str)

df['Credit']  = df['Credit'].str.replace(".",",")
df['Debit']   = df['Debit'].str.replace(".",",")
df['Balance'] = df['Balance'].str.replace(".",",")
###################################################

df.to_csv(r'UnifiedTable.csv',sep=";",index=False)