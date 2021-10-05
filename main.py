import pandas as pd, datetime

##############################################################################################################################
#  Objetivo: Ler arquivos com códigos das etiquetas de um sistema legado, afim de fazer a migração para um novo sistema
#  Para maiores detalhes do Case, ler arquivo Leia_me.txt
##############################################################################################################################


##############################################################################################################################
# LEITURA DOS ARQUIVOS CSV
##############################################################################################################################
df_lote_forn = pd.read_csv("lote_fornecedor1.csv",sep=";")
df_lote_forn2 = pd.read_csv("lote_fornecedor2.csv",sep=";", encoding = "cp1252")
df_carga = pd.read_csv("carga.csv",sep=";", encoding = "cp1252")

##############################################################################################################################
# CONHECER OS DADOS
##############################################################################################################################

#print(df_lote_forn.shape)
#print(df_lote_forn.head())
#print(df_lote_forn.info())
#print(df_carga.info())
#print(df_carga.head())


##############################################################################################################################
# SEÇÃO: CONVERSÃO DOS TIPOS DE CARACTERES DAS COLUNAS E TRATAMENTO DE DADOS INVÁLIDOS
##############################################################################################################################
df_lote_forn.CD_IDENTIFICADOR = pd.Series(df_lote_forn.CD_IDENTIFICADOR).convert_dtypes(convert_string=True)
df_lote_forn.CD_PRODUTO = pd.Series(df_lote_forn.CD_PRODUTO.astype(str).convert_dtypes(infer_objects=True, convert_string=True ))
df_lote_forn.CD_LOTE = pd.Series(df_lote_forn.CD_LOTE).convert_dtypes(convert_string=True)
df_lote_forn.DT_VALIDADE = pd.to_datetime(df_lote_forn.DT_VALIDADE, errors = 'coerce')
df_lote_forn.DT_VALIDADE = pd.to_datetime(df_lote_forn.DT_VALIDADE.convert_dtypes(convert_string=True))
df_lote_forn.CD_UNI_PRO = pd.Series(df_lote_forn.CD_UNI_PRO.astype(str).convert_dtypes(infer_objects=True, convert_string=True ))
df_lote_forn.DS_UNIDADE = pd.Series(df_lote_forn.DS_UNIDADE).convert_dtypes(convert_string=True)
##############################################################################################################################
df_lote_forn2.CD_IDENTIFICADOR = pd.Series(df_lote_forn2.CD_IDENTIFICADOR).convert_dtypes(convert_string=True)
df_lote_forn2.CD_PRODUTO = pd.Series(df_lote_forn2.CD_PRODUTO.astype(str).convert_dtypes(infer_objects=True, convert_string=True ))
df_lote_forn2.CD_LOTE = pd.Series(df_lote_forn2.CD_LOTE).convert_dtypes(convert_string=True)
df_lote_forn2.DT_VALIDADE = pd.to_datetime(df_lote_forn2.DT_VALIDADE, errors = 'coerce')
df_lote_forn2.DT_VALIDADE = pd.to_datetime(df_lote_forn2.DT_VALIDADE.convert_dtypes(convert_string=True))
df_lote_forn2.CD_UNI_PRO = pd.Series(df_lote_forn2.CD_UNI_PRO.astype(str).convert_dtypes(infer_objects=True, convert_string=True ))
df_lote_forn2.DS_UNIDADE = pd.Series(df_lote_forn2.DS_UNIDADE).convert_dtypes(convert_string=True)
###############################################################################################################################
# No arquivo carga como existem muitas colunas que não serão usadas, optei por reduzir o df, para simplificar o entendimento
###############################################################################################################################
df_carga = df_carga[ ["CODIGO_MV", "LOTE", "DT_VALIDADE"]]
df_carga.CODIGO_MV = pd.Series(df_carga.CODIGO_MV.astype(str).convert_dtypes(infer_objects=True, convert_string=True ))
df_carga.LOTE = pd.Series(df_carga.LOTE.convert_dtypes(infer_objects=True, convert_string=True ))
df_carga.DT_VALIDADE = pd.to_datetime(df_carga.DT_VALIDADE, errors = 'coerce')
df_carga.DT_VALIDADE = pd.to_datetime(df_carga.DT_VALIDADE.convert_dtypes(convert_string=True))


###############################################################################################################################
# Listas para receber o resultado do Processamento dos arquivos
###############################################################################################################################
dic_found = []
dic_not_found = []

###############################################################################################################################
# Processamento do arquivo 1
###############################################################################################################################

df_lote_forn = df_lote_forn

dic_lote_forn  = df_lote_forn.to_dict(orient="records")


for item in dic_lote_forn:

     prod = item["CD_PRODUTO"]
     lote = item["CD_LOTE"]
     validade = item["DT_VALIDADE"]

     df_carga_pesquisa = df_carga.query(f'CODIGO_MV == """{prod}""" & LOTE == """{ lote}""" & DT_VALIDADE == """{validade}"""')

     if len(df_carga_pesquisa)==0:
        dic_not_found.append(item)
     else:
        dic_found.append(item)

###############################################################################################################################
# Processamento do arquivo 2
###############################################################################################################################

df_lote_forn2 = df_lote_forn2

dic_lote_forn2  = df_lote_forn2.to_dict(orient="records")


for item in dic_lote_forn2:

     prod = item["CD_PRODUTO"]
     lote = item["CD_LOTE"]
     validade = item["DT_VALIDADE"]

     df_carga_pesquisa = df_carga.query(f'CODIGO_MV == """{prod}""" & LOTE == """{ lote}""" & DT_VALIDADE == """{validade}"""')

     if len(df_carga_pesquisa)==0:
        dic_not_found.append(item)
     else:
        dic_found.append(item)

###############################################################################################################################
# GERAÇÃO DOS ARQUIVOS CSV PARA A CARGA DA ETIQUETA E OUTRO PARA OS PRODUTOS/LOTES NÃO ENCONTRADOS
###############################################################################################################################

df_etiquetas = pd.DataFrame.from_dict(dic_found)
df_etiquetas.to_csv("etiquetas.csv", index = False, sep=";")


df_not_found = pd.DataFrame.from_dict(dic_not_found)
df_not_found.to_csv("lotes_nao_encontrados.csv", index = False, sep=";")

print("Fim do processamento!!!")
