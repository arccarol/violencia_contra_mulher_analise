import pandas as pd

df1 = pd.read_csv("data/dados2025.csv", sep=";")
df2 = pd.read_csv("data/dados2024.csv",sep=";")
df3 = pd.read_csv("data/dados2023.csv", sep=";")

print(df1.head(10))
print(df2.head(10))
print(df3.head(10))

print(df1.columns)
print(df2.columns)
print(df3.columns)

df1["dt_notific"] = pd.to_datetime(df1["dt_notific"], format="mixed", dayfirst=True, errors="coerce")
df2["dt_notific"] = pd.to_datetime(df2["dt_notific"], format="mixed", dayfirst=True, errors="coerce")
df3["dt_notific"] = pd.to_datetime(df3["dt_notific"], format="mixed", dayfirst=True, errors="coerce")

df1["ano"] = df1["dt_notific"].dt.year
df2["ano"] = df2["dt_notific"].dt.year
df3["ano"] = df3["dt_notific"].dt.year

df1["mes"] = df1["dt_notific"].dt.month_name(locale="pt_BR")
df2["mes"] = df2["dt_notific"].dt.month_name(locale="pt_BR")
df3["mes"] = df3["dt_notific"].dt.month_name(locale="pt_BR")

df1 = df1.drop(columns=["dt_nasc"])
df2 = df2.drop(columns=["dt_nasc"])
df3 = df3.drop(columns=["dt_nasc"])

#arrumando os municipios (tranformar em estado)

municipios = pd.read_csv("data/municipios.csv")
estados   = pd.read_csv("data/estados.csv")

import unicodedata

def normalizar(texto):
    if pd.isna(texto):
        return texto
    texto = unicodedata.normalize("NFKD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    return texto.upper().strip()


df1["municipio_norm"] = df1["id_mn_resi"].apply(normalizar)
municipios["municipio_norm"] = municipios["nome"].apply(normalizar)

df1 = df1.merge(
    municipios[["municipio_norm", "codigo_uf"]],
    on="municipio_norm",
    how="left"
)

df1 = df1.merge(
    estados[["codigo_uf", "nome"]],
    on="codigo_uf",
    how="left"
)

df1.rename(columns={"nome": "estado"}, inplace=True)

df1.drop(
    columns=["id_mn_resi", "municipio_norm", "codigo_uf"],
    inplace=True,
    errors="ignore"
)

df1 = df1.dropna(subset=["estado"])

print(df1["estado"].unique())


#df2

df2["municipio_norm"] = df2["id_mn_resi"].apply(normalizar)
municipios["municipio_norm"] = municipios["nome"].apply(normalizar)

df2 = df2.merge(
    municipios[["municipio_norm", "codigo_uf"]],
    on="municipio_norm",
    how="left"
)

df2 = df2.merge(
    estados[["codigo_uf", "nome"]],
    on="codigo_uf",
    how="left"
)

df2.rename(columns={"nome": "estado"}, inplace=True)

df2.drop(
    columns=["id_mn_resi", "municipio_norm", "codigo_uf"],
    inplace=True,
    errors="ignore"
)

df2 = df2.dropna(subset=["estado"])

print(df2["estado"].unique())

#df3

df3["municipio_norm"] = df3["id_mn_resi"].apply(normalizar)
municipios["municipio_norm"] = municipios["nome"].apply(normalizar)

df3 = df3.merge(
    municipios[["municipio_norm", "codigo_uf"]],
    on="municipio_norm",
    how="left"
)

df3 = df3.merge(
    estados[["codigo_uf", "nome"]],
    on="codigo_uf",
    how="left"
)

df3.rename(columns={"nome": "estado"}, inplace=True)

df3.drop(
    columns=["id_mn_resi", "municipio_norm", "codigo_uf"],
    inplace=True,
    errors="ignore"
)

df3 = df3.dropna(subset=["estado"])

print(df3["estado"].unique())

df1 = df1.drop(columns=["les_autop", "ident_gen", "orient_sex"])
df2 = df2.drop(columns=["les_autop", "ident_gen", "orient_sex"])
df3 = df3.drop(columns=["les_autop", "ident_gen", "orient_sex"])

df1 = df1.dropna()
df2 = df2.dropna()  
df3 = df3.dropna()

print(len(df1))
print(len(df2))
print(len(df3))

print(df1.isna().sum())
print(df2.isna().sum())
print(df3.isna().sum()) 


#modificando nomes das colunas para padronização

df1 = df1.rename(columns={
    "dt_notific": "data_notificacao",
    "nu_idade_n": "Idade",
    "cs_sexo": "sexo",
    "cs_raca": "raca_cor",
    "local_ocor": "local_ocorrencia",
    "out_vezes": "outras_vezes",
    "viol_fisic": "violencia_fisica",
    "viol_psico": "violencia_psicologica",
    "viol_sexu": "violencia_sexual",
    "num_envolv": "numero_envolvidos",
    "autor_sexo": "sexo_autor"
    
})

df2 = df2.rename(columns={
    "dt_notific": "data_notificacao",
    "nu_idade_n": "Idade",
    "cs_sexo": "sexo",
    "cs_raca": "raca_cor",
    "local_ocor": "local_ocorrencia",
    "out_vezes": "outras_vezes",
    "viol_fisic": "violencia_fisica",
    "viol_psico": "violencia_psicologica",
    "viol_sexu": "violencia_sexual",
    "num_envolv": "numero_envolvidos",
    "autor_sexo": "sexo_autor"
    
})

df3 = df3.rename(columns={
    "dt_notific": "data_notificacao",
    "nu_idade_n": "Idade",
    "cs_sexo": "sexo",
    "cs_raca": "raca_cor",
    "local_ocor": "local_ocorrencia",
    "out_vezes": "outras_vezes",
    "viol_fisic": "violencia_fisica",
    "viol_psico": "violencia_psicologica",
    "viol_sexu": "violencia_sexual",
    "num_envolv": "numero_envolvidos",
    "autor_sexo": "sexo_autor"
    
})

print(df1.columns)
print(df2.columns)          
print(df3.columns)

# Removendo valores indesejados

df1 = df1[df1["local_ocorrencia"] != "Outro"]
df2 = df2[df2["local_ocorrencia"] != "Outro"]
df3 = df3[df3["local_ocorrencia"] != "Outro"]

df1 = df1[df1["local_ocorrencia"] != "Ignorado"]
df2 = df2[df2["local_ocorrencia"] != "Ignorado"]
df3 = df3[df3["local_ocorrencia"] != "Ignorado"]

df1 = df1[df1["sexo_autor"] != "Ignorado"]
df2 = df2[df2["sexo_autor"] != "Ignorado"]
df3 = df3[df3["sexo_autor"] != "Ignorado"]

df1 = df1[df1["numero_envolvidos"] != "Ignorado"]
df2 = df2[df2["numero_envolvidos"] != "Ignorado"]
df3 = df3[df3["numero_envolvidos"] != "Ignorado"]

print(df1["local_ocorrencia"])

print(df1["sexo_autor"])

print(df1["numero_envolvidos"])

print(df1.columns)
print(df2.columns)
print(df3.columns)

df1 = df1.rename(columns={
    "ano": "ano_ocorrencia",
    "mes": "mes_ocorrencia",
    
})

df2 = df2.rename(columns={
    "ano": "ano_ocorrencia",
    "mes": "mes_ocorrencia",
    
})

df3 = df3.rename(columns={
    "ano": "ano_ocorrencia",
    "mes": "mes_ocorrencia",
    
})

print(df1.columns)
print(df2.columns)
print(df3.columns)

# Removendo linhas com a palavra "ignorado" em qualquer coluna

padrao = r'(?i)ignorado|ignorada'

df1 = df1[~df1.apply(
    lambda linha: linha.astype(str).str.contains(padrao, na=False).any(),
    axis=1
)]

df2 = df2[~df2.apply(
    lambda linha: linha.astype(str).str.contains(padrao, na=False).any(),
    axis=1
)]

df3 = df3[~df3.apply(
    lambda linha: linha.astype(str).str.contains(padrao, na=False).any(),
    axis=1
)]


# JUNTAR OS 3 DATAFRAMES (2023, 2024, 2025)


df_final = pd.concat(
    [df3, df2, df1],  # ordem cronológica
    ignore_index=True
)

print("Total de registros:", len(df_final))
print(df_final.head())
print(df_final.columns)


df_final.to_csv(
    "resultado_tratado_2023_2025.csv",
    index=False,
    encoding="utf-8-sig"
)
