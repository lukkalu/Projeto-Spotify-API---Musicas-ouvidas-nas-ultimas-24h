#!/usr/bin/env python
# coding: utf-8

# Importando as bibliotecas

import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import pytz

# Definição constantes

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Lucas Heluany"
TOKEN = "BQBkVdQWEqaYDf86UtZSjByeNBAj65Poi6gVtoId4oYXQHY4jPhLR7Sw6bkOO-17FtXR5TvkEJ6yK_61H-fzGr9eVaVzRLjnSe03M2pkUUrXGluGwozraUpLa-8aG3Q6zFtYzLT-vi0xnk0Wuc95i56eo5La-3n3-BSqlizlaDB57VpMB52uVZQp"

# Criando as funções de check

def check_if_valid_data(df: pd.DataFrame) -> bool:
# Checando se o DF está nulo
    if df.empty:
        print("Nenhuma música foi baixada. Finalizando execução")
        return False
    
# Checando chave primária
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Chave primária violada")
        
# Checando por unulos
    if df.isnull().values.any():
        raise Exception("Valores nulos encontrados")

# Checando que todos os timestamps são de ontem
    #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    #timestamps = df["timestamp"].tolist()
    #for timestamp in timestamps:
    #    if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #        raise Exception("Pelo menos uma das músicas retornadas não tem o timestamp de ontem")
            
    return True

# Definição dos cabeçalhos da requisição

headers = {
    "Accept" : "application/json",
    "Content-Type" : "application/json",
    "Authorization" : "Bearer {TOKEN}".format(TOKEN = TOKEN)
}


# Padroniza o tempo para o timestamp do Unix em milisegundos
today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days = 1)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

# Definição da Request

r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

# Definindo os arrays com as informações que vamos usar

data = r.json()

song_names = []
artist_names = []
played_at_list = []
timestamps = []

# Slice do JSON para os dados que vamos usar

for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

# Criando o dicionário para transformar num dataframe

song_dict = {
    "song_name" : song_names,
    "artist_name" : artist_names,
    "played_at" : played_at_list,
    "timestamp" : timestamps
}

# Criando o DF

song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

# Validando os dados
if check_if_valid_data(song_df):
    print("Dados válidos. Seguir para o processo de carga.")

# Carregando

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""

cursor.execute(sql_query)
print("Banco aberto com sucesso")

try:
    song_df.to_sql("my_played_tracks", engine, index = False, if_exists='append')
except:
    print("Dados já existem no banco!")
    
conn.close()

print("Banco fechado com sucesso!")
