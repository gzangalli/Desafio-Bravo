import os
import logging
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import psycopg2
from dotenv import load_dotenv
from warnings import filterwarnings
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
import pytz

# Configuração do sistema de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

# Ignorar avisos
filterwarnings("ignore")

# Carregar variáveis de ambiente
load_dotenv('.env')

# Configuração do fuso horário para agendamento do job
timezone = pytz.timezone('America/Sao_Paulo')

# Configuração de acesso ao banco de dados
DB_CONFIG = {
    'dbname': os.getenv('DBNAME'),
    'user': os.getenv('DBUSER'),
    'password': os.getenv('DBPASSWORD'),
    'host': os.getenv('DBHOST'),
    'port': os.getenv('DBPORT')
}

# Configuração de acesso ao BigQuery
BIGQUERY_CONFIG = {
    'credential_file': r'arquivos/chave_teste_dev.json',
    'projeto': os.getenv('PROJETO'),
    'dataset': os.getenv('DATASET'),
    'tabelas': os.getenv('TABELA'),
}


def get_bigquery_client():
    """Retorna um cliente BigQuery autenticado."""
    credentials = service_account.Credentials.from_service_account_file(
        BIGQUERY_CONFIG['credential_file'])
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def busca_dados_bravo():
    """Busca dados no BigQuery."""
    try:
        client = get_bigquery_client()
        query = f"SELECT * FROM `{BIGQUERY_CONFIG['projeto']}.{
            BIGQUERY_CONFIG['dataset']}.{BIGQUERY_CONFIG['tabelas']}`"
        resultado = client.query(query)
        df = resultado.to_dataframe()

        logging.info("Busca de dados Bravo concluída.")
        return df
    except Exception as e:
        logging.error(f"Erro ao buscar dados no BigQuery: {e}")
        raise


def inserir_dados_bravo(df):
    """Insere dados da tabela stage bravo no banco de dados PostgreSQL."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.set_client_encoding('UTF8')
        with conn.cursor() as cur:
            tabela = 'pokemon.stg_pokemon_bravo'

            cur.execute(f"""
                TRUNCATE TABLE {tabela}
            """)

            for _, row in df.iterrows():

                cur.execute(f"""
                    INSERT INTO {tabela} (nome, numero, ranking)
                    VALUES (%s, %s, %s)
                """, (row['nome'], row['numero'], row['ranking']))

            logging.info("Inserção de dados Bravo concluída.")


def busca_dados_api():
    """Busca dados na API PokeAPI."""
    all_data = []
    for pokemon_id in pokemon_ids:
        try:
            pokemon_url = f'https://pokeapi.co/api/v2/pokemon/{pokemon_id}/'
            species_url = f'https://pokeapi.co/api/v2/pokemon-species/{pokemon_id}/'

            pokemon_response = requests.get(pokemon_url).json()
            species_response = requests.get(species_url).json()

            tipo = [t['type']['name'] for t in pokemon_response['types']]
            habilidades = [h['ability']['name']
                           for h in pokemon_response['abilities']]
            numero_pokedex = pokemon_response['id']
            geracao = species_response['generation']['name']

            all_data.append({
                'tipo': ', '.join(tipo),
                'habilidades': ', '.join(habilidades),
                'pokedex': numero_pokedex,
                'geracao': geracao
            })
        except Exception as e:
            logging.error(f"Erro ao buscar dados da API para o ID {pokemon_id}: {e}")

    logging.info("Busca de dados API concluída.")
    return pd.DataFrame(all_data)


def inserir_dados_api(df):
    """Insere dados da tabela stage API no banco de dados PostgreSQL."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            tabela = 'pokemon.stg_pokemon_api'

            cur.execute(f"""
               TRUNCATE TABLE {tabela}
            """)

            for _, row in df.iterrows():

                cur.execute(f"""
                    INSERT INTO {tabela} (tipo, habilidades, pokedex, geracao)
                    VALUES (%s, %s, %s, %s)
                """, (row['tipo'], row['habilidades'], row['pokedex'], row['geracao']))

            logging.info("Inserção de dados API concluída.")


def insere_pokemon():
    """Combina dados das tabelas stage e insere/atualiza na tabela fato Pokémon no banco de dados."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                tabela_fato = 'pokemon.fato_pokemon'
                tabela_stage_bravo = 'pokemon.stg_pokemon_bravo'
                tabela_stage_api = 'pokemon.stg_pokemon_api'

                # Combinar os dados das tabelas stage
                cur.execute(f"""
                    WITH dados_combinados AS (
                        SELECT
                            sa.pokedex,
                            sb.nome,
                            sb.ranking,
                            sa.tipo,
                            sa.habilidades,
                            sa.geracao
                        FROM {tabela_stage_bravo} sb
                        INNER JOIN {tabela_stage_api} sa
                        ON sb.numero = sa.pokedex
                    )
                    SELECT * FROM dados_combinados;
                """)

                dados = cur.fetchall()

                # Verificar se os dados existem na tabela de fato e realizar INSERT ou UPDATE
                for row in dados:
                    pokedex = row[0]
                    nome = row[1]
                    ranking = row[2]
                    tipo = row[3]
                    habilidades = row[4]
                    geracao = row[5]

                    # Verifica se o Pokémon já está na tabela de fato
                    cur.execute(
                        f"SELECT COUNT(*) FROM {tabela_fato} WHERE pokedex = %s", (pokedex,))
                    existe = cur.fetchone()[0] > 0

                    # Se existir, atualiza os dados. Se não, insere
                    if existe:
                        cur.execute(f"""
                            UPDATE {tabela_fato}
                            SET nome = %s, tipo = %s, habilidades = %s, geracao = %s, ranking = %s
                            WHERE pokedex = %s
                        """, (nome, tipo, habilidades, geracao, ranking, pokedex))
                    else:
                        cur.execute(f"""
                            INSERT INTO {tabela_fato} (pokedex, nome, tipo, habilidades, geracao, ranking)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (pokedex, nome, tipo, habilidades, geracao, ranking))

                logging.info(
                    "Inserção/atualização de dados Pokémon na tabela fato concluída.")

    except Exception as e:
        logging.error(f"Erro ao inserir dados Pokémon: {e}")
        raise


# Função para agendar a execução
def atualizacao_diaria():
    try:
        # Busca de dados
        df_bravo = busca_dados_bravo()
        global pokemon_ids
        pokemon_ids = df_bravo['numero'].tolist()
        df_api = busca_dados_api()

        # Inserção de dados
        inserir_dados_bravo(df_bravo)
        inserir_dados_api(df_api)
        insere_pokemon()
    except Exception as e:
        logging.error(f"Erro no processamento principal: {e}")


# Inicialização do agendamento
if __name__ == '__main__':
    scheduler = BackgroundScheduler()

    scheduler.add_job(atualizacao_diaria, CronTrigger(hour=21, minute=53, timezone=timezone))

    scheduler.start()

    logging.info("Agendamento de tarefa iniciado.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
