import logging
import pymongo
import psycopg2
from pymongo.errors import ConnectionFailure
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os


# 1. Configura o "diário" do seu app (Logging)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)



def load_config():
    """Carrega configurações de variáveis de ambiente."""
    load_dotenv()
    config = {
        # MongoDB
        "MONGO_HOST": os.getenv("MONGO_HOST","localhost"),
        "MONGO_PORT": int(os.getenv("MONGO_PORT", 27017)),
        "MONGO_USER": os.getenv("MONGO_USER"),
        "MONGO_PASSWORD": os.getenv("MONGO_PASSWORD"),
        "MONGO_DB": os.getenv("MONGO_DB"),
        
        # Postgres
        "PG_HOST": os.getenv("POSTGRES_DB_HOST", "localhost"),
        "PG_PORT": int(os.getenv("POSTGRES_DB_PORT", 5432)),
        "PG_USER": os.getenv("POSTGRES_USER"),
        "PG_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "PG_DB": os.getenv("POSTGRES_DB")
    }
    
    # Validação de variáveis críticas
    required_vars = ["MONGO_PASSWORD", "POSTGRES_PASSWORD"]
    
    if not required_vars:
        logger.error(f"Variáveis de ambiente não configuradas")
    
    logger.info("Configurações carregadas com sucesso")
    return config


def conectar_mongo(config):
    """Monta a URI de forma segura e conecta ao banco."""
    # Usamos quote_plus para evitar erros de caracteres especiais na senha (RFC 3986)
    user = quote_plus(config["MONGO_USER"])
    pw = quote_plus(config["MONGO_PASSWORD"])
    host = config["MONGO_HOST"]
    port = config["MONGO_PORT"]
    db_name = config["MONGO_DB"]

    # URI Montada corretamente
    uri = f"mongodb://{user}:{pw}@{host}:{port}/{db_name}?authSource=admin/"

    
    try:
        # Tenta a conexão
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        logger.info(f"Conectado ao MongoDB em {host}:{port} com sucesso!\n Usuario {user}")
        return client
        
    except ConnectionFailure:
        logger.error("Falha de conexão: O servidor está offline ou o endereço é inválido.")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
    
    return None


def conectar_postgres(config):
    """Conecta ao banco PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=config["PG_HOST"],
            port=config["PG_PORT"],
            user=config["PG_USER"],
            password=config["PG_PASSWORD"],
            dbname=config["PG_DB"]
        )
        logger.info(f"Conectado ao Postgres em {config['PG_HOST']}:{config['PG_PORT']} com sucesso!")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao Postgres: {e}")
    return None


# if __name__ == "__main__":
#     cfg = load_config()
#     conectar_postgres(cfg)

