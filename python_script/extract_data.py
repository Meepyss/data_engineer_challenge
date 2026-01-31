import logging
from datetime import datetime
from bson import ObjectId
from db_connections import load_config, conectar_mongo, conectar_postgres
from psycopg2.extras import Json

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


#Função de serializar os ObjectsID que são armazenados em tipos especiais do BSON.
def json_serial(doc):
    """Converte documentos do MongoDB para tipos serializáveis em JSON sem usar comprehensions."""
    
    # Se for uma lista, iteramos e chamamos a função recursivamente
    if isinstance(doc, list):
        lista_auxiliar = []
        for item in doc:
            lista_auxiliar.append(json_serial(item))
        return lista_auxiliar

    # Se for un dicionário, tratamos cada par chave-valor
    if isinstance(doc, dict):
        dicionario_auxiliar = {}
        for k, v in doc.items():
            if isinstance(v, ObjectId):
                dicionario_auxiliar[k] = str(v)
            else:
                dicionario_auxiliar[k] = json_serial(v)
        return dicionario_auxiliar

    # Caso base: se não for lista nem dict, retorna o próprio valor
    return doc

def extract_load():
    config = load_config()
    
    mongo_client = conectar_mongo(config)
    pg_conn = conectar_postgres(config)
    
    if not mongo_client or not pg_conn:
        logger.error("Falha ao conectar aos bancos de dados. Abortando.")
        return

    pg_cursor = None
    try:
        mongo_db = mongo_client[config["MONGO_DB"]]
        collections = mongo_db.list_collection_names()
        
        pg_cursor = pg_conn.cursor()
        
        extraction_date = datetime.now()
        
        for coll_name in collections:
            logger.info(f"Processando coleção: {coll_name}")
            
            # 1. Garantir que a tabela existe no Postgres (Raw/Staging)
            table_name = f"raw_{coll_name}"
            create_table_query = f"""
                CREATE SCHEMA IF NOT EXISTS bronze;
                CREATE TABLE IF NOT EXISTS bronze.{table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    _extraction_date TIMESTAMP,
                    _source_file_or_id TEXT
                );
            """
            pg_cursor.execute(create_table_query)
            
            # 2. Extrair dados do MongoDB
            cursor = mongo_db[coll_name].find({})
            
            # 3. Inserir no Postgres (Append-only)
            count = 0
            for doc in cursor:
                source_id = str(doc.get('_id'))
                # Converter  BJSON para JSON serializable (Por causa do ObjectId)
                clean_doc = json_serial(doc)
                
                insert_query = f"""
                INSERT INTO bronze.{table_name} (data, _extraction_date, _source_file_or_id) 
                VALUES (%s, %s, %s);
                """
                pg_cursor.execute(insert_query, (Json(clean_doc), extraction_date, source_id))
                count += 1
            
            pg_conn.commit()
            logger.info(f"Coleção {coll_name}: {count} documentos carregados com sucesso em {table_name}.")
            
    except Exception as e:
        logger.error(f"Erro crítico durante o processo ELT. Abortando pipeline: {e}")
        if pg_conn:
            pg_conn.rollback()
        
    finally:
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
        if mongo_client:
            mongo_client.close()

if __name__ == "__main__":
    extract_load()
