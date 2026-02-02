#!/bin/bash
set -e

echo "Iniciando importação de dados para o MongoDB..."

for file in /data/json_files/*.json; do
    filename=$(basename "$file")
    collection_name="${filename%.*}"

    echo "Importando $file para collection '$collection_name'..."

    mongoimport --host localhost \
                --port 27017 \
                --username "$MONGO_INITDB_ROOT_USERNAME" \
                --password "$MONGO_INITDB_ROOT_PASSWORD" \
                --authenticationDatabase admin \
                --db "$MONGO_INITDB_DATABASE" \
                --collection "$collection_name" \
                --type json \
                --file "$file" \
                --jsonArray \
                --drop

    echo "Collection '$collection_name' importada com sucesso!"
done

echo "Importação concluída!"