Este repositório contém a solução desenvolvida para o desafio técnico de Engenharia de Dados da Magazord. O objetivo foi criar um pipeline de dados robusto para migrar dados transacionais do MongoDB para um Data Warehouse no PostgreSQL, estruturando os dados para facilitar relatórios financeiros e comerciais.

### Arquitetura da Solução

O pipeline foi desenvolvido em Python e orquestrado via Docker. A arquitetura de dados segue o padrão Medallion Architecture (Bronze, Silver, Gold), implementada diretamente no PostgreSQL para garantir performance e organização dos dados.

## Fluxo de Dados (ELT)

**Extração (Source -> Bronze):**
- Coleta de dados brutos do MongoDB (coleções sales, products, users) utilizando Python (pymongo).
- Carga no PostgreSQL em tabelas Bronze (raw_carts, raw_products, raw_users) mantendo o formato original em colunas JSONB.

**Transformação (Bronze -> Silver):**
- Utilização de SQL nativo com operadores JSON (->>, jsonb_to_recordset) para "explodir" arrays aninhados..
- Limpeza de tipos (Casts para NUMERIC, INT, TIMESTAMP).
- Deduplicação: Uso de DISTINCT ON ordenado pela data de extração para garantir o processamento apenas do registro mais recente.
- Armazenamento em tabelas físicas na camada Silver.

**Apresentação (Silver -> Gold):**
- Criação de View analítica na camada Gold (gold.fat_vendas) para facilitar o consumo por ferramentas de BI, com joins pré-resolvidos e métricas calculadas.

**Modelagem de Dados**

A modelagem foi dividida em esquemas lógicos dentro do PostgreSQL (bronze, silver e gold).

**Camada Silver (Tabelas Físicas Tratadas)**

Nesta camada, os dados estão normalizados, tipados e limpos.

- **silver.sales_items (Tabela Fato):**
    - Origem: bronze.raw_carts.
    - Granularidade: Item do pedido. Os arrays de produtos dentro do carrinho JSON foram explodidos para linhas individuais.
    - Conteúdo: Métricas financeiras (quantity, price_unit, total_gross, discount_pct, total_net) e chaves estrangeiras.
    - Chave Primária Composta: order_id + product_id.

- **silver.products (Dimensão):**
    - Origem: bronze.raw_products.
    - Conteúdo: Catálogo de produtos com atributos descritivos (title, brand, category) e logísticos (weight, dimensions).
    - Tratamento: Achatamento de objetos aninhados (ex: dimensions -> width, height).

- **silver.users (Dimensão):**
    - Origem: bronze.raw_users.
    - Conteúdo: Dados cadastrais de clientes (first_name, email) e geográficos (cidade, estado, pais).

**Camada Gold (Views de Negócio)**

Focada em facilitar consultas analíticas do usuário final.

- **gold.fat_vendas:**
    - View que unifica sales_items com products.
    - Calcula métricas derivadas, como valor_desconto (Bruto - Líquido).
    - Traz descrições de produtos (Categoria, Marca) diretamente na linha da venda para filtros rápidos em Dashboards.

**Requisitos Não-Funcionais Atendidos**

Decisões técnicas tomadas para garantir a robustez e a qualidade da engenharia de software:

1. **Idempotência e Consistência**

- O pipeline roda ou não roda. Se houver falhar durante o processamente dos dados, ele será finalizado e não gerará novos arquivos.
- O pipeline foi projetado para ser executado múltiplas vezes sem duplicar dados, mesmo em caso de reprocessamento.
- Deduplicação na Leitura: Uso de DISTINCT ON (id) ORDER BY extraction_date DESC ao ler da camada Bronze, garantindo que apenas a versão mais recente do registro seja processada.
- Upsert (On Conflict): As cargas na camada Silver utilizam a cláusula ON CONFLICT DO UPDATE, garantindo que registros existentes sejam atualizados (ex: mudança de preço ou estoque) e novos sejam inseridos.

2. **Observabilidade e Logs**

- Utilização da biblioteca logging do Python.
- Monitoramento detalhado do início e fim de cada etapa da extração e carga.
- Tratamento de exceções para garantir que falhas de conexão sejam reportadas corretamente.

3. **Segurança e Configuração**

- Credenciais de banco de dados gerenciadas via variáveis de ambiente (.env) e injetadas pelo Docker Compose.
- Nenhuma senha ou chave de acesso está hardcoded nos scripts.

**Como Executar o Projeto**

Todo o ambiente está containerizado.

**Pré-requisitos**

- Docker instalado.

**Passo a Passo**

Clone o repositório:

```bash
git clone https://github.com/Meepyss/data_engineer_challenge
```

Suba o ambiente:

O comando abaixo inicia o MongoDB e o PostgreSQL. Nota: O PostgreSQL executa automaticamente os scripts de DDL (criação de schemas/tabelas) e a carga inicial localizados na pasta mapeada, já populando o Data Warehouse.

```bash
docker-compose up -d --build
```
Aguarde até que o container e os volumes estejam prontos.

Acesse os Dados:

Conecte-se ao PostgreSQL (localhost:5432) (DBeaver, pgAdmin) e consulte a view final:

```sql
SELECT * FROM gold.fat_vendas LIMIT 10;
```

Caso queria verificar os scripts SQL, os mesmos se encontram em python_script/sql.

(Opcional) Executar o Pipeline Manualmente:

Caso queira ver o script Python rodando e processando novos dados:

```bash
docker exec -it etl_runner python python_script/extract_data.py
```


Feito por Rodrigo Adriano Kreusch
