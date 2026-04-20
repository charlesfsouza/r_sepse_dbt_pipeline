
git checkout -b develop

python -m venv venv
source venv/Scripts/activate

pip install -r requirements.txt





dbt init
source .env


dbt debug

dbt deps

echo $DBT_DBNAME


# APOS INSERIR AS SEEDS

dbt debug
dbt seed

# 
dbt build --select stg_clientes
dbt build --select stg_avaliacoes

dbt run --select models/staging/

dbt run --select stg_produtos

dbt run --select models/staging/
dbt build --select stg_carrinho


# staging

dbt build --select models/intermediate/

dbt run --select models/

git status
git add .
git commit -m 'criacao dos arquivos de staging'

git push --set-upstream origin develop


# intermediaria

git status
git add .
git commit -m 'criacao dos arquivos da camada intermediaria'

git push --set-upstream origin develop

# marts
dbt build --select models/
dbt run --select models/



git status
git add .
git commit -m 'criacao estrategia incremental e testes personalizados'
git push --set-upstream origin develop

dbt docs generate
dbt docs serve --host 127.0.0.1

# increl

dbt run --select models/staging/incremental.sql


# apos criar o arquivo country_codes.csv na pasta seed
´
dbt seed

#Projeto um
echo "# dbt_udemy_projeto_1" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin git@github.com:charlesfsouza/dbt_udemy_projeto_1.git

