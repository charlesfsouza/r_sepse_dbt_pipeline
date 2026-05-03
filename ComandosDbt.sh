
git checkout -b develop

python -m venv venv
source venv/Scripts/activate

source .env

pip install dbt-duckdb  
pip install -r requirements.txt

# dbt init


git checkout -b main
echo "# r_sepse_dbt_pipeline" >> README.md
git add README.md

git commit -m "first commit"
git branch -M main
git push -u origin main


dbt deps
dbt debug

git status
git add .
git commit -m 'criacao dos modelos de staging'

git push --set-upstream origin develop


git pull orgin main



dbt debug
dbt seed

# 
dbt run --select models/staging
dbt run --select models/staging/stg__sexo.sql


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

