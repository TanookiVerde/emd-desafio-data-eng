# Desafio de Data Engineer - EMD

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de uma API instantânea. A API consiste nos dados de GPS do BRT que são gerados na hora da consulta com o último sinal transmitido por cada veículo.

Para o desafio, será necessário construir uma pipeline que captura os dados minuto a minuto e gera um arquivo no formato CSV. O arquivo gerado deverá conter no mínimo 10 minutos de dados capturados (estruture os dados da maneira que achar mais conveniente), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá conter o ID do onibus, posição e sua a velocidade.

A pipeline deverá ser construída subindo uma instância local do Prefect (em Python). Utilize a versão *0.15.9* do Prefect.

## Ambiente de Execução

### Preparação

1. Crie um ambiente virtual usando `venv`
    - Windows: `python -m venv .\env`
    - Linux: `python -m venv env`
1. Ative o ambiente virtual
    - Windows: `env\Scripts\Activate.ps1`
    - Linux: `source env/bin/activate`
1. Instale as dependências definidas em `requirements.txt`:
    - Windows: `pip install -r requirements.txt`
    - Linux: `pip install -r requirements.txt`

### Rodando

1. Ative o ambiente virtual
    - Windows: `env\Scripts\Activate.ps1`
    - Linux: `source env/bin/activate`
1. Rode na raiz do projeto o comando: `python run.py`