["PT-BR"]("imagens\Brasil.png")
["EN-US"]("imagens\EUA.png")


# Projeto
Este repositório mantém um projeto de Engenharia de Dados que visa coletar dados do cenário econômico brasileiro. Inicialmente, a maior parte dos dados é sobre empresas listadas na B3 (Brasil, Bolsa, Balcão).

O projeto consiste em coletar dados usando python e escrevê-los no datalake S3, fazendo então a limpeza e transformação necessária dos dados. Assim que os dados estiverem disponíveis no datalake, eles serão transformados em um Data Lake House usando Delta e Pyspark. Todo o ambiente será baseado em contêineres Docker orquestrados por operadores kubernetes no Airflow.

## Status do projeto:
| Etapa | Estado |
| ------| ------ |
| Python - ELT Data Lake | Feito |
| Docker | Pendente |
| Data Lake | Pendente |
| Data LakeHouse | Pendente |
| Data Vault | Pendente |
| Airflow | Pendente |
| Cluster Kubernetes | Pendente |


# Project.

This repository keeps a Data Engineering project that aims to gather data from Braziian economic landscape. Initially, most of data is about companies which are listed on B3 (Brasil, Bolsa, Balcão).

The project consists of collecting data using python applications and writting it on S3 datalake, then making needed data cleaning and tranformations. Once data is available on datalake, it is transformed into a Data Lake House by using Delta and Pyspark. The whole enviroment will be based on Docker containers orchestrated by kubernetes operators in Airflow.

## Project Status.
| Stage | Status |
| ------| ------ |
| Python - ELT DataLake | Feito |
| Docker | Pending |
| Data Lake | Pending |
| Data LakeHouse | Pending |
| Data Vault | Pending |
| Airflow | Pending |
| Cluster Kubernetes | Pending |