# Projeto
Este repositório mantém um projeto de Engenharia de Dados que visa coletar dados do cenário econômico brasileiro. Inicialmente, a maior parte dos dados é sobre empresas listadas na B3 (Brasil, Bolsa, Balcão).

O projeto consiste em coletar dados usando python e escrevê-los no datalake S3, fazendo então a limpeza e transformação necessária dos dados. Assim que os dados estiverem disponíveis no datalake, eles serão transformados em um Data Lake House usando Delta e Pyspark. Todo o ambiente será baseado em contêineres Docker orquestrados por operadores kubernetes no Airflow.

## Status do projeto:
| Etapa | Estado |
| ------| ------ |
| Python - ELT Data Lake | Feito |
| Docker_python | Feito |
| Docker_spark | Feito |
| Data Lake | Feito |
| Exploração dos dados | feito |
| Data LakeHouse | Em andamento |
| Data Vault | Em andamento |
| Airflow | Pendente |
| Cluster Kubernetes | Pendente |

## Configuração do ambiente (Etapa Datalake).
A primeira etapa para configurar o ambiente é clonar este repositório.
Após clonar, crie uma pasta chamada secrets, dentro da pasta builder, e dentro dessa pasta crie um arquivo chamado .env. Após criá-lo, registre suas credenciais AWS conforme abaixo:
```
aws_access_key_id=suaaccesskey
aws_secret_access_key_id=suasecretaccesskey
aws_region=us-east-2
```
Certifique-se alterar a região para a sua região preferida, e de não fazer upload desse arquivo para repositórios públicos. Pois suas credenciais podem ser utilizadas maliciosamente por terceiros.
Feito isso, já é possível executar o processo em Docker container. Basta ajustar para **local**, o valor do dicionário workdir nos módulos conn_builder e extract_builder.

Mas caso queira executar localmente, siga os passos a seguir:
No módulo params.py, acesse o dicionário chamado workdir e altere o caminho da chave **local** inserindo o caminho para a pasta DATALAKEHOUSE_DADOS_ECONOMICOS na máquina local. Faça o mesmo no arquivo main.py, ajustando a variável pwd.
Logo após, ajuste para local, o valor do dicionário workdir nos módulos conn_builder e extract_builder.




Para evitar problemas de compatibilidade de dependências, será usado o Docker para conteinerizar a aplicação.
Para padronizar, chamaremos a imagem de elt_app com a versão latest. <br>
```docker build -t elt_app:latest -f Dockerfile_python . ```

Dockerfile
``` 
FROM python:3.9-bullseye

RUN mkdir -p /workspaces/app
RUN apt update 
RUN apt install nano

COPY /builder /workspaces/app/
COPY /requirements.txt /workspaces/app/requirements.txt
COPY /main.py /workspaces/app/environment.py


WORKDIR /workspaces/app
RUN pip install -r requirements.txt
RUN pip install --no-cache-dir cryptography

ENV PYTHONPATH=/usr/local/lib/python3.9/dist-packages
ENV CONTAINERPATH=/workspaces/app 
```

Após construir a imagem você poderá acessá-la e checar alguns detalhes:

``` docker run -it ***containerID*** bash ```
Ao executar o comando acima você pode verificar o container criado, usando shell:
pwd: mostrará o diretório de trabalho, que deve ser /workspaces/app
ls: mostrará dois arquivos e uma pasta, sendo main.py, requirements.txt e a pasta builder.

## Extração dos dados para o S3
O arquivo main.py faz a chamada das classes utilizadas para extrair os dados da fonte e salvar no S3, criando a pasta do dia e salvando o arquivo. <br>
O Datalake contém as camadas landing e processed, sendo que a landing recebe os arquivos sem alteração na estrutura. A camada processed recebe os arquivos da landing convertidos em csv com encoding UTF-8. <br>
No S3 também existem as camadas bronze, silver e gold, que servem de abstração para abordagem Data Lakehouse, que serão explicadas na sessão ***Data Lakehouse***. Os módulos contidos na pasta builder movimentam os dados até a camada bronze, salvando-os em formato parquet. <br>
Módulos:
Dentro da pasta builder, estão os módulos responsáveis por toda a criação da extração até a camada bronze. Estes módulos estão segmentados de acordo com sua finalidade, por exemplo o módulo conn_builder contém as classes desenvolvidas para estabelecer conexões. O módulo dir_builder contém as classes desenvolvidas para a estruturação de diretórios, bem como o módulo extract_builder contém as classes criadas para as extrações. 
No módulo conn_builder você encontrará a criação de conexão com AWS, GCP, Mongo e MySQL, embora algumas classes ainda não estejam em uso, foram desenvolvidas para etapas futuras.
Para executar o arquivo main.py, execute o seguinte comando: <br>
``` docker run containerID python main.py ```
Ao executar o comando serão apresentados os logs de cada etapa do processo, e por fim você terá os arquivos armazenados no S3 conforme esperado. Caso tenha algum problema na execução, consulte novamente a sessão configuração de ambiente, apresentada ateriormente. Erros comuns são:
Access Denied: Esse erro aponta que o script não localizou as credenciais AWS ou elas estão invélidas, e as possíveis causas são: <br>
1 - A pasta secrets não foi criada e devidamente abastecida com as credenciais AWS
2 - A pasta secrets foi criada, porém as credenciais estão inválidas.
3 - O caminho para a pasta secrets, registrado no módulo conn_builder está errado.
4 - O dicionário workdir dentro do módulo params não está apontando para o local raiz exato para o projeto. Veja um exemplo da definição do caminho para o arquivo .env:
```
pwd = params.workdir['local']

dotenv_path = Path(fr'{pwd}/builder/secrets/.env')
```
A variável pwd recebe o caminho do root, que pode ser local ou container. Este valor vem do dicionário workdir contido no módulo params.py. Caso você tente executar localmente, o parâmetro do dicionário deverá ser **local** e o valor do dicionário deverá ser o caminho para a pasta DATALAKEHOUSE_DADOS_ECONOMICOS clonada na sua máquina.
Caso for executar em container Docker, não é necessário alterar o caminho, pois ele já é definido como WORKDIR durante a montagem da imagem Dockerfile_python. 

# Primeiras análises dos dados
Com os dados do Datalake, foram feitas algumas análises em SQL e POWER BI.
[Power BI](https://github.com/josecarlos-dataengineer/DataLakehouse_Dados_Economicos/blob/main/SQL/Analise.md#power-bi)
[SQL](https://github.com/josecarlos-dataengineer/DataLakehouse_Dados_Economicos/blob/main/SQL/Analise.md#power-bi)





