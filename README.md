# Exemplo Sql Server - MinIO
Exemplo para buscar informações no Sql Server e gravar no MinIO(S3).


## DOC

### Referência
- https://docs.min.io/docs/python-client-api-reference.html


### Exemplo .ENV

```
#Database
SERVER = xxx
DATABASE = xxx
DRIVER = ODBC+Driver+17+for+SQL+Server
USERNAME_LOGIN = xxx
PASSWORD = xxx

#MinIO
MINIO = xxx(URL)
ACCESS_KEY = xxx
SECRET_KEY = xxx
LANDING = xxx
```