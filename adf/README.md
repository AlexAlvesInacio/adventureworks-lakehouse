# Azure Data Factory

Pasta reservada para os recursos do Data Factory `adf-adventureworks-lab-alex`
(pipelines, datasets, serviços vinculados e gatilhos), versionados pela integração Git do próprio ADF.

## Configuração da integração Git

No ADF Studio, em **Gerenciar > Configuração do Git**:

| Campo | Valor |
|---|---|
| Tipo de repositório | GitHub |
| Repositório | este repositório |
| Branch de colaboração | `main` |
| Branch de publicação | `adf_publish` |
| Pasta raiz | `/adf` |

A pasta raiz `/adf` mantém os arquivos do Data Factory separados do código do Databricks.
Chaves do Integration Runtime e senhas dos serviços vinculados não entram no repositório.
