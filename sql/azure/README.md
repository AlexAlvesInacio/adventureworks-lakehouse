# Scripts do Azure SQL

Scripts de preparação do banco `sqldb-adventureworks-staging`. Nenhuma senha é versionada.

| Arquivo | Conteúdo |
|---|---|
| `01_ctl_config_tabela.sql` | DDL da tabela de controle `ctl.ConfigTabela` (gerar a partir do banco, ver abaixo) |
| `02_usuario_databricks_leitura.sql` | Usuário somente leitura usado pela conexão federada do Databricks |

## Como gerar o DDL da tabela de controle

O DDL deve vir do banco real, para não divergir do que o Data Factory usa.
No SSMS ou no Azure Data Studio, clique com o botão direito em `ctl.ConfigTabela`,
escolha **Script Table as > CREATE To > New Query Editor Window** e salve o resultado
como `01_ctl_config_tabela.sql`. Inclua também o `INSERT` com o cadastro das tabelas,
se quiser recriar o ambiente do zero.
