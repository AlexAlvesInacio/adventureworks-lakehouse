-- Usuário somente leitura usado pela conexão federada do Databricks
-- (con_azure_adventureworks_sql). Executar no banco sqldb-adventureworks-staging,
-- autenticado como administrador do Microsoft Entra.
--
-- Pré-requisito: o servidor precisa aceitar autenticação SQL
-- (desmarcar "Dar suporte somente à autenticação do Microsoft Entra").
--
-- A senha NÃO é versionada. Substitua o marcador apenas no momento da execução.

CREATE USER databricks_leitura
WITH PASSWORD = '<DEFINIR_NO_MOMENTO_DA_EXECUCAO>';

ALTER ROLE db_datareader ADD MEMBER databricks_leitura;

-- Conferência
SELECT u.name AS usuario, r.name AS papel
FROM sys.database_role_members AS m
INNER JOIN sys.database_principals AS u ON u.principal_id = m.member_principal_id
INNER JOIN sys.database_principals AS r ON r.principal_id = m.role_principal_id
WHERE u.name = 'databricks_leitura';
