# Teste de ponta a ponta: SQL Server local → Azure SQL → Databricks

**Data:** 20/11/2026 · **Resultado:** aprovado, valor esperado igual ao obtido.

## Objetivo

Comprovar que uma alteração feita no SQL Server local percorre todo o pipeline e chega,
com o valor correto, à camada ouro do Databricks.

## Alteração controlada na origem

Item 13 do pedido 43660 (`Sales.SalesOrderDetail`): `OrderQty` de 1 para 11 e `ModifiedDate = GETDATE()`.
O cabeçalho (`Sales.SalesOrderHeader`) teve o `SubTotal` recalculado e o `ModifiedDate` atualizado,
para que item e pedido entrassem juntos na carga incremental.

| Item | Valor |
|---|---|
| Preço unitário | 419,4589 |
| Desconto | 0 |
| Aumento esperado | 10 × 419,4589 = 4.194,589 |
| Subtotal do pedido antes | 1.294,2529 |
| Subtotal do pedido depois | 5.488,8419 |

## Execução

| Etapa | Ferramenta | Resultado |
|---|---|---|
| 1. Linha de base | Databricks SQL Editor | Receita total na ouro: 109.846.381,40 |
| 2. Alteração na origem | DBeaver, SQL Server local | Item com 11 unidades; pedido com novo subtotal às 14:49 |
| 3. Ingestão | Azure Data Factory, `pl_adventureworks_orquestrador` | 73 atividades com sucesso, cerca de 38 minutos |
| 4. Conferência no staging | Azure SQL, `stg.Sales_SalesOrderDetail` e `stg.Sales_SalesOrderHeader` | Valores novos com data de 20/11/2026 14:49 |
| 5. Transformação | Databricks, `job_adventureworks_lakehouse` | Bronze, prata e ouro com sucesso, 18 min 34 s, validação da ouro aprovada |
| 6. Resultado | Databricks SQL Editor | Receita total na ouro: 109.850.575,99 |

**Conferência:** 109.846.381,40 − 1.294,25 + 5.488,84 = **109.850.575,99**, idêntico ao valor obtido.

## Aprendizados

- A ordem importa: o Databricks lê o staging do Azure, então o Data Factory precisa terminar antes do job.
  Uma primeira execução do job, disparada antes da ingestão, foi cancelada e aparece no histórico.
- A conferência do staging funciona como portão: só se roda o Databricks quando a alteração aparece no Azure.
- O contador do painel, em milhões com duas casas, não mostra variações pequenas; o teste foi comprovado por consulta.
- O orquestrador processa 71 tabelas em sequência; o custo fixo por atividade domina o tempo (cerca de 38 min).

## Próximas melhorias

- Agendar o job do Databricks após o Data Factory, ou acioná-lo pelo próprio Data Factory.
- Paralelizar o ForEach do Data Factory.
- Detectar mudanças de esquema na origem e alertar por e-mail.
- Painel de observabilidade no Grafana.

## Evidências

As capturas estão em [`docs/evidencias`](evidencias/), numeradas na ordem do projeto.
E-mails, endereços IP e identificadores da assinatura foram desfocados nas capturas.
