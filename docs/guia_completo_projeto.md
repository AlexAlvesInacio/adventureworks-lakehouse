# AdventureWorks Lakehouse: guia completo do projeto, passo a passo

Autor: Alex Alves. Período: novembro de 2026.

Este documento descreve, em ordem e com detalhes, tudo o que foi construído no laboratório AdventureWorks Lakehouse: a origem dos dados na máquina local, a cópia incremental para o Azure, a arquitetura medalhão no Databricks e a organização do código com Git e CI/CD. Ele foi escrito para ser lido do começo ao fim, como um roteiro, e também para servir de base de consulta. Cada etapa explica o que foi feito, por que foi feito, como foi feito e quais problemas apareceram no caminho.

Credenciais, senhas e tokens não aparecem neste documento. Onde eles são necessários, o texto indica apenas onde ficam guardados.

---

## 1. Visão geral

### 1.1 Objetivo

O objetivo do projeto é simular, em ambiente de laboratório, um pipeline de dados completo e realista, como os usados em empresas: os dados nascem num banco de dados operacional, são copiados de forma incremental para a nuvem, passam por camadas de tratamento até virarem um modelo pronto para análise, e todo o código que faz isso é versionado, testado e publicado de forma automática.

O banco usado é o AdventureWorks, uma base de exemplo da Microsoft que simula uma fabricante de bicicletas, com vendas, clientes, produtos, compras, produção e recursos humanos.

### 1.2 Arquitetura em uma frase

O SQL Server da máquina local é a origem; o Azure Data Factory copia os dados, de forma incremental, para um banco Azure SQL, no schema de staging; o Databricks lê esse staging por federação e constrói três camadas, bronze, prata e ouro; e o GitHub guarda o código e publica as mudanças no Databricks por meio do GitHub Actions.

### 1.3 O fluxo dos dados, em ordem

Primeiro, os dados estão no SQL Server instalado na máquina local, no banco AdventureWorks2025.

Segundo, o Azure Data Factory, usando um Integration Runtime auto-hospedado instalado na própria máquina, lê esses dados e grava no Azure SQL, no schema chamado stg. Uma tabela de controle, ctl.ConfigTabela, diz quais tabelas copiar e como.

Terceiro, o Databricks enxerga o Azure SQL por uma conexão federada, como se fosse um catálogo próprio, chamado azure_adventureworks.

Quarto, um job do Databricks executa três notebooks em sequência. A bronze copia o staging para tabelas Delta, de forma incremental. A prata padroniza nomes, tipos e textos. A ouro monta um modelo estrela de vendas.

Quinto, o resultado é consumido por um painel do Databricks e, no futuro, pelo Power BI.

Em paralelo a tudo isso, o código dos notebooks e a definição do job ficam no repositório adventureworks-lakehouse, no GitHub. Mudanças nascem na branch dev, são validadas automaticamente e chegam à branch main por Pull Request. Ao entrar na main, são publicadas no Databricks.

### 1.4 Nomes dos recursos

No computador local: servidor SQL Server com a instância MSSQLSERVER01, acessível em 127.0.0.1, porta 1433, banco AdventureWorks2025. A pasta do repositório fica em D:\Projetos dados\adventureworks-lakehouse.

No Azure: grupo de recursos rg-adventureworks-lab; servidor lógico sql-adventureworks-lab-alex, com endereço sql-adventureworks-lab-alex.database.windows.net; banco sqldb-adventureworks-staging, na oferta gratuita sem servidor; Data Factory adf-adventureworks-lab-alex; Integration Runtime auto-hospedado ir-adventureworks-local.

No Databricks Free Edition: workspace com endereço https://dbc-bf6e5a3f-5188.cloud.databricks.com; catálogo workspace, com os schemas adventureworks_bronze, adventureworks_silver e adventureworks_gold, e os equivalentes de desenvolvimento, com prefixo dev_; catálogo estrangeiro azure_adventureworks; conexão con_azure_adventureworks_sql; jobs job_adventureworks_lakehouse, o estável, e a versão de desenvolvimento, com prefixo dev e o nome do usuário.

No GitHub: repositório AlexAlvesInacio/adventureworks-lakehouse, com as branches main e dev. O repositório anterior, adventureworks-adf-ingestion, ficou como registro da primeira fase, só com o Data Factory.

### 1.5 Tecnologias

SQL Server e T-SQL, na origem. DBeaver, como cliente de banco na máquina local. Azure SQL Database, como staging. Azure Data Factory, para orquestrar a cópia. Databricks Free Edition, com Unity Catalog, Lakehouse Federation, tabelas Delta, PySpark e Spark SQL. GitHub, GitHub Actions e Declarative Automation Bundles, antigamente chamados de Databricks Asset Bundles, para CI/CD. VS Code, Ruff e pytest, para edição, verificação e testes do código.

---

## 2. Parte 1: a origem na máquina local

### 2.1 O que existe na máquina

A origem é um SQL Server instalado no computador local, na instância MSSQLSERVER01, com o banco AdventureWorks2025 restaurado. Ele faz o papel do sistema operacional de uma empresa, aquele onde os dados nascem: pedidos, clientes, produtos. O acesso para consultas e alterações de teste foi feito pelo DBeaver.

### 2.2 Por que a origem fica na máquina local

Em muitas empresas, os sistemas ainda rodam em servidores próprios, fora da nuvem. Simular isso obriga o pipeline a resolver um problema real: como a nuvem lê dados que estão atrás da rede de um escritório. A resposta, no Azure, é o Integration Runtime auto-hospedado, explicado na parte 3.

### 2.3 A coluna ModifiedDate

Quase todas as tabelas do AdventureWorks têm uma coluna chamada ModifiedDate, com a data e hora da última alteração da linha. Ela é a base de toda a carga incremental do projeto: em vez de copiar tudo a cada vez, o pipeline pergunta "quais linhas mudaram desde a última vez?". Um cuidado importante: essa técnica depende de a coluna ser atualizada sempre que a linha muda. Se alguém alterar um dado sem atualizar o ModifiedDate, a mudança não é percebida.

---

## 3. Parte 2: a cópia incremental para o Azure

### 3.1 O Azure SQL, que recebe os dados

Foi criado um servidor lógico Azure SQL, sql-adventureworks-lab-alex, com o banco sqldb-adventureworks-staging, na oferta gratuita sem servidor. Essa oferta tem duas características que influenciaram o projeto. A primeira é a pausa automática: depois de um tempo sem uso, o banco desliga, e a primeira conexão seguinte pode levar até um minuto enquanto ele religa. A segunda é a cota mensal gratuita: se ela acabar, o banco pode ficar pausado até o mês seguinte.

Dentro do banco existem dois schemas importantes. O schema stg recebe as 71 tabelas copiadas da origem, com o nome no formato schema de origem, sublinhado, nome da tabela; por exemplo, Sales.SalesOrderDetail vira stg.Sales_SalesOrderDetail. O schema ctl guarda a tabela de controle.

### 3.2 A tabela de controle ctl.ConfigTabela

Essa tabela é o coração da arquitetura. Em vez de criar um pipeline para cada tabela, existe uma única lista que diz, para cada tabela, como ela deve ser carregada. Essa abordagem se chama carga orientada por metadados. As colunas principais são estas. Id, o identificador da linha. A tabela de origem e a TabelaDestino, que é o nome no staging, como Sales_SalesOrderDetail. TipoCarga, que pode ser FULL, para cópia completa, ou INCREMENTAL. ColunaMarco, a coluna usada como marca d'água, normalmente ModifiedDate. ChavesJson, a chave primária da tabela, em formato de lista JSON, como ["SalesOrderID", "SalesOrderDetailID"]. UltimoMarco, a maior data já copiada pelo Data Factory. E Ativo, que diz se a tabela participa do processo.

O ponto mais importante é que essa mesma tabela é lida pelo Data Factory e pelo Databricks. Ela funciona como um contrato entre as duas ferramentas: quando uma tabela é incluída ou desativada, as duas pontas acompanham sozinhas.

Existe também uma tabela antiga, stg.ControleCarga, de uma primeira versão do incremental, que controlava só duas tabelas de vendas. Ela ficou parada e pode ser removida.

### 3.3 O Integration Runtime auto-hospedado

O Data Factory roda na nuvem e não enxerga a máquina local. Para resolver isso, foi instalado na máquina um programa da Microsoft chamado Integration Runtime auto-hospedado, registrado no Data Factory com o nome ir-adventureworks-local. Ele funciona como uma ponte: o Data Factory manda as instruções, e o runtime, de dentro da máquina, lê o SQL Server e envia os dados para o Azure. Para qualquer carga funcionar, o serviço desse runtime precisa estar em execução, o que se confere no ADF Studio, em Gerenciar, Runtimes de integração.

### 3.4 Os pipelines do Data Factory

O Data Factory tem cinco pipelines. O pl_adventureworks_carga_inicial fez a primeira cópia. O pl_adventureworks_incremental é a versão antiga, que tratava só o cabeçalho e os itens de pedido. O pl_adventureworks_carga_tabela é o pipeline genérico, que carrega uma única tabela a partir de parâmetros. E o pl_adventureworks_orquestrador é o que se usa no dia a dia.

O orquestrador funciona assim. Uma atividade de pesquisa, chamada lookup_tabelas_ativas, lê a ctl.ConfigTabela e traz a lista das tabelas com Ativo verdadeiro. Em seguida, um laço ForEach, chamado foreach_tabelas, percorre essa lista e, para cada tabela, executa o pipeline de carga, na atividade executar_carga_tabela, passando os dados daquela linha como parâmetros. Com 71 tabelas, uma execução completa tem 73 atividades: o lookup, o ForEach e as 71 cargas.

### 3.5 A técnica de carga incremental: marca d'água com upsert

A técnica usada se chama carga incremental por marca d'água, em inglês watermark. Funciona como a marca que a água deixa na parede: o pipeline guarda até onde já leu e, na próxima vez, busca só o que passou dessa marca. Na prática, para cada tabela, o filtro é: ModifiedDate maior que o último marco e menor ou igual ao novo marco, que é capturado no início da cópia. Depois que a cópia termina com sucesso, o novo marco é gravado na tabela de controle.

No destino, a gravação é feita com upsert, palavra que junta update e insert: se a chave da linha já existe no staging, ela é atualizada; se não existe, é inserida. É isso que impede duplicações quando a mesma linha é alterada várias vezes.

Duas limitações dessa técnica foram documentadas. Ela não percebe exclusões, porque uma linha apagada simplesmente some e não aparece em nenhuma consulta. E ela depende de o ModifiedDate ser atualizado. A alternativa mais completa é o CDC, captura de dados de alterações, em que o próprio SQL Server lê o log de transações e registra inserções, atualizações e exclusões. O CDC exige o SQL Server Agent, então não funciona na edição Express, e fica como evolução futura.

### 3.6 Tempos observados

A primeira carga completa levou cerca de 1 hora e 40 minutos. Os motivos foram o envio de todos os dados pela internet de casa, o custo fixo de preparação de cada atividade, de 20 a 90 segundos, a execução das tabelas em sequência e o banco gratuito do Azure, com poucos recursos. Uma execução incremental do orquestrador, no teste final, levou cerca de 38 minutos, quase todos gastos nesse custo fixo por tabela. A melhoria planejada é configurar o ForEach para rodar 4 tabelas em paralelo.

### 3.7 Rede e segurança no Azure SQL

O Azure SQL tem um firewall próprio, que funciona como a portaria de um prédio: só deixa passar endereços IP autorizados. Foram criadas três regras: o IP da máquina local; o IP de saída do Databricks, que se mostrou não confiável porque o ambiente serverless não tem IP fixo; e, por fim, uma regra de laboratório, chamada Databricks-Free-Lab-ALL, que libera de 0.0.0.0 até 255.255.255.255, ou seja, qualquer endereço. Essa regra só é aceitável em laboratório, com senha forte e usuário somente leitura.

Para controlar a exposição, o interruptor usado é a opção "Acesso à rede pública", na tela de Rede do servidor. Em "Redes selecionadas", a porta está aberta e as regras valem. Em "Desabilitar", ninguém entra pela internet, mas as regras ficam guardadas. Quando o acesso está desabilitado, tanto o Data Factory quanto o Databricks param de funcionar.

A opção "Permitir que serviços e recursos do Azure acessem este servidor" foi mantida marcada porque o Data Factory depende dela. Ela não ajuda o Databricks Free Edition, que roda na AWS.

### 3.8 Autenticação no Azure SQL

O servidor foi criado aceitando apenas autenticação do Microsoft Entra, ou seja, login com conta Microsoft. Isso causou problemas na conexão do Databricks, descritos na parte 4. A solução foi desmarcar, em Configurações, Microsoft Entra ID, a opção "Dar suporte somente à autenticação do Microsoft Entra para esse servidor", liberando também o login com usuário e senha.

Em seguida, foi criado um usuário próprio para o Databricks, chamado databricks_leitura, diretamente dentro do banco, com permissão apenas de leitura, pelo papel db_datareader. Assim, mesmo que a senha vaze, ninguém consegue alterar nem apagar dados. O script está no repositório, em sql/azure/02_usuario_databricks_leitura.sql, com a senha substituída por um marcador.

---

## 4. Parte 3: o Databricks

### 4.1 O Databricks Free Edition e suas limitações

O Databricks Free Edition é a versão gratuita da plataforma. Ele roda na AWS, e não no Azure, usa apenas compute serverless, permite um único workspace por conta e restringe parte das conexões de saída para a internet. Essas características explicam vários desvios do caminho: o IP de saída não é fixo, a atividade nativa de Databricks do Data Factory não funciona com ele, e algumas operações têm limites de uso.

### 4.2 A organização do Unity Catalog

O Unity Catalog organiza tudo em três níveis: catálogo, schema e tabela. No nome workspace.adventureworks_gold.fato_vendas, workspace é o catálogo, adventureworks_gold é o schema e fato_vendas é a tabela. Essa hierarquia foi motivo de dois enganos no caminho: tratar um schema como catálogo e filtrar o information_schema pela coluna errada.

Os catálogos existentes no workspace são: workspace, com as camadas do projeto; azure_adventureworks, estrangeiro, que aponta para o Azure SQL; samples, com bases de exemplo da Databricks; system, com as tabelas de sistema, como uso, auditoria, histórico de consultas e execuções de jobs; e dois catálogos vazios de cursos antigos.

### 4.3 A conexão federada com o Azure SQL

A Lakehouse Federation permite consultar um banco externo como se ele fosse um catálogo do Databricks, sem copiar dados. São dois objetos. A conexão guarda o endereço e as credenciais do servidor. O catálogo estrangeiro aponta para um banco específico, usando a conexão.

A primeira tentativa usou autenticação OAuth com a conta Microsoft e enfrentou três problemas. O primeiro foi o erro AADSTS501481, que diz que o code verifier não confere com o code challenge. Isso acontece no fluxo de segurança chamado PKCE, quando o login é feito mais de uma vez, a página é recarregada ou o assistente fica aberto em várias abas; a solução foi refazer tudo numa janela anônima, fazendo login uma única vez. O segundo foi a instância nomeada: o campo foi preenchido com MSSQLSERVER, mas o Azure SQL não usa instância nomeada, e o campo deve ficar vazio. O terceiro, e decisivo, foi que o token da conta Microsoft vencia em cerca de 1 hora e o provedor não fornecia o token de renovação; um job agendado falharia sempre.

A solução definitiva foi criar uma segunda conexão, con_azure_adventureworks_sql, com usuário e senha, usando o usuário somente leitura databricks_leitura. Depois, o catálogo azure_adventureworks foi apagado e recriado com o mesmo nome, apontando para a nova conexão. Como o nome não mudou, nenhum código precisou ser alterado. Por fim, a conexão antiga e um catálogo vazio criado por engano foram removidos.

Os valores usados na conexão foram: host sql-adventureworks-lab-alex.database.windows.net, porta 1433, usuário databricks_leitura, instância nomeada vazia e a opção de confiar no certificado desmarcada.

### 4.4 Tabelas Delta

Todas as tabelas criadas no Databricks estão no formato Delta, o padrão da plataforma. Por baixo, os dados ficam em arquivos Parquet, que é um formato colunar e compacto. O Delta acrescenta um registro de transações, a pasta _delta_log, que funciona como um livro-caixa: cada operação vira uma versão e só passa a valer quando termina por inteiro. Disso vêm os recursos usados no projeto: o merge, que permite atualizar e inserir linhas com segurança; o histórico de versões, consultado com DESCRIBE HISTORY; a viagem no tempo, consultando a tabela como estava numa versão anterior; e o mapeamento de colunas por nome, que permite nomes de colunas com espaço. Também por isso é seguro cancelar um job no meio: nada fica pela metade.

### 4.5 Spark, PySpark e SQL

Todo o processamento no Databricks é feito pelo Spark, de forma distribuída. Python e SQL são apenas linguagens para dar ordens a esse motor. Quando o código usa spark.table, F.col ou DeltaTable, ele está montando um plano que o Spark executa. A parte em Python puro do projeto é só a orquestração leve: o laço que percorre as tabelas e a leitura das chaves em JSON. A boa prática seguida foi nunca trazer os dados para o Python linha por linha, como faria um toPandas em tabela grande.

### 4.6 Carga inicial da bronze

A primeira carga copiou as tabelas do staging para o schema adventureworks_bronze do catálogo workspace. O processo listava as tabelas do schema stg e, para cada uma, executava um CREATE OR REPLACE TABLE a partir de um select de todas as colunas.

Três situações apareceram. A primeira foi uma falha ao listar todos os schemas do SQL Server, que incluem schemas internos; a solução foi fixar a lista no schema stg. A segunda foram 3 tabelas a mais: a stg.ControleCarga e duas cópias antigas, sem prefixo, das tabelas de vendas, do primeiro teste do Data Factory; elas foram apagadas da bronze. A terceira foi a tabela dbo_AWBuildVersion, que falhou porque tem uma coluna chamada "Database Version", com espaço, que o Delta não aceita no modo padrão; a solução foi criar a tabela com a propriedade delta.columnMapping.mode igual a name, que separa o nome lógico do nome físico da coluna. Essa propriedade passou a ser usada em toda carga completa.

Ao final, a bronze ficou com as 71 tabelas ativas do cadastro, e a contagem da Person_Address conferiu com a origem: 19614 linhas nos dois lados.

### 4.7 Bronze incremental

O notebook nb_bronze_incremental faz o seguinte, para cada tabela ativa da ctl.ConfigTabela.

Se a tabela for do tipo FULL, não tiver coluna de marco ou chave, ou ainda não existir na bronze, faz uma carga completa, recriando a tabela.

Se for incremental, calcula o marco próprio da bronze, que é a maior data de modificação já presente na tabela Delta. Esse marco é independente do UltimoMarco do Azure, porque o marco do Azure diz até onde o Data Factory leu da máquina local, e não até onde o Databricks leu do staging.

Depois, busca no staging as linhas com data maior ou igual a esse marco. O "maior ou igual" é proposital: se duas linhas tiverem exatamente a mesma data, nenhuma se perde. O efeito colateral é que as linhas com a data mais recente são reprocessadas a cada execução, o que é inofensivo por causa do passo seguinte.

Por fim, aplica um merge pela chave primária da coluna ChavesJson: atualiza as linhas existentes e insere as novas.

Cada tabela roda dentro de um tratamento de erro, e o resultado é gravado na tabela log_cargas, com data, tabela, tipo de execução, linhas processadas, marco anterior e mensagem de erro. Se alguma tabela falhar, o notebook termina com erro no final, para impedir que a prata e a ouro rodem sobre dados incompletos.

O teste do incremental foi feito alterando, no staging, o nome da categoria 1 de Bikes para "Bikes - teste databricks", com a data atual. O notebook processou a tabela de categorias como incremental, com 1 linha, e o histórico Delta mostrou a versão original e a nova.

### 4.8 Prata: padronização genérica

A camada prata é feita por um único notebook genérico, nb_silver_padronizacao, que aplica as mesmas regras às 71 tabelas. A boa prática adotada foi esta: tratamentos comuns na prata, de forma genérica e orientada por metadados; regras de negócio na ouro, por assunto.

As regras da prata são cinco. Nomes de tabelas e colunas em snake_case, tudo minúsculo e separado por sublinhado; por exemplo, ProductCategoryID vira product_category_id, e Sales_SalesOrderHeader vira sales_sales_order_header. Remoção da coluna técnica rowguid. Textos sem espaços nas pontas e textos vazios transformados em nulo. Uma única linha por chave, mantendo a mais recente, com a função de janela row_number. E uma coluna de auditoria, data_processamento_silver.

A carga segue o mesmo modelo incremental da bronze, lendo a bronze e com log próprio. A primeira execução levou cerca de 5 minutos e criou as 71 tabelas. Um ponto de governança foi anotado: a tabela person_password contém hash e salt de senhas e não deve chegar às camadas de análise.

### 4.9 Ouro: modelo estrela de vendas

A camada ouro, no notebook nb_gold_vendas, monta um modelo estrela, com uma fato no centro e dimensões ao redor, com nomes em português, porque é a camada consumida pelo negócio.

A fato_vendas tem o grão de item de pedido, ou seja, uma linha por produto vendido em cada pedido. Ela junta os itens com o cabeçalho do pedido e calcula valor bruto, valor do desconto e valor líquido. O frete e o imposto, que só existem no cabeçalho, são distribuídos entre os itens de forma proporcional ao valor líquido de cada um, técnica chamada rateio, feita com uma soma por janela.

As dimensões são cinco. A dim_data, gerada do primeiro ao último ano das vendas, com ano, trimestre, mês e dia da semana em português. A dim_cliente, que diferencia pessoa física de loja. A dim_produto, com subcategoria e categoria, e textos padrão para nulos. A dim_territorio, com o país. E a dim_vendedor, com um membro especial de código menos 1, "Sem vendedor", para as vendas online, que não têm vendedor. Essa técnica se chama membro desconhecido e garante que nenhuma venda se perca nos relacionamentos.

A ouro também cria a view vw_vendas_detalhadas, que junta a fato com todas as dimensões, para uso em painéis e no Power BI.

O notebook termina com uma validação automática: a quantidade de linhas da fato deve ser igual à de itens da prata, a view não pode perder linhas, o total líquido deve ser igual à soma dos subtotais dos pedidos e o frete rateado deve ser igual ao frete total. Na primeira execução, os números foram 121317 linhas, total líquido de 109.846.381,40 e frete de 3.183.430,25, todos idênticos à prata.

### 4.10 Painel no Databricks

Foi criado um painel AI/BI, chamado Vendas AdventureWorks, sobre a view vw_vendas_detalhadas, com as medidas Receita, como soma do valor líquido, Pedidos, como contagem distinta de pedidos, e Ticket médio, como receita dividida por pedidos. Calcular o ticket como medida garante o valor correto em qualquer filtro. O primeiro contador mostrou 109,85 milhões. Uma lição do teste final: esse formato, em milhões com duas casas, não mostra variações pequenas.

### 4.11 O job

Os três notebooks foram reunidos num job com três tarefas em sequência: bronze_incremental, silver_padronizacao e gold_vendas. Cada tarefa depende da anterior e só roda se ela terminar com sucesso. Cada uma tem uma nova tentativa automática, com espera de 1 minuto, que resolve falhas passageiras, como o banco do Azure acordando da pausa.

Duas lições operacionais apareceram. A primeira: cada clique em "Run now" cria uma execução, e o job só roda uma por vez, então cliques repetidos geram uma fila; foi preciso cancelar execuções extras. A segunda: a opção "Run now with different settings" serve para rodar só algumas tarefas.

A diferença entre Job e Pipeline no Databricks também foi esclarecida. O Job é um orquestrador: executa tarefas em ordem, com dependências, novas tentativas e agendamento. O Pipeline, hoje chamado Lakeflow Declarative Pipelines, é uma forma declarativa de escrever transformações, em que se declara quais tabelas devem existir e a plataforma cuida da ordem e do incremental. Como os notebooks do projeto são do estilo passo a passo, quem os executa é um Job.

---

## 5. Parte 4: Git e CI/CD

### 5.1 Por que organizar o código antes do Power BI

Até esse ponto, os notebooks e o job tinham sido criados à mão, na tela do Databricks. Isso funciona, mas não deixa histórico, não permite revisão e torna fácil quebrar algo sem perceber. A decisão foi organizar o código e o CI/CD antes de conectar o Power BI, para que toda mudança futura nas camadas passe por validação e fique registrada.

### 5.2 O repositório

Foi criado o repositório adventureworks-lakehouse no GitHub. A documentação da primeira fase, do repositório adventureworks-adf-ingestion, foi trazida para dentro dele, no arquivo docs/ingestao_adf.md. A estrutura de pastas é esta.

A pasta adf é reservada para os recursos do Data Factory, quando a integração Git dele for ligada, com a pasta raiz /adf.

A pasta databricks contém o bundle: o arquivo databricks.yml, a pasta resources, com a definição do job, e a pasta src, com os três notebooks e a pasta utils, onde fica a função de padronização de nomes.

A pasta sql/azure contém os scripts do Azure SQL.

A pasta tests contém os testes automáticos.

A pasta docs contém a documentação, este guia, o relatório do teste e a pasta evidencias, com as capturas de tela.

A pasta .github/workflows contém os dois workflows, ci.yml e cd.yml.

E, na raiz, ficam o README.md, o .gitignore, que impede o envio de arquivos de credenciais e temporários, e o ruff.toml, com as regras de verificação do código.

### 5.3 Branches e fluxo de trabalho

São duas branches. A dev é onde as mudanças nascem e são testadas. A main guarda a versão estável e só recebe mudanças por Pull Request.

O fluxo é: alteração na dev; o CI valida e publica no ambiente de desenvolvimento; abre-se um Pull Request da dev para a main; o CI valida de novo; faz-se o merge; e a CD publica no ambiente estável. Publicar não executa a carga de dados: rodar o job continua sendo uma ação separada e controlada.

### 5.4 Declarative Automation Bundles

O bundle é a forma da Databricks de descrever um projeto inteiro em arquivos de texto, para versionar no Git e publicar de forma automática. É o conceito de infraestrutura como código: em vez de clicar para montar o job, ele é descrito num arquivo, e a linha de comando do Databricks cria ou atualiza o job a partir dele.

O arquivo databricks.yml define o nome do bundle, adventureworks_lakehouse; o endereço do workspace; as variáveis, que são o catálogo, o catálogo de origem e os três schemas; e os alvos, que são os ambientes de destino. O alvo dev usa o modo de desenvolvimento e os schemas dev_adventureworks_bronze, dev_adventureworks_silver e dev_adventureworks_gold; nesse modo, o job ganha um prefixo com o nome do usuário, uma etiqueta com o usuário e tem os agendamentos pausados. O alvo prod usa os schemas estáveis, adventureworks_bronze, adventureworks_silver e adventureworks_gold.

Como o Free Edition permite um único workspace, a separação entre ambientes é feita por schema, dentro do mesmo workspace.

O arquivo resources/job_lab_adventureworks.yml descreve o job. A chave interna é job_lab_adventureworks, que é como o bundle reconhece o job entre um deploy e outro. O nome exibido passou a ser job_adventureworks_lakehouse, com as tags projeto igual a adventureworks-lakehouse e gerenciado_por igual a bundle. O job tem cinco parâmetros, catalogo, catalogo_origem, schema_bronze, schema_silver e schema_gold, cujos valores vêm das variáveis do alvo. Esses parâmetros são entregues automaticamente aos widgets dos notebooks. As três tarefas apontam para os notebooks da pasta src, com depends_on para a ordem, max_retries igual a 1, min_retry_interval_millis igual a 60000, que é 1 minuto, e retry_on_timeout verdadeiro. Sem bloco de compute, as tarefas rodam em serverless.

Os três comandos principais do bundle são: validate, que confere a configuração sem alterar nada; deploy, que publica os notebooks e cria ou atualiza o job no alvo escolhido; e run, que executa o job. O projeto usa só os dois primeiros na automação.

### 5.5 Notebooks parametrizados

Para que o mesmo código servisse aos dois ambientes, os nomes dos schemas deixaram de ser fixos nos notebooks. Cada notebook declara widgets, com dbutils.widgets.text, e lê os valores com dbutils.widgets.get. Quando o job roda, ele envia os valores do seu ambiente. Os valores padrão dos widgets, usados apenas quando alguém executa o notebook à mão, foram ajustados para os schemas dev, para que uma execução manual de teste nunca grave nas camadas estáveis.

Os notebooks são arquivos .py que começam com a linha "Databricks notebook source" e têm separadores de célula; por isso o Databricks os abre como notebooks.

### 5.6 Testes e verificação de código

O Ruff é a ferramenta de verificação de código Python. Ele faz duas coisas. A verificação, ou lint, aponta problemas como variáveis inexistentes, importações não usadas e linhas longas demais, sem mudar nada. A formatação reescreve a aparência do código para um padrão único. A configuração do projeto, no ruff.toml, limita as linhas a 120 caracteres e declara spark, dbutils e display como nomes conhecidos, porque eles só existem dentro do Databricks.

Os testes usam o pytest e cobrem a função para_snake_case, garantindo, por exemplo, que ProductCategoryID vire product_category_id e que "Database Version" vire database_version.

Uma lição apareceu no primeiro Pull Request: a extensão do Ruff no VS Code formatava os arquivos ao salvar e juntou trechos de várias linhas numa só, misturando formatação com a mudança de lógica e dificultando a revisão. A recomendação é fazer a formatação num commit separado.

### 5.7 Os workflows do GitHub Actions

O workflow de CI, no arquivo ci.yml, roda a cada envio para a dev e a cada Pull Request para a main. Ele tem dois jobs. O primeiro, "Qualidade e testes", instala o Ruff e o pytest, verifica o código e roda os testes. O segundo, que só começa se o primeiro passar, instala a linha de comando do Databricks, valida o bundle no alvo dev e, nos envios para a dev, publica no alvo dev.

O workflow de CD, no arquivo cd.yml, roda a cada envio para a main, ou seja, a cada merge, e também pode ser disparado manualmente pelo botão "Run workflow". Ele valida e publica o bundle no alvo prod. Um controle de concorrência impede dois deploys ao mesmo tempo.

Os dois workflows leem dois segredos do repositório: DATABRICKS_HOST, com o endereço do workspace, e DATABRICKS_TOKEN, com um token de acesso pessoal do Databricks. O token foi gerado em Settings, Developer, Access tokens, com validade de 30 dias e escopo de APIs gerais. Os segredos ficam em Settings, Secrets and variables, Actions, e nunca aparecem nos registros. A recomendação oficial da Databricks é usar uma identidade de serviço com federação de identidade, sem segredo armazenado; o token pessoal foi a alternativa adotada para o laboratório.

### 5.8 Como o projeto foi enviado ao GitHub pelo VS Code

O Git foi usado pelo VS Code, com a integração que já vem nele. A sequência foi esta. Abrir a pasta do projeto e confiar nela, saindo do modo restrito. Inicializar o repositório no painel de controle de código. Fazer o primeiro commit, preparando todos os arquivos. Adicionar o remoto, com o comando "Git: Add Remote", o endereço do repositório e o nome origin. Publicar a branch main, autorizando o VS Code no GitHub com o código de verificação em duas etapas. Criar a branch dev a partir da main, pelo nome da branch na barra inferior, e publicá-la.

Os três estágios do Git ficaram claros nesse processo: a pasta de trabalho, onde estão os arquivos editados; a área de preparação, onde ficam os que entram no próximo commit; e o histórico, onde o commit fica gravado. As letras ao lado dos arquivos indicam a situação: U para arquivo novo, M para modificado, A para adicionado e D para apagado.

As mensagens de commit seguem o padrão Conventional Commits: feat para funcionalidade nova, fix para correção, docs para documentação, chore para manutenção, style para formatação, refactor para reorganização sem mudança de comportamento, test para testes e ci para os workflows.

### 5.9 Primeiros deploys e organização dos jobs

Depois de cadastrar os segredos, o CI e a CD rodaram de novo e publicaram o bundle. O job dev rodou com sucesso em 12 minutos e 38 segundos, gravando nos schemas dev. A comparação entre os ambientes confirmou que o código parametrizado gera o mesmo resultado: 121317 linhas e 109.846.381,40 nos dois lados.

Na limpeza, o job errado foi apagado por engano: o novo, criado pelo bundle, em vez do antigo, feito à mão. Ele foi recriado em segundos, disparando a CD manualmente, o que mostrou na prática a vantagem de ter o job em código. A forma segura de diferenciar os dois foi a faixa "Managed by Declarative Automation Bundles", que só aparece no job controlado pelo bundle. Depois, o job antigo foi apagado.

Os notebooks publicados pelo bundle ficam numa pasta .bundle, dentro da pasta do usuário no Workspace. Eles devem ser tratados como somente leitura, porque qualquer edição feita ali é sobrescrita no próximo deploy. A fonte oficial do código é o repositório.

### 5.10 O primeiro Pull Request

O primeiro ciclo completo de mudança teve dois commits na dev. O primeiro, do tipo chore, renomeou o job para job_adventureworks_lakehouse e acrescentou as tags. O segundo, do tipo fix, apontou os valores padrão dos widgets para os schemas dev. Cada envio disparou o CI, que publicou no ambiente de desenvolvimento.

Depois foi aberto o Pull Request número 1, da dev para a main, com título e descrição explicando as mudanças. A revisão das diferenças mostrou as alterações pedidas e também a reformatação automática do Ruff. O CI rodou sobre o Pull Request, com 4 verificações aprovadas, e o merge foi feito com um merge commit, preservando o histórico. A branch dev não foi apagada, porque é permanente. A CD publicou no ambiente estável, e os dois jobs passaram a mostrar o nome novo e as tags.

### 5.11 Formas de editar o código

O VS Code é o caminho usado no projeto. As outras opções discutidas foram a linha de comando do Git, que executa por baixo o mesmo que os botões do VS Code, e as Git folders do Databricks, que ligam uma pasta do workspace ao repositório e permitem editar e fazer commit pelo navegador. Em qualquer caso, o caminho até o ambiente estável é sempre o mesmo: Pull Request e automação.

---

## 6. Parte 5: o teste de ponta a ponta

### 6.1 Objetivo e alteração

O teste comprovou que uma alteração feita no SQL Server local percorre todo o pipeline e chega, com o valor correto, à camada ouro. A alteração escolhida foi somar 10 unidades ao item 13 do pedido 43660, que passou de 1 para 11 unidades, com o ModifiedDate atualizado. O cabeçalho do pedido também teve o subtotal recalculado e a data atualizada, para que item e pedido entrassem juntos na carga incremental e a validação da ouro continuasse fechando.

O item tem preço unitário de 419,4589 e nenhum desconto. Então o aumento esperado era de 10 vezes 419,4589, ou seja, 4.194,589. O subtotal do pedido passou de 1.294,2529 para 5.488,8419.

### 6.2 Roteiro, na ordem correta

Primeiro, registrar a linha de base na ouro: receita total de 109.846.381,40.

Segundo, alterar a origem no DBeaver, com confirmação automática ligada.

Terceiro, conferir que o Integration Runtime está em execução e que o acesso público do Azure SQL está aberto.

Quarto, disparar o pl_adventureworks_orquestrador no Data Factory e esperar terminar. As 73 atividades terminaram com sucesso, em cerca de 38 minutos.

Quinto, conferir o staging no Azure. Esse passo funciona como um portão: só se segue se a alteração aparecer. O item 13 apareceu com 11 unidades e o pedido com o subtotal novo e a data de 20 de novembro de 2026, às 14:49.

Sexto, executar o job job_adventureworks_lakehouse. As três tarefas terminaram com sucesso, em 18 minutos e 34 segundos, incluindo a validação automática da ouro.

Sétimo, conferir a ouro. A receita total passou a 109.850.575,99, exatamente o valor calculado: 109.846.381,40, menos 1.294,25, mais 5.488,84.

### 6.3 O que deu errado e o que se aprendeu

Na primeira tentativa, o job do Databricks foi disparado antes de o Data Factory rodar. O staging ainda tinha os dados antigos, então o job processaria tudo sem mostrar diferença. A execução foi cancelada e aparece no histórico como cancelada pelo usuário. A lição é que a ordem é obrigatória: o Databricks lê o Azure, e quem leva os dados da máquina local ao Azure é o Data Factory.

Outra lição: o contador do painel, em milhões com duas casas, mostrou 109,85 milhões antes e depois, porque a diferença de 4.194 é pequena demais para o arredondamento. A prova do teste foi feita por consulta. Para evidência visual, o contador deve mostrar o número completo.

Também ficou esclarecido um número que parecia estranho no DBeaver: o subtotal aparecia como 5.488,8419 com separadores de milhar, e foi lido como 54 mil. A conferência no Azure resolveu a dúvida.

O relatório detalhado está em docs/teste_ponta_a_ponta.md, e as 23 capturas, em docs/evidencias.

---

## 7. Problemas encontrados e soluções

Esta seção reúne, em formato de consulta rápida, os problemas que apareceram e como foram resolvidos.

Erro AADSTS501481 ao criar a conexão do Databricks: fluxo de login OAuth interrompido ou repetido. Solução: refazer em janela anônima, com um único login, ou, melhor, usar autenticação por usuário e senha.

Token da conexão vencendo em 1 hora, sem renovação: limitação de contas pessoais usadas como convidadas no Entra. Solução: conexão com usuário e senha do SQL.

Botão "Redefinir senha" desativado no Azure SQL: servidor aceitando apenas autenticação do Entra. Solução: desmarcar essa exigência em Configurações, Microsoft Entra ID.

Databricks sem acesso ao Azure SQL pelo firewall: o serverless não tem IP fixo. Solução de laboratório: regra de 0.0.0.0 a 255.255.255.255 e controle pelo interruptor de acesso à rede pública.

Falha de conexão JDBC na primeira tentativa do job: banco gratuito acordando da pausa automática. Solução: nova tentativa automática na tarefa.

Falha ao listar schemas do SQL Server pelo catálogo federado: schemas internos do SQL Server. Solução: lista fixa com o schema stg.

Tabela dbo_AWBuildVersion falhando: coluna com espaço no nome. Solução: delta.columnMapping.mode igual a name.

Esquema tratado como catálogo, e filtro do information_schema vazio: confusão entre os níveis do Unity Catalog. Solução: usar table_catalog para catálogo e table_schema para schema.

Notebook Python colado no Editor de consultas do Azure: linguagens diferentes. Solução: SQL do Azure no Azure; Python no notebook do Databricks.

Várias execuções na fila do job: cliques repetidos em "Run now". Solução: clicar uma vez e esperar a confirmação; cancelar as execuções extras.

Job errado apagado: dois jobs com o mesmo nome. Solução: recriar pelo deploy manual da CD e identificar o job do bundle pela faixa "Managed by Declarative Automation Bundles".

Widgets com valores padrão apontando para os schemas estáveis: risco de uma execução manual gravar no ambiente estável. Solução: padrões apontando para os schemas dev.

Formatação automática misturada à mudança de lógica: extensão do Ruff formatando ao salvar. Solução: commit separado de formatação ou desligar "Format On Save".

Job executado antes do Data Factory, no teste: ordem invertida. Solução: Data Factory, conferência do staging e só então o Databricks.

Página do Databricks fora do ar no navegador, com ERR_CONNECTION_TIMED_OUT: oscilação de conexão. Solução: recarregar; o job continua rodando no servidor.

---

## 8. Glossário

Arquitetura medalhão: organização dos dados em camadas de qualidade crescente, bronze, prata e ouro.

Bronze: cópia fiel da origem, sem tratamento, em formato Delta.

Prata: dados limpos, padronizados e com uma linha por chave.

Ouro: dados prontos para o negócio, com regras de negócio e modelo dimensional.

Modelo estrela: uma tabela fato, com os números, cercada de tabelas de dimensão, com as descrições.

Grão: o nível de detalhe de uma linha da fato; aqui, o item de pedido.

Membro desconhecido: linha especial de dimensão, como "Sem vendedor", que evita perder registros sem correspondência.

Rateio: distribuição proporcional de um valor do cabeçalho entre os itens.

Marca d'água, ou watermark: maior valor já processado de uma coluna de data, usado para buscar só as novidades.

Upsert: atualizar se existe, inserir se não existe; no Databricks, feito pelo merge.

CDC: captura de dados de alterações, leitura das mudanças pelo log de transações do banco.

Carga orientada por metadados: um processo genérico guiado por uma tabela de configuração.

Integration Runtime auto-hospedado: programa instalado na máquina local que permite ao Data Factory ler dados de dentro da rede.

Lakehouse Federation: consulta a bancos externos como catálogos do Databricks, sem copiar dados.

Catálogo estrangeiro: catálogo do Databricks que aponta para um banco externo.

Delta: formato de tabela com registro de transações, versões, merge e viagem no tempo.

Unity Catalog: camada de governança do Databricks, com catálogos, schemas, tabelas e permissões.

Serverless: compute gerenciado pela plataforma, sem cluster próprio.

Job: orquestrador de tarefas do Databricks.

Bundle: projeto Databricks descrito em arquivos, para versionamento e deploy automático.

Alvo, ou target: ambiente de destino de um bundle, como dev ou prod.

CI, integração contínua: validação automática a cada mudança.

CD, entrega contínua: publicação automática depois da aprovação.

Pull Request: pedido para juntar uma branch em outra, com revisão e verificações.

Merge: junção de uma branch em outra.

Workflow: arquivo de automação do GitHub Actions.

Segredo: valor sensível guardado de forma criptografada no GitHub.

Lint: verificação automática de problemas no código.

Schema drift: mudança de esquema na origem, como uma coluna renomeada.

Observabilidade: acompanhamento da saúde do pipeline, com métricas, registros e alertas.

---

## 9. Próximos passos

Proteger a branch main, para que ela aceite apenas Pull Requests com o CI aprovado.

Agendar o job do Databricks para depois do Data Factory ou fazer o próprio Data Factory acioná-lo por uma chamada de API ao terminar.

Paralelizar o ForEach do Data Factory, com 4 tabelas por vez, para reduzir os cerca de 38 minutos.

Otimizar a bronze: processar várias tabelas em paralelo, pular tabelas sem mudança comparando versões Delta e remover a contagem extra de linhas.

Detectar mudanças de esquema na origem, como colunas renomeadas, registrar num log de esquema, bloquear a tabela afetada com mensagem clara e alertar por e-mail, com um roteiro documentado de correção.

Montar um painel de observabilidade no Grafana, com as execuções do Data Factory, os tempos do job, as linhas processadas e as falhas por tabela, usando o Grafana Cloud ou um Grafana instalado na VPS da Hostinger.

Conectar o Power BI ao modelo estrela da camada ouro.

Trocar o token do Databricks antes de vencer, a cada 30 dias, e avaliar a identidade de serviço com federação.

Decidir sobre a formatação automática do Ruff e, se mantida, aplicar num commit separado e incluir a verificação de formatação no CI.

Remover a tabela antiga stg.ControleCarga e o pipeline antigo de incremental, se não forem mais usados.
