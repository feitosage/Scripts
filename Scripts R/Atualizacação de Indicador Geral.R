# Atualização Desempenho UNIDRED

tempo_ini = round(Sys.time())

# Carregando pacotes

pacotes <-
  c('dplyr','lubridate','readxl','RPostgreSQL','RODBC','DBI','tidyr','data.table','rlang', 'bizdays', 'withr', 'readbulk', 'bit64','RMySQL')
lapply(pacotes,library, character.only = TRUE)
rm (pacotes)

tryCatch({
  
  # Criando conexão com o banco de dados MYSQL (CSLOG)
  
  CON_MYSQL = dbConnect(MySQL(), host = '192.168.150.22', user = 'schulze.dw', password = 'cUr3w13cG', dbname = 'cslog_schulze_prod')
  
  # Datas
  
  dia_atual = Sys.Date()
  
  Dia      <- substr(dia_atual,9,10)
  Mes      <- substr(dia_atual,6,7)
  Ano      <- substr(dia_atual,1,4)
  AnoAbrv    <- substr(dia_atual,3,4)
  MesNome   <- months(dia_atual)
  
  # Consulta da base ativa de contratos do dia
  
  qry_contratos = paste0("SELECT DISTINCT CURDATE() AS DATA_BASE,
       CO.ID_CONTR,
       CASE
		   WHEN CA.NOME = '0001-Unicred' THEN 'Amigável'
           ELSE 'Prejuízo' END AS CARTEIRA,    
         (SELECT UC.COOPERATIVA FROM UNICRED_COOPERATIVA UC WHERE UC.COD_COOPERATIVA = PU.COD_COOPERATIVA LIMIT 1) AS COOPERATIVA
FROM CONTRATO AS CO
INNER JOIN CARTEIRA CA 
  ON CO.ID_CARTEIRA = CA.ID_CARTEIRA
INNER JOIN PARC_GERAL_UNICRED PU
  ON PU.ID_CONTR = CO.ID_CONTR
WHERE CA.ID_EMPRESA = 197
AND CO.STATUS = 0")
  
  # Guardando o resultado das consultas em dataframes
  
  df_contratos_ativos = dbGetQuery(CON_MYSQL, qry_contratos)
  
  # Criando coluna de contagem de contratos (verifica se há contrato em duplicado em cooperativas distintas)
  
  df_contratos_ativos = df_contratos_ativos %>%
    group_by(ID_CONTR) %>%
    mutate(CONTAGEM = row_number())
  
  # Removendo contratos duplicados
  
  df_contratos_ativos = df_contratos_ativos%>%dplyr::filter(COOPERATIVA != "NA")
  df_contratos_ativos = df_contratos_ativos%>%dplyr::filter(CONTAGEM == 1)
  
  # Reordenando as colunas do dataset de contratos
  
  df_contratos_ativos = as.data.frame(df_contratos_ativos[c(1:4)])
  
  # Consulta de Acionamentos (Humano)
  
  qry_acionamentos_cslog = paste0("


SELECT H.ID_CONTR, 
	     H.CODIGO, 
       H.DATA, 
       'HUMANO' AS STATUS_ACIONAMENTO, 
       CA.NOME,
       CASE WHEN CA.NOME = '0001-Unicred' THEN 'AMIGÁVEL'
      ELSE 'PREJUÍZO' END AS 'CARTEIRA' 
FROM HISTORICO H 
INNER JOIN CONTRATO CO 
	ON H.ID_CONTR = CO.ID_CONTR 
INNER JOIN CARTEIRA CA 
	ON CO.ID_CARTEIRA = CA.ID_CARTEIRA 
WHERE CAST(H.DATA AS DATE) >= '",dia_atual,"'
AND ((CA.ID_EMPRESA = 197))
                                   
")
  
  
  # Consulta de saídas de contratos
  
  qry_devolucoes = paste0("

SELECT DISTINCT CO.ID_CONTR,
       '",dia_atual,"' AS DATA_BASE,
			 (SELECT UC.COOPERATIVA FROM UNICRED_COOPERATIVA UC WHERE UC.COD_COOPERATIVA = PU.COD_COOPERATIVA LIMIT 1) COOP,
			 CASE WHEN CO.ID_CARTEIRA = 1 THEN 'Amigável'
       ELSE 'Prejuízo' END AS 'Carteira',   
       PG.NUM_CONTRATO,
       CO.DATA_CAD AS DATA_INCLUSAO,
       PG.DATA_SAIDA_EFET,
       PG.NUM_PARC
FROM CONTRATO CO
INNER JOIN CARTEIRA CA
  ON CA.ID_CARTEIRA = CO.ID_CARTEIRA
INNER JOIN PARC_GERAL_UNICRED PU
	ON PU.ID_CONTR = CO.ID_CONTR
INNER JOIN PARC_GERAL PG
	ON PG.ID_CONTR = CO.ID_CONTR
WHERE CA.ID_EMPRESA = 197 AND CAST(PG.DATA_SAIDA_EFET AS DATE) = '",dia_atual,"' AND CO.STATUS IN (8,4,0);
                                   
")
  
  # consulta entradas de contratos
  
  qry_entradas = paste0(
    
    "SELECT CT.NOME AS CARTEIRA
  ,        C.ID_CONTR
  ,        C.NUM_CONTRATO
  ,        C.CPF
  ,        C.NOME
  ,        COD.CONTRATO AS STATUS
  ,        PG.NUM_PARC
  ,        PG.NUM_CONTRATO
  ,        COD2.PARC_GERAL_STATUS
  ,        PG.DATA_IMPORTACAO
  ,        PG.DATA_CARGA
  ,        PG.DATA_SAIDA_EFET
  ,        IF(ISNULL(CONCAT(PG.NUM_PARC,'/',PGU.QTDE_PARC_TIT)),'1/1',CONCAT(PG.NUM_PARC,'/',PGU.QTDE_PARC_TIT)) AS 'PARC./PLANO'
  ,        PG.VALOR
  ,        PG.VENC
  ,        SUBSTRING(U.`PRODUTO`,1,50) AS PRODUTO
  ,        CONCAT(UO.COD_COOPERATIVA,'-',UO.COOPERATIVA) AS COOPERATIVA
  ,        UO.COD_COOPERATIVA
  ,        IF(US.ID_STATUS > -1, CONCAT(US.ID_STATUS, '- ', US.DESCRICAO), '') AS STATUS_UNICRED  
  ,        1 AS QNT
  FROM CARTEIRA CT
  ,      CONTRATO C
  ,      PARC_GERAL PG
  ,      PARC_GERAL_UNICRED PGU
  ,      UNICRED_PRODUTO U
  ,      UNICRED_COOPERATIVA UO
  ,      UNICRED_STATUS US
  ,      CODIGO COD
  ,      CODIGO COD2 
  WHERE CT.ID_EMPRESA = 197   
  AND CT.ID_CARTEIRA = C.ID_CARTEIRA   
  AND C.ID_CONTR = PG.ID_CONTR   
  AND PG.ID_CONTR = PGU.ID_CONTR   
  AND PG.NUM_PARC = PGU.NUM_PARC   
  AND PG.NUM_CONTRATO = PGU.NUM_CONTRATO   
  AND PGU.ID_PRODUTO = U.ID_PRODUTO   
  AND PGU.COD_COOPERATIVA = UO.COD_COOPERATIVA   
  AND PGU.ID_STATUS = US.ID_STATUS   
  AND C.`STATUS` = COD.COD   
  AND PG.`STATUS` = COD2.COD
  AND CAST(PG.DATA_CARGA AS DATE) = '",dia_atual,"'      
")
  
  df_devolucoes = dbGetQuery(CON_MYSQL, qry_devolucoes)
  df_entradas = dbGetQuery(CON_MYSQL, qry_entradas)
  df_acionamentos_cslog = dbGetQuery(CON_MYSQL, qry_acionamentos_cslog)
  
  # Fechando conexão do banco de dados MYSQL
  
  dbDisconnect(CON_MYSQL)
  
  # Criando conexão com o banco de dados SQL SERVER (DISCADOR OLOS)
  
  CON_SQLSERVER_OLOS = odbcDriverConnect('driver={SQL Server};server=replica.grupo.schulze;database=Log_Olos;Uid=mis_operacao;Pwd=Mis@schulze#41')
  
  # Consulta de Acionamentos (discador)
  
  qry_acionamentos_discador = paste0("SELECT CustomerId AS 'ID_CONTR'
			 ,DispositionId AS CODIGO
			 ,FORMAT(StartDate, 'yyyy-MM-dd HH:mm:ss') AS DATA,
			 CASE WHEN DispositionId IN (1, 2, 3, 4, 5, 6, 7, 8, 9,10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24) THEN 'MAQUINA'
			 ELSE 'HUMANO' END AS 'STATUS_ACIONAMENTO'
			 ,Route AS NOME
FROM [Log_Olos].[dbo].[AttemptsRawData]
WHERE cast(StartDate as date) >= '",dia_atual,"'
AND CampaignName like '%unicr%'
AND len(CustomerId)>=1
ORDER BY StartDate"
  )
  
  df_acionamentos_discador = RODBC::sqlQuery(CON_SQLSERVER_OLOS, qry_acionamentos_discador)
  
  df_acionamentos_discador = inner_join(df_acionamentos_discador,df_contratos_ativos, by = 'ID_CONTR') %>%
    select(ID_CONTR, CODIGO, DATA, STATUS_ACIONAMENTO, NOME, CARTEIRA)
  
  ## TRATAMENTO DE DEPARA PARA ACIONAMENTOS DO CSLOG
  
  # DEPARA TENTATIVA
  
  depara_tentativa = data.frame(CODIGO =c(2,3,5,6,11,15,18,20,21,22,26,31,37,40,48,51,52,63,77,82,84,89,97,102,132,188,251,275,279,314,316,400,418,424,425,429,508,509,510,600,635,652,662,682,743,759,828,829,878,898,963,983,1085,1090,1096,1121,1152,1157,1192,1254,1362,1549,1551,1738,1829,1890,1891,2086,2092,2333,2375,2396,2590,3905,4612,952,399,541,542,654,683,1159,1475,2296,2298,2535,3086,3782,3789,3915,3962,4055,4067,5968)
                                
  )
  
  for (i in 1:nrow(depara_tentativa)) {
    depara_tentativa$TENTATIVA[i] = 1
  }
  
  analitico_acionamento_crm = merge(df_acionamentos_cslog, depara_tentativa, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_crm$TENTATIVA[is.na(analitico_acionamento_crm$TENTATIVA)] = 0
  
  
  # DEPARA ALO
  
  depara_alo = data.frame(CODIGO = c(3,5,6,11,15,18,22,31,37,77,82,84,89,97,188,251,314,316,400,424,508,509,510,600,682,743,753,878,898,963,983,1085,1096,1121,1152,1549,1738,1829,1890,2086,2092,2333,2375,2590,4612,542,1159,2298,2535,3086,3782,3789,3915,4055,4067,5968
                                     
                                     
  ))
  
  for (i in 1:nrow(depara_alo)) {
    depara_alo$ALO[i] = 1
  }
  
  analitico_acionamento_crm = merge(analitico_acionamento_crm, depara_alo, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_crm$ALO[is.na(analitico_acionamento_crm$ALO)] = 0
  
  
  # DEPARA CPC
  
  depara_cpc = data.frame(CODIGO = c(3,5,6,11,15,18,37,77,84,188,316,400,424,509,600,743,878,898,983,1085,1152,1549,1738,1890,2086,2092,2333,2375,2590,2298,2535,3782,3789,3915,4055,5968))
  
  for (i in 1:nrow(depara_cpc)) {
    depara_cpc$CPC[i] = 1
  }
  
  analitico_acionamento_crm = merge(analitico_acionamento_crm, depara_cpc, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_crm$CPC[is.na(analitico_acionamento_crm$CPC)] = 0
  
  # DEPARA ACORDO
  
  depara_acordo = data.frame(CODIGO = c(600,1738))
  
  for (i in 1:nrow(depara_acordo)) {
    depara_acordo$ACORDO[i] = 1
  }
  
  analitico_acionamento_crm = merge(analitico_acionamento_crm, depara_acordo, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_crm$ACORDO[is.na(analitico_acionamento_crm$ACORDO)] = 0
  
  ## TRATAMENTO DE DEPARA PARA ACIONAMENTOS DO OLOS
  
  # DEPARA TENTATIVA
  
  depara_tentativa_olos = data.frame(CODIGO =c(243,355,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,20,111,291,315,318,323,325,336,379,384,386,102,104,108,112,113,119,121,134,266,272,283,320,337,338,339,340,341,342,343,345,346,350,351,352,353,354,356,357,371,372,373,374,375,376,377,378,380,381,385,387,388,414,415,416,103)
                                     
  )
  
  for (i in 1:nrow(depara_tentativa_olos)) {
    depara_tentativa_olos$TENTATIVA[i] = 1
  }
  
  analitico_acionamento_discador = merge(df_acionamentos_discador, depara_tentativa_olos, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_discador$TENTATIVA[is.na(analitico_acionamento_discador$TENTATIVA)] = 0
  
  # DEPARA ALO
  
  depara_alo_olos = data.frame(CODIGO = c(243,355,102,104,108,112,113,119,121,134,266,272,283,320,337,338,339,340,341,342,343,345,346,350,351,352,353,354,356,357,371,372,373,374,375,376,377,378,380,381,385,387,388,414,415,416,103
                                          
  ))
  
  for (i in 1:nrow(depara_alo_olos)) {
    depara_alo_olos$ALO[i] = 1
  }
  
  analitico_acionamento_discador = merge(analitico_acionamento_discador, depara_alo_olos, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_discador$ALO[is.na(analitico_acionamento_discador$ALO)] = 0
  
  # DEPARA CPC
  
  depara_cpc_olos = data.frame(CODIGO = c(243,355,102,104,108,113,266,352,353,354,371,372,373,374,375,376,377,378,380,381,387,414,415,416,103
  ))
  
  for (i in 1:nrow(depara_cpc_olos)) {
    depara_cpc_olos$CPC[i] = 1
  }
  
  analitico_acionamento_discador = merge(analitico_acionamento_discador, depara_cpc_olos, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_discador$CPC[is.na(analitico_acionamento_discador$CPC)] = 0
  
  # DEPARA ACORDO
  
  depara_acordo_olos = data.frame(CODIGO = c(387,103))
  
  for (i in 1:nrow(depara_acordo_olos)) {
    depara_acordo_olos$ACORDO[i] = 1
  }
  
  analitico_acionamento_discador = merge(analitico_acionamento_discador, depara_acordo_olos, by = "CODIGO", all.x = TRUE)
  
  analitico_acionamento_discador$ACORDO[is.na(analitico_acionamento_discador$ACORDO)] = 0
  
  
  # Consolidação de dataframes de acionamentos CRM e Discador
  
  analitico_acionamentos_consolidado = rbind(analitico_acionamento_crm, analitico_acionamento_discador)
  
  # Salvando o arquivo de Analítico de Acionamentos
  
  caminho_analitico = paste0('//192.168.150.132/powerbi/Fontes_excel/Unicred/Acionamentos/Analitico/',Ano,'/',Mes,' - ',MesNome)
  
  setwd(caminho_analitico)
  
  print(paste0("o arquivo de acionamento diário foi salvo em:",caminho_analitico))
  
  write.table(analitico_acionamentos_consolidado, file = paste0(dia_atual,'.csv'), sep=";", col.names = TRUE, row.names = FALSE, quote = FALSE)
  
  # Salvando o arquivo de devolucoes
  
  caminho_devolucoes = paste0('//192.168.150.132/powerbi/Fontes_excel/Unicred/Acionamentos/Devolucoes/',Ano,'/',Mes,' - ',MesNome)
  
  setwd(caminho_devolucoes)
  
  print(paste0("o arquivo de devoluções foi salvo em:",caminho_devolucoes))
  
  write.table(df_devolucoes, file = paste0(dia_atual,'.csv'), sep=";", col.names = TRUE, row.names = FALSE, quote = FALSE)
  
  # Salvando o arquivo de entradas
  
  caminho_entradas = paste0('//192.168.150.132/powerbi/Fontes_excel/Unicred/Acionamentos/Entradas/',Ano,'/',Mes,' - ',MesNome)
  
  setwd(caminho_entradas)
  
  print(paste0("O arquivo de analítico de contratos ativos do dia foi salvo em:",caminho_entradas))
  
  write.table(df_entradas, file = paste0(dia_atual,'.csv'), sep=";", col.names = TRUE, row.names = FALSE, quote = FALSE)
  
  
  # Salvando arquivo de Base de Contratos ativos do dia
  
  caminho_analitico = paste0('//192.168.150.132/powerbi/Fontes_excel/Unicred/Acionamentos/Contratos/',Ano,'/',Mes,' - ',MesNome)
  print(paste0("O arquivo de analítico de contratos ativos do dia foi salvo em:",caminho_analitico))
  
  setwd(caminho_analitico)
  
  write.table(df_contratos_ativos, file = paste0(dia_atual,'.csv'), sep=";", col.names = TRUE, row.names = FALSE, quote = FALSE)
  
  # Criando Sintético de Acionamentos
  
  analitico_acionamentos_consolidado$DATA = substr(analitico_acionamentos_consolidado$DATA,1,10)
  
  analitico_acionamentos_consolidado$CARTEIRA = toupper(analitico_acionamentos_consolidado$CARTEIRA)
  
  base_analitico = left_join(analitico_acionamentos_consolidado, df_contratos_ativos, by = 'ID_CONTR')
  
  base_analitico$COOPERATIVA[is.na(base_analitico$COOPERATIVA)] = 0 
  
  base_analitico = base_analitico %>%
    dplyr::filter(COOPERATIVA != "0")
  
  base_analitico = as.data.frame(base_analitico [c(1,2,3,4,6,7,8,9,10,13)])
  
  
  sintetico_base = base_analitico %>%
    group_by(ID_CONTR) %>%
    summarize_at(vars(TENTATIVA , ALO, CPC,ACORDO), ~sum(.))
  
  sintetico_acionamento = left_join(df_contratos_ativos, sintetico_base, by = ('ID_CONTR'))
  
  sintetico_acionamento[is.na(sintetico_acionamento)] = 0
  
  # Criando indicadores uniques
  
  sintetico_acionamento$TENTATIVA_UNQ = ifelse(sintetico_acionamento$TENTATIVA > 0,1,0)
  sintetico_acionamento$ALO_UNQ = ifelse(sintetico_acionamento$ALO > 0,1,0)
  sintetico_acionamento$CPC_UNQ = ifelse(sintetico_acionamento$CPC > 0,1,0)
  sintetico_acionamento$ACORDO_UNQ = ifelse(sintetico_acionamento$ACORDO > 0,1,0)
  sintetico_acionamento$BASE = 1
  
  sintetico_acionamento = sintetico_acionamento %>%
    group_by(DATA_BASE, COOPERATIVA, CARTEIRA) %>%
    summarize_at(vars(BASE, TENTATIVA, ALO, CPC, ACORDO, TENTATIVA_UNQ, ALO_UNQ, CPC_UNQ, ACORDO_UNQ), ~sum(.))
  
  # Conectando à base do SQL Server
  
  CON_SQLSERVER = odbcDriverConnect('driver={SQL Server};server=db.grupo.schulze;database=Planejamento;Uid=mis_operacao;Pwd=1tqyFgpU9*5&')
  
  # Excluindo acionamento do dia atual
  
  query_exclusao = paste0(  'SET NOCOUNT ON
                           delete from Planejamento.DBO.DW_UNICRED_ACIONAMENTO_SINTETICO        
                           where DATA_BASE = cast(getdate() as date)')
  
  apagando_registros = sqlQuery(CON_SQLSERVER, query_exclusao)
  
  # Populando a tabela com o sintetico de acionamento
  
  sqlSave(CON_SQLSERVER, sintetico_acionamento, 'DW_UNICRED_ACIONAMENTO_SINTETICO', rownames = FALSE, append = TRUE)
  
  # caminho_sintetico = paste0('J:/Unicred/UNICRED BI PLANEJAMENTO/SINTETICO/',Ano,'/',Mes,' - ',MesNome)
  
  # setwd(caminho_sintetico)
  
  #write.table(sintetico_acionamento, file = paste0(dia_atual,'.csv'), sep=";", col.names = TRUE, row.names = FALSE, quote = FALSE)
  
  print("A inserção dos dados sintético foi realizada em Planejamento.dbo.DW_UNICRED_ACIONAMENTO_SINTETICO")
  
  # Fechando conexão do banco de dados
  odbcClose(CON_SQLSERVER)
  
  # carregando arquivo de insumo de abs
  
  consolidado = list.files(path = "J:/Unicred/UNICRED BI PLANEJAMENTO/ABS", 
                           pattern = "*.xlsx", 
                           full.names = TRUE,
                           include.dirs = TRUE
  ) %>%
    lapply(read_excel) %>%
    bind_rows()
  
  # salvando dados de abs na rede 
  
  setwd('//192.168.150.132/powerbi/Fontes_excel/Unicred/ABS')
  
  write.csv2(x = consolidado, 'abs_unicred.csv', row.names = FALSE)
  
  print(paste0("A Atualização foi finalizada com Sucesso em ",Sys.time()))
  
}, error = function(err) {
  mensagem = paste("Por favor, fechar o script, e, em seguida, abra-o e rode-o novamente. Erro:", err$message)
  print(mensagem)
})

# Atualização de relatório de Desempenho Geral (UNICRED)

tempo_fim = round(Sys.time())

duracao = as.numeric(difftime(tempo_fim, tempo_ini, units = 'secs'))
unidade = ifelse(duracao <= 60, "seg", "min")

print(paste0('Atualização finalizada em ', round(duracao,2), " ", unidade))

