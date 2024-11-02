# Carregando pacotes

pacotes =
  c('hms','dplyr','lubridate','readxl','RPostgreSQL','RODBC','DBI','tidyr','data.table','rlang', 'bizdays', 'withr', 'readbulk', 'bit64','RMySQL','stringr','fs')
lapply(pacotes,library, character.only = TRUE)
rm (pacotes)

# Período

dia_atual = Sys.Date()
dia_semana = format(dia_atual, "%A")

# Loop 

max_iter = ifelse(dia_semana == "segunda-feira",1,2)

contador = 1

# Conectando à base do SQL Server

CON_SQLSERVER_COB = odbcDriverConnect('driver={SQL Server};server=db.grupo.schulze;database=congelado;Uid=mis_operacao;Pwd=1tqyFgpU9*5&')

while (contador <= max_iter) {
  
  # Criando Consultas 
  
  query_cpc_temp = paste0( "

  drop table if exists #baseaci
  drop table if exists #basebd
  drop table if exists #baseteste
  
SET NOCOUNT ON

DECLARE @INI DATETIME
DECLARE @FIM DATETIME
  
SET @INI = (select top 1 dt_data from Planejamento.dbo.DW_Calendario
       where fl_dia_util = 1 and dt_data <= GETDATE()-",contador,"
      order by dt_data desc)
  
  
  --- às segundas-feiras, alterar @INI para a atualização de sábado no formato: '2022-01-08 00:00:00.000'
  --- (select top 1 dt_data from Planejamento.dbo.DW_Calendario
       ---where fl_dia_util = 1 and dt_data <= GETDATE()-1
       ---order by dt_data desc)
  
  SET @FIM = @INI+1
  
  SELECT DISTINCT BC.CD_PROCESSO
  , BC.CD_TITULO
  , BC.CD_DEVEDOR
  ,nr_atraso
  into #basebd
  FROM CONGELADO.DBO.BASEDISTRIBUICAO AS BC WITH (NOLOCK)
  WHERE (BC.DT_BASE>= @INI 
         AND BC.DT_BASE < @FIM)
  AND BC.CD_GRUPO_CLIENTE = 4
  
  
  SELECT DISTINCT AC.CD_PROCESSO
  , AC.NR_TELEFONE AS TELEFONE
  , ac.cd_id id
  ,fl_cpc --- modificado de alo para cpc  
  into #baseaci 
  FROM Cobranca.DBO.ACIONAMENTO AS AC WITH (NOLOCK)
  inner join Cobranca..AcionamentoContatoResultado acr on acr.cd_acionamento_contato = ac.cd_acionamento_contato and acr.cd_acionamento_resultado = ac.cd_acionamento_resultado
  WHERE ac.cd_processo in (select cd_processo from #basebd )
  and LEN(AC.NR_TELEFONE) > 9
  AND (AC.DT_DATA>= @INI AND AC.DT_DATA < @FIM)
  AND fl_cpc =1
  ")
  
  print(query_cpc_temp)                           
  query_cpc_final = paste0("SELECT
                           DISTINCT BD.CD_PROCESSO
                           ,(CASE
                             WHEN CL.NR_CPF <= 0 
                             THEN CL.NR_CGC
                             WHEN CL.NR_CPF > 0 
                             THEN CL.NR_CPF
                             WHEN CL.NR_CPF IS NULL 
                             THEN CL.NR_CGC END) 
                           AS CPF_CNPJ,
                           CL2.nr_cpf AS CPF_ACIONADOR
                           ,CAST(AC2.DT_DATA AS DATE) AS DATA
                           ,CONVERT(TIME(0), AC2.DT_DATA, 0) AS HORA
                           ,BD.CD_TITULO
                           ,TAB.TELEFONE
                           ,AR.DE_ACIONAMENTO_RESULTADO
                           ,(select cd_acionador 
                             from Cobranca.dbo.acionamento as aci with (nolock) 
                             where aci.cd_id = tab.id) 
                           as ID_OPER
                           ,(select case when cd_tipo_chamada in (0,2,10,12,13,14,16,17) THEN 'DIGITAL' ELSE 'VOZ' END AS TIPO_CHAMADA
                             from Cobranca.dbo.Acionamento as aci with (nolock)
                             where aci.cd_id = tab.id) as TIPO_CHAMADA
                           ,ISNULL((select isnull(nr_duracao / 86400,0) * 1
                                    from Cobranca.dbo.AcionamentoDiscador as ad with (nolock) 
                                    where ad.cd_processo = tab.cd_processo 
                                    and ad.cd_id = tab.id),0) 
                           as TEMPO_CHAMADA
                           , Cobranca.dbo.MostraUsuario(cd_acionador) AS ACIONADOR
                           ,cl.no_cliente AS Cliente
                           , CASE WHEN AC2.cd_acionamento_contato = 98 THEN 'RECEPTIVO' ELSE 'ATIVO' END AS 'ORIGEM',
                           nr_atraso AS Atraso
                           
                           FROM(SELECT *
                                  FROM #baseaci AS AC WITH (NOLOCK)) 
                                AS TAB,
                                (SELECT *
                                   FROM #basebd AS BC WITH (NOLOCK)) 
                                 AS BD,
                                 Cobranca.DBO.ACIONAMENTO AS AC2 WITH (NOLOCK),
                                 COBRANCA.DBO.ACIONAMENTORESULTADO AS AR WITH (NOLOCK),
                                 COBRANCA.DBO.CLIENTE AS CL WITH (NOLOCK),
                                 COBRANCA.DBO.CLIENTE AS CL2 WITH (NOLOCK)
                                 WHERE (BD.CD_PROCESSO = TAB.CD_PROCESSO)
                                 AND TAB.ID = AC2.CD_ID
                                 AND AC2.CD_ACIONAMENTO_RESULTADO = AR.CD_ACIONAMENTO_RESULTADO
                                 AND CL.CD_CLIENTE = BD.CD_DEVEDOR
                                 AND CL2.CD_CLIENTE = AC2.cd_acionador
                                 ORDER BY HORA ASC
  
")
  
  # Carregando o dataset
  
  df_cpc_1_temp = sqlQuery(CON_SQLSERVER_COB, query_cpc_temp)
  df_cpc_1 = sqlQuery(CON_SQLSERVER_COB, query_cpc_final)
  
  # Criando variáveis de tempo
  
  data_acionamento = head(df_cpc_1$DATA, 1)
  dia = substr(data_acionamento,9,10)
  mes = substr(data_acionamento,6,7)
  ano = substr(data_acionamento,1,4)
  
  data_americana = as.Date(paste0(ano,'-',mes,'-',dia))
  mostraMes = months(data_americana)
  
  data_arquivo = paste(ano,mes,dia, sep = "")
  
  # Carregando o dataset de acionamento
  
  caminho_arquivo = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/Novo_Acionamento_Diario/',ano,'/',mostraMes,'/','1_jud_financeira_schulze_ativo_',data_arquivo,'.txt')
  
  acionamento_df = read.csv(caminho_arquivo, sep = "|")
  
  acionamento_df = as.data.frame(acionamento_df [c(7)])
  
  # Bate de Blocklist
  
  acionamento_df_bate = acionamento_df[!duplicated(acionamento_df),]
  acionamento_df$bate = 1
  
  names(df_cpc_1)[names(df_cpc_1) == "CD_TITULO"] = "contrato"
  
  df_cpc_1 = left_join(df_cpc_1, acionamento_df, by = "contrato")
  
  # tratando registros n/a dos acionamentos
  
  df_cpc_1 = df_cpc_1[!duplicated(df_cpc_1),]
  
  df_cpc_1[is.na(df_cpc_1)] = 0
  
  # filtrando os dados que não constam na block
  
  df_cpc_1 = df_cpc_1%>%dplyr::filter(bate!=0)
  
  df_cpc_1  = as.data.frame(df_cpc_1 [c(1:15)])
  
  # Renomeando colunas 
  
  names(df_cpc_1)[names(df_cpc_1) == 'CPF_CNPJ'] = "NR_CPF_CNPJ"
  names(df_cpc_1)[names(df_cpc_1) =='Cliente'] = "NM_CLIE"
  names(df_cpc_1)[names(df_cpc_1) == 'ACIONADOR'] = "Nome_Cadastrador"
  names(df_cpc_1)[names(df_cpc_1) == 'CPF_ACIONADOR'] = "CPF_OPER"
  
  df_cpc_1$NR_CPF_CNPJ = as.character(df_cpc_1$NR_CPF_CNPJ)
  df_cpc_1$CPF_OPER = as.character(df_cpc_1$CPF_OPER)
  
  # Criando Colunas
  df_cpc_1$TP_PESS = ifelse(nchar(df_cpc_1$`NR_CPF_CNPJ`) <= 11, "PF", "PJ")
  df_cpc_1$DDD = substr(df_cpc_1$TELEFONE,1,2)
  df_cpc_1$Telefone1 = substr(df_cpc_1$TELEFONE,3,15)
  df_cpc_1$Carteira = "Leves"
  df_cpc_1$Canal = "RECUPERAÇÃO DE CRÉDITO"
  df_cpc_1$Fila = "FINANCEIRA"
  df_cpc_1$NM_EMP = "SERGIO SCHULZE & ADVOGADOS ASSOCIADOS"
  df_cpc_1$ID_EMP = '8006030033'
  df_cpc_1$Mes = substr(df_cpc_1$DATA,6,7)
  
  # Função para ajustar o CNPJ e CPF
  ajustar_id <- function(id) {
    # Extrai os números restantes
    numeros <- substr(id, 2, nchar(id))
    
    # Adiciona zeros à esquerda até que a parte numérica tenha 15 caracteres
    numeros_ajustados <- str_pad(numeros, width = 15, pad = "0")
    
    return(numeros_ajustados)
  }
  
  # Aplica a função ao dataframe
  
  df_cpc_1$NR_CPF_CNPJ = sapply(df_cpc_1$NR_CPF_CNPJ, ajustar_id)
  
  # criando faixas de atraso
  
  df_cpc_1$FAIXA_ATRASO = ifelse(df_cpc_1$Atraso < 61, '01 a 60',
                                 ifelse(df_cpc_1$Atraso  < 91, '61 a 90',
                                        ifelse(df_cpc_1$Atraso  < 181, '91 a 180',
                                               ifelse(df_cpc_1$Atraso  < 361, 'Over 180', 'Prejuízo'))))
  
  # Reordenando as colunas do dataset
  
  df_cpc_1  = as.data.frame(df_cpc_1 [c(2,16:18,13,22,23,3,9,24,4,5,8,12,6,20,21,19,25,10)])
  
  # Renomeando colunas
  
  names(df_cpc_1)[names(df_cpc_1) =='Telefone1'] = "TELEFONE"
  names(df_cpc_1)[names(df_cpc_1) =='TIPO_CHAMADA'] = "Ilha"
  
  # Carregando arquivo de DEPARA de Descrição
  
  depara_resultado = read_xlsx('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/DEPARA/DEPARA_2024.xlsx')
  
  depara_resultado = as.data.frame(depara_resultado)
  
  df_cpc_1 = merge(df_cpc_1, depara_resultado, by = c("DE_ACIONAMENTO_RESULTADO","DE_ACIONAMENTO_RESULTADO"))
  
  # Tratando casos com valor ausente
  
  df_cpc_1[is.na(df_cpc_1 )] = "X"
  df_cpc_1  = df_cpc_1%>%dplyr::filter('Finalização'!= "X")
  
  # Filtando dados que não tem descrições no compliance
  
  unique(df_cpc_1$`Descrição`)
  df_cpc_1 = df_cpc_1%>%dplyr::filter(!(Descrição %in% c("FALECIDO", "DESCONHECE CLIENTE", "INVÁLIDO", "DNR NO RING BACK")))
  
  # Reordenando as colunas do dataset
  
  df_cpc_1  = as.data.frame(df_cpc_1 [c(2:13,22,14:20)])
  
  # Renomeando Coluna
  
  names(df_cpc_1)[names(df_cpc_1) =='Descrição'] = "status_acionamento"
  unique(df_cpc_1$`status_acionamento`)
  
  # Criando coluna de Status_Cobranca
  
  df_cpc_1$Status_Cobranca = ifelse(df_cpc_1$status_acionamento %in% c("CASH ATUALIZACAO", "RENEG", "CASH QUITACAO", "CASH PARCIAL"),"NEGÓCIO", "NÃO NEGÓCIO")
  
  # Criando coluna Produto
  
  df_cpc_1$Produto = ifelse(df_cpc_1$status_acionamento %in% c("CASH ATUALIZACAO", "RENEG", "CASH QUITACAO", "CASH PARCIAL"), df_cpc_1$status_acionamento,"")
  
  # Criando e Formatando coluna Data_Acioamento
  
  df_cpc_1$dia = substr(df_cpc_1$DATA,9,10)
  df_cpc_1$mes = substr(df_cpc_1$DATA,6,7)
  df_cpc_1$ano = substr(df_cpc_1$DATA,1,4)
  
  df_cpc_1$Data_Acionamento = paste0(df_cpc_1$dia,'/',df_cpc_1$mes,'/',df_cpc_1$ano)
  
  # Criando de Formatando coluna Dt_Base
  
  data_atual = Sys.Date()
  
  data_atual_dia = substr(data_atual,9,10)
  data_atual_mes = substr(data_atual,6,7)
  data_atual_ano = substr(data_atual,1,4)
  
  df_cpc_1$Dt_Base = paste0(data_atual_dia,'/',data_atual_mes,'/',data_atual_ano)
  
  # Carregando arquivo de Pagamentos do dia referente
  
  data_arquivo = tail(df_cpc_1$Data_Acionamento,1)
  data_arquivo = gsub("/",".",data_arquivo)
  dir_arquivo = paste0('J:/Santander/Gerencial/Rotinas/1 - Santander/2 - Boletos Pagos/',ano,'/',mostraMes,'/','Santander_BoletosPagos_',data_arquivo,'.xlsx')
  
  df_pagamentos = read_xlsx(dir_arquivo)
  
  # Criando dataset de pagamentos
  
  df_pagamentos = as.data.frame(df_pagamentos [c(1,4)])
  
  df_pagamentos$"Tipo Boleto" = ifelse(df_pagamentos$"Tipo Boleto" == "ATUALIZAÇÃO", "CASH ATUALIZACAO",
                                       ifelse(df_pagamentos$"Tipo Boleto" == "PARCIAL", "CASH PARCIAL", "CASH QUITAÇÃO"))
  
  # Bate de Pagamentos
  
  df_pagamentos = df_pagamentos[!duplicated(df_pagamentos),]
  
  names(df_pagamentos)[names(df_pagamentos) == "Contrato"] = "contrato"
  
  # Criando join de contratos que houve pagamentos
  
  df_cpc_1 = left_join(df_cpc_1, df_pagamentos, by = "contrato")
  
  # Renomeando a coluna de Tipo Boleto para oferta
  
  names(df_cpc_1)[names(df_cpc_1) == "Tipo Boleto"] = "oferta"
  
  # Tratando Valores Ausentes
  
  df_cpc_1[is.na(df_cpc_1)] = ""
  
  # Criando coluna DT_RECU
  
  df_cpc_1$DT_RECU = ifelse(df_cpc_1$oferta != "", df_cpc_1$Data_Acionamento,"")
  
  # Criando coluna de marcacao_reclamacao
  
  df_cpc_1$Marcacao_Reclamacao = ""
  
  df_final = as.data.frame(df_cpc_1 [c(1:5,29,6:10,27,26,12,13,21,14:19,22,28,30,20)])
  
  # Renomeando colunas
  
  names(df_final)[names(df_final) == 'HORA'] = "Hora_Acionamento"
  names(df_final)[names(df_final) == 'contrato'] = "contrato_acordo"
  
  # ordenando as linhas pela coluna de início da ligação no dia
  
  df_final = df_final[order(df_final$Hora_Acionamento, decreasing = F),]
  
  nome_var = paste0("df_teste", contador)
  
  assign(nome_var, df_final)
  
  # Imprime o nome da variável criada e seu conteúdo para verificação
  print(paste("Variável criada:", nome_var))
  print(get(nome_var))
  
  # Incrementa o contador
  contador = contador + 1
  
}

if(dia_semana == "segunda-feira"){
  
  # Salvando o arquivo do dia
  
  dir_pasta = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/CPC/',ano,'/',months(Sys.Date()),'/')
  print(dir_pasta)
  
  if (!dir_exists(dir_pasta)) {
    dir_create(dir_pasta)
    cat("Pasta criada:", dir_pasta, '\n')
  } else {
    cat("A Pasta já existe. Não foi necessário criar o diretório: ", dir_pasta, '\n')
  }
  
  dir_save_report = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/CPC/',ano,'/',months(Sys.Date()),'/','SERGIOSCHULZE_FINANCEIRA_',data_atual_dia,data_atual_mes,data_atual_ano,'.xlsx')
  writexl::write_xlsx(x = df_final, path = dir_save_report)
  
  print(paste0('O Report de CPC Santander foi salvo no diretório: ',dir_save_report))
  
  # Fechando conexão do banco de dados
  print("fechando conexão com o banco de dados")
  odbcClose(CON_SQLSERVER_COB)
} else{
  
  df_final = rbind(df_teste1,df_teste2)
  
  # Salvando o arquivo do dia
  
  dir_pasta = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/CPC/',ano,'/',months(Sys.Date()),'/')
  print(dir_pasta)
  
  if (!dir_exists(dir_pasta)) {
    dir_create(dir_pasta)
    cat("Pasta criada:", dir_pasta, '\n')
  } else {
    cat("A Pasta já existe. Não foi necessário criar o diretório: ", dir_pasta, '\n')
  }
  
  dir_save_report = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/CPC/',ano,'/',months(Sys.Date()),'/','SERGIOSCHULZE_FINANCEIRA_',substr(Sys.Date(),9,10),substr(Sys.Date(),6,7),substr(Sys.Date(),1,4),'.xlsx')
  writexl::write_xlsx(x = df_final, path = dir_save_report)
  
  print(paste0('O Report de CPC Santander foi salvo no diretório: ',dir_save_report))
  
  # Fechando conexão do banco de dados
  odbcClose(CON_SQLSERVER_COB)
  # Limpar o console do RStudio
  
  cat("\014")
  print("Conexão com o banco de dados encerrada")
  
}

print(paste0("O relatório foi salvo com êxito em ",dir_pasta))

print(paste0("Caminho do filezilla: /Files/JUDICIAL/BASES DE RELATÓRIO/SCHULZE "))

print(paste0("Por favor, upar arquivo gerado no filezilla e sinalizar envio por email ao banco"))
