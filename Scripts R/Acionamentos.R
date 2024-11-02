Acionamentos

# Cronômetro inicial
time_ini = Sys.time() 
print(time_ini)

# Carregando pacotes

pacotes <-
  c('hms','dplyr','lubridate','readxl','RODBC','DBI','tidyr','data.table','rlang', 'bizdays', 'withr', 'readbulk', 'bit64','RMySQL','stringr','fs')
lapply(pacotes,library, character.only = TRUE)
rm (pacotes)

# Conectando à base do SQL Server

CON_SQLSERVER_COB = odbcDriverConnect('driver={SQL Server};server=db.grupo.schulze;database=congelado;Uid=mis_operacao;Pwd=1tqyFgpU9*5&')

# Criando Consultas

qry_acionamentos_1 = paste0("
drop table if exists #baseaci
drop table if exists #basebd
drop table if exists #baseteste

DECLARE @INI DATETIME
DECLARE @FIM DATETIME

SET @INI = (select top 1 dt_data from Planejamento.dbo.DW_Calendario
where fl_dia_util = 1 and dt_data <= GETDATE()-1
order by dt_data desc)

--- às segundas-feiras, alterar @INI para a atualização de sábado no formato: '2022-01-08 00:00:00.000'
--- (select top 1 dt_data from Planejamento.dbo.DW_Calendario
---where fl_dia_util = 1 and dt_data <= GETDATE()-1
---order by dt_data desc)

SET @FIM = @INI+1

SELECT DISTINCT BC.CD_PROCESSO
	, BC.CD_TITULO
	, BC.CD_DEVEDOR
into #basebd
FROM CONGELADO.DBO.BASEDISTRIBUICAO AS BC WITH (NOLOCK)
WHERE (BC.DT_BASE>= @INI 
	AND BC.DT_BASE < @FIM)
	AND BC.CD_GRUPO_CLIENTE = 4


SELECT DISTINCT AC.CD_PROCESSO
	, AC.NR_TELEFONE AS TELEFONE
	, ac.cd_id id
	, fl_alo
	into #baseaci 
FROM Cobranca.DBO.ACIONAMENTO AS AC WITH (NOLOCK)
inner join Cobranca..AcionamentoContatoResultado acr on acr.cd_acionamento_contato = ac.cd_acionamento_contato and acr.cd_acionamento_resultado = ac.cd_acionamento_resultado
WHERE ac.cd_processo in (select cd_processo from #basebd )
and LEN(AC.NR_TELEFONE) > 9
AND (AC.DT_DATA>= @INI AND AC.DT_DATA < @FIM)                  
")

qry_acionamentos_final = paste0("
SELECT
 DISTINCT BD.CD_PROCESSO
	,(CASE
		WHEN CL.NR_CPF <= 0 
			THEN CL.NR_CGC
		WHEN CL.NR_CPF > 0 
			THEN CL.NR_CPF
		WHEN CL.NR_CPF IS NULL 
			THEN CL.NR_CGC END) 
	AS 'CPF/CNPJ'
	,CAST(AC2.DT_DATA AS DATE) AS Data
	,CONVERT(TIME(0), AC2.DT_DATA, 0) AS HORA
	,BD.CD_TITULO as Contrato
	,TAB.TELEFONE as Telefone
	,AR.DE_ACIONAMENTO_RESULTADO
	,(select cd_acionador 
		from Cobranca.dbo.acionamento as aci with (nolock) 
		where aci.cd_id = tab.id) 
	as ACIONADOR
	,(select case when cd_tipo_chamada in (0,2,10,12,13,14) THEN 'DIGITAL' ELSE 'HUMANO' END AS TIPO_CHAMADA
		from Cobranca.dbo.Acionamento as aci with (nolock)
		where aci.cd_id = tab.id) as TIPO_CHAMADA
	,ISNULL((select isnull(nr_duracao / 86400,0) * 1
				from Cobranca.dbo.AcionamentoDiscador as ad with (nolock) 
				where ad.cd_processo = tab.cd_processo 
					and ad.cd_id = tab.id),0) 
	as TEMPO_CHAMADA
	,Cobranca.dbo.MostraUsuario(cd_acionador) AS 'Login Banco'
	, CASE WHEN AC2.cd_acionamento_contato = 98 THEN 'RECEPTIVO' ELSE 'ATIVO' END AS 'ORIGEM'
	,TAB.fl_alo
FROM(SELECT *
		FROM #baseaci AS AC WITH (NOLOCK)) 
	AS TAB,
	(SELECT *
		FROM #basebd AS BC WITH (NOLOCK)) 
	AS BD,
Cobranca.DBO.ACIONAMENTO AS AC2 WITH (NOLOCK),
COBRANCA.DBO.ACIONAMENTORESULTADO AS AR WITH (NOLOCK),
COBRANCA.DBO.CLIENTE AS CL WITH (NOLOCK)
WHERE (BD.CD_PROCESSO = TAB.CD_PROCESSO)
	AND TAB.ID = AC2.CD_ID
	AND AC2.CD_ACIONAMENTO_RESULTADO = AR.CD_ACIONAMENTO_RESULTADO
	AND CL.CD_CLIENTE = BD.CD_DEVEDOR
ORDER BY TAB.fl_alo DESC, HORA ASC")

# Carregando dataset

df_acionamentos = sqlQuery(CON_SQLSERVER_COB, qry_acionamentos_1)
df_acionamentos = sqlQuery(CON_SQLSERVER_COB, qry_acionamentos_final)

# Backup do arquivo de acionamento

df_base = as.data.frame(df_acionamentos)
df_acionamentos = df_base

# Fechando conexão do banco de dados
odbcClose(CON_SQLSERVER_COB)

print("Gerado dados de Acionamentos com Sucesso. Conexão com o banco de dados COB Encerrada")

# criando variáveis de calendário para carregar arquivo da blocklist correspondente

data_arquivo = head(as.data.frame(df_acionamentos[c(3)]),1)
data_arquivo = data_arquivo %>% 
  dplyr::distinct(Data, .keep_all = T)
data_variavel = data_arquivo

dia = substr(data_variavel$Data,9,10)
mes = substr(data_variavel$Data,6,7)
ano = substr(data_variavel$Data,1,4)

data_americana_texto = paste0(ano,"",mes,"",dia)
data_americana = as.Date(paste0(ano,"-",mes,"-",dia))
print(data_americana)
mostraMes = months(data_americana)

# Carregando arquivo da blocklist

df_blocklist = read.csv(paste0('C:/Users/6956/Desktop/Blocklist/Outubro/BLOQUEADOS_',data_americana_texto,'.txt'))

# Particionando os dataframe para tratamento de dados por telefone e tipo pessoa

df_block_telefone = as.data.frame(df_blocklist [c(9)])
df_block_pessoa = as.data.frame(df_blocklist [c(3,4)])

# Tratamento de dados do dataset de tipo pessoa

df_block_pessoa[is.na(df_block_pessoa)] = 0
df_block_pessoa = df_block_pessoa%>%dplyr::filter(cpf_cnpj_reclamante!=0)
df_block_pessoa$cpf_cnpj_reclamante = as.character(df_block_pessoa$cpf_cnpj_reclamante)
df_block_pessoa$tipo = ifelse(df_block_pessoa$tp_pessoa_reclamante == "PF", "F", "J")
df_block_pessoa$pessoa = paste(df_block_pessoa$tipo,df_block_pessoa$cpf_cnpj_reclamante , sep = "")

# Função para ajustar o CNPJ e CPF
ajustar_id <- function(id) {
  # Extrai a letra inicial
  letra <- substr(id, 1, 1)
  
  # Extrai os números restantes
  numeros <- substr(id, 2, nchar(id))
  
  # Adiciona zeros à esquerda até que a parte numérica tenha 15 caracteres
  numeros_ajustados <- str_pad(numeros, width = 15, pad = "0")
  
  # Combina a letra inicial com os números ajustados
  id_ajustado <- paste0(letra, numeros_ajustados)
  
  return(id_ajustado)
}

# Aplica a função ao dataframe

df_block_pessoa$novo_cpf = sapply(df_block_pessoa$pessoa, ajustar_id)

# Modelando dataframes de blocklist

df_block_pessoa = as.data.frame(df_block_pessoa [c(5)])

# Criando coluna de bate no dataset

df_block_pessoa$bate = 1
df_block_telefone$bate = 1

# bate de block list de número telefônico
df_acionamentos$Telefone = as.character(df_acionamentos$Telefone)
df_block_telefone$telefone_agrupado = as.character(df_block_telefone$telefone_agrupado)
df_acionamentos = left_join(df_acionamentos, df_block_telefone, c('Telefone' = 'telefone_agrupado'))

# Tratando registros ausentes, do bate, com 0

df_acionamentos[is.na(df_acionamentos)] = 0

# filtrando os dados que não constam na block de telefone

df_acionamentos = df_acionamentos%>%dplyr::filter(bate==0)

# Limpeza e Reordem das colunas do dataset (no novo acionamento, manter a coluna "Origem")

df_acionamentos = as.data.frame(df_acionamentos [c(2:12)])

# Tratamento de formato das colunas

df_acionamentos$TIPO_PESSOA = ifelse(nchar(df_acionamentos$`CPF/CNPJ`) <= 11, "F", "J")
df_acionamentos$`Login Banco` = ifelse(df_acionamentos$`Login Banco` == "","MAQUINA", df_acionamentos$`Login Banco`)
df_acionamentos$`Tipo Acionamento` = ifelse(df_acionamentos$`Login Banco` == "MAQUINA", "MAQUINA", "ACIONADOR")
df_acionamentos$Telefone = as.character(df_acionamentos$Telefone)
df_acionamentos$`CPF/CNPJ` = as.character(df_acionamentos$`CPF/CNPJ`)

# Valida Caracteres

df_acionamentos$Valida_Char = ifelse(nchar(df_acionamentos$Telefone) >= 10 & nchar(df_acionamentos$Telefone) <= 11, "Sim", "Não")
df_acionamentos = df_acionamentos%>%dplyr::filter(Valida_Char == "Sim")

# Validando DDD e Dígito

df_acionamentos$DDD = substr(df_acionamentos$`Telefone`,1,2)
df_acionamentos$Valida_Digito = substr(df_acionamentos$`Telefone`,3,3)
df_acionamentos$Valida_Digito = as.integer(df_acionamentos$Valida_Digito)
df_acionamentos = df_acionamentos%>%dplyr::filter(df_acionamentos$Valida_Digito > 1)
depara_ddd = read_xlsx('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/DEPARA/DDD.xlsx')
depara_ddd$DDD = as.character(depara_ddd$DDD)
df_acionamentos = left_join(df_acionamentos, depara_ddd, c("DDD" = "DDD"))
df_acionamentos[is.na(df_acionamentos)] = "X"
df_acionamentos = df_acionamentos%>%dplyr::filter(UF != "X")

# Carregando arquivo de DEPARA de Descrição

depara_resultado = read_xlsx('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/DEPARA/DEPARA_2024.xlsx')
df_acionamentos = left_join(df_acionamentos, depara_resultado, by = c("Tipo Acionamento", "DE_ACIONAMENTO_RESULTADO"))

# Tratando casos com valor ausente

df_acionamentos[is.na(df_acionamentos)] = "X"
df_acionamentos = df_acionamentos%>%dplyr::filter(Finalização!= "X")

# Filtando dados que não tem descrições no compliance
df_acionamentos = df_acionamentos%>%dplyr::filter(!(Descrição %in% c("FALECIDO", "DESCONHECE CLIENTE")))

# Coluna de Tipo Atendimento

df_acionamentos$Tipo_Atendimento = ifelse(df_acionamentos$Atendimento == "HM" & df_acionamentos$TIPO_CHAMADA == "DIGITAL", "WT",
                                          ifelse(df_acionamentos$Atendimento == "HM" & df_acionamentos$TIPO_CHAMADA == "HUMANO", "HM",df_acionamentos$Atendimento))
# Criando coluna de Escritório

df_acionamentos$`Escritório` = "SERGIO SCHULZE & ADVOGADOS ASSOCIADOS"

# Reordenando Dataframe
df_bkp = df_acionamentos

df_acionamentos = as.data.frame(df_acionamentos[c(22,1,2,3,4,5,9,10,11,12,18,19,21)])

# Ajustando o formato de CPF

df_acionamentos$novo_cpf = paste0(df_acionamentos$TIPO_PESSOA, df_acionamentos$`CPF/CNPJ`)

# Aplica a função ao dataframe

df_acionamentos$novo_cpf = sapply(df_acionamentos$novo_cpf, ajustar_id)

# Fazendo bate dos CPF e CNPJ que estão bloqueados 

df_acionamentos = left_join(df_acionamentos, df_block_pessoa, c('novo_cpf' = 'novo_cpf'))

# tratando registros n/a dos acionamentos

df_acionamentos[is.na(df_acionamentos)] = 0

# filtrando os dados que não constam na block

df_acionamentos = df_acionamentos%>%dplyr::filter(bate==0)

# Reordenando colunas e substituindo o novo modelo de coluna de data

df_acionamentos = as.data.frame(df_acionamentos [c(1,3,4,7,8,6,5,14,12,11,13,9)])

# renomeando colunas

names(df_acionamentos)[names(df_acionamentos) == 'Data'] = 'data'
names(df_acionamentos)[names(df_acionamentos) == 'novo_cpf'] = 'cpf_cnpj'
names(df_acionamentos)[names(df_acionamentos) == 'Telefone'] = 'telefone'
names(df_acionamentos)[names(df_acionamentos) == 'Descrição'] = 'descricao_atendimento'
names(df_acionamentos)[names(df_acionamentos) == 'ORIGEM'] = 'origem'
names(df_acionamentos)[names(df_acionamentos) == 'Tipo_Atendimento'] = 'tipo_atendimento'
names(df_acionamentos)[names(df_acionamentos) == 'Finalização'] = 'finalizacao'
names(df_acionamentos)[names(df_acionamentos) == 'Contrato'] = 'contrato'
names(df_acionamentos)[names(df_acionamentos) == 'Login Banco'] = 'login_usuario'

# Função para converter tempo em segundos
converter_para_segundos <- function(tempo) {
  partes <- strsplit(tempo, ":")[[1]]
  horas <- as.numeric(partes[1])
  minutos <- as.numeric(partes[2])
  segundos <- as.numeric(partes[3])
  total_segundos <- horas * 3600 + minutos * 60 + segundos
  return(total_segundos)
}

# Aplicando função para conversão de tempo

df_acionamentos$`Início Ligação` = sapply(df_acionamentos$HORA, converter_para_segundos)

df_acionamentos$`Início Ligação` = df_acionamentos$`Início Ligação` / (24*60*60)

df_acionamentos$`Fim Ligação` = df_acionamentos$`Início Ligação` + df_acionamentos$`TEMPO_CHAMADA`

# Criando função que converte para formato de hms

converter_para_formato_hms <- function(segundos) {
  total_segundos <- segundos * 24 * 60 * 60  # multiplicado pelo total de segundos em um dia
  horas <- floor(total_segundos / 3600)
  minutos <- floor((total_segundos %% 3600) / 60)
  segundos <- round(total_segundos %% 60)
  return(sprintf("%02d:%02d:%02d", horas, minutos, segundos))
}

# Convertendo colunas para o formato de hms
df_acionamentos$`Início Ligação` = sapply(df_acionamentos$`Início Ligação`, converter_para_formato_hms)
df_acionamentos$`Fim Ligação`= sapply(df_acionamentos$`Fim Ligação`, converter_para_formato_hms)

# Reordenando colunas e substituindo o novo modelo de coluna de data

df_acionamentos = as.data.frame(df_acionamentos [c(1,2,13:14,5:12)])

# contando os números de repetições de telefone acionado (o total permitido é 10)

df_acionamentos <- df_acionamentos %>%
  group_by(telefone) %>%
  mutate(contagem = row_number())

# filtrando ligações até 10 repetições

summary(df_acionamentos$contagem)

df_acionamentos = df_acionamentos%>%dplyr::filter(contagem<=10)
df_acionamentos = df_acionamentos%>%dplyr::filter(`Início Ligação`>00:00:00)

summary(df_acionamentos$contagem)

# excluindo cluna de contagem

df_acionamentos = as.data.frame(df_acionamentos [c(1:12)])

# filtrando ddd (quando é necessário por conta de algum feriado)

df_acionamentos$ddd = substr(df_acionamentos$`telefone`,1,2)

unique(df_acionamentos$ddd)

#df_acionamentos = df_acionamentos%>%dplyr::filter(ddd != 51)
#df_acionamentos = df_acionamentos%>%dplyr::filter(ddd != 53)
#df_acionamentos = df_acionamentos%>%dplyr::filter(ddd != 54)
#df_acionamentos = df_acionamentos%>%dplyr::filter(ddd != 55)

unique(df_acionamentos$ddd)

# Carregando dataset time zone

timezone = readxl::read_xlsx('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/DEPARA/Depara_Timezone.xlsx')

# Tratamento de dados para depara

timezone$ini = substr(timezone$`Período INI Santander`,11,19)
timezone$fim = substr(timezone$`Período FIM Santander`,11,19)

timezone = as.data.frame(timezone [c(1,4,5)])

df_acionamentos$ddd = as.character(df_acionamentos$ddd)
timezone$ddd = as.character(timezone$ddd)

df_acionamentos = left_join(df_acionamentos, timezone, by = 'ddd')

df_acionamentos$ini_schulze = sapply(df_acionamentos$`Início Ligação`, converter_para_segundos)
df_acionamentos$ini_schulze  = df_acionamentos$ini_schulze  / (24*60*60)
df_acionamentos$ini_zone = sapply(df_acionamentos$ini, converter_para_segundos)
df_acionamentos$ini_zone  = df_acionamentos$ini_zone / (24*60*60)
df_acionamentos$fim_zone = sapply(df_acionamentos$fim, converter_para_segundos)
df_acionamentos$fim_zone  = df_acionamentos$fim_zone  / (24*60*60)

# Criando condicional de status da time zone

df_acionamentos$status_zone = ifelse((df_acionamentos$ini_schulze < df_acionamentos$ini_zone & df_acionamentos$origem == "ATIVO") | (df_acionamentos$ini_schulze > df_acionamentos$fim_zone & df_acionamentos$origem == "ATIVO"), "Fora do time", "No time")
# Filtrando dados fora do time

df_acionamentos = df_acionamentos%>%dplyr::filter(`status_zone` != 'Fora do time')

# ordenando as linhas pela coluna de início da ligação no dia

df_acionamentos = df_acionamentos[order(df_acionamentos$`Início Ligação`, decreasing = F),]

# Renomenando dataset

names(df_acionamentos)[names(df_acionamentos) == 'Início Ligação'] = 'inicio_ligacao'
names(df_acionamentos)[names(df_acionamentos) == 'Fim Ligação'] = 'fim_ligacao'
names(df_acionamentos)[names(df_acionamentos) == 'Escritório'] = 'escritorio'

# criando variáveis de calendário para automatizar o save

data_arquivo = head(as.data.frame(df_acionamentos[c(2)]),1)
data_arquivo = data_arquivo %>% 
  dplyr::distinct(data, .keep_all = T)
data_variavel = data_arquivo

dia = substr(data_variavel$data,9,10)
mes = substr(data_variavel$data,6,7)
ano = substr(data_variavel$data,1,4)

data_americana = as.Date(paste0(ano,'-',mes,'-',dia))

mostraMes = months(data_americana)

df_acionamentos = as.data.frame(df_acionamentos [c(1:12)])

caminho = paste0('J:/Santander/Gerencial/Rotinas/3 - Acionamentos/Novo_Acionamento_Diario/',ano,'/',mostraMes)
print(caminho)

if (!dir_exists(caminho)) {
  dir_create(caminho)
  cat("Pasta criada:", caminho, '\n')
} else {
  cat("A Pasta já existe. Não foi necessário criar o diretório: ", caminho, '\n')
}

setwd(caminho)

write.table(df_acionamentos, file = paste0('1_jud_financeira_schulze_ativo_',ano,'',mes,'',dia,'.txt'), row.names = FALSE, col.names = TRUE, sep = "|", fileEncoding =  'UTF-8', quote = F )

cat(paste0('Analítico de Acionamentos gerado e salvo com sucesso em ',caminho,' \n\nAbrindo diretório do arquivo gerado.\n'))

shell.exec(caminho)

# Cronômetro Final
time_fim = Sys.time()
duracao = round(time_fim - time_ini)

print(paste0("O Relatório foi gerado em ", duracao, ifelse(duracao<=0.60," Segundos", " Minutos")))
