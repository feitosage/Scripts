# Carregando pacotes

pacotes <-
  c('hms','dplyr','lubridate','readxl','RODBC','DBI','tidyr','data.table','rlang', 'bizdays', 'withr', 'readbulk', 'bit64','RMySQL','stringr','fs')
lapply(pacotes,library, character.only = TRUE)
rm (pacotes)

# Variáveis de tempo

ini = '2024-10-01 00:00:00.000' ## definir primeiro o dia do mês
fim = '2024-10-28 23:59:00.000' ## definir o último o dia do mês

primeiro_dia_formatado = substr(ini,1,10)
ultimo_dia_formatado = substr(fim,1,10)
ano = substr(ini,1,4)
nome_mes = months(as.Date(primeiro_dia_formatado))

# Conectando à base do SQL Server

CON_SQLSERVER_COB = odbcDriverConnect('driver={SQL Server};server=db.grupo.schulze;database=congelado;Uid=mis_operacao;Pwd=1tqyFgpU9*5&')

# Criando Consultas

qry_funil_1 = paste0("
drop table if exists #base
drop table if exists #base2
drop table if exists #base3
drop table if exists #funique


select distinct
cd_processo,
cd_titulo,

max(dt_base)dt_base_min

into #base
from (select distinct
		cd_processo,
		nr_codigo_cliente,
	
		dt_base,
		cd_titulo
	 from (select distinct
			bd.cd_processo,
			nr_codigo_cliente,
			dt_cadastro,
			cd_titulo,
			dt_base
		  from Congelado..BaseDistribuicao bd with(nolock)
		  left join Cobranca.dbo.cobrancaBV  cbv on cbv.cd_processo = bd.cd_processo 

		  where dt_base >='",ini,"'
		  and dt_base <='",fim,"'
		  and cd_grupo_cliente in(4)
		  and bd.cd_processo not in(select cd_processo from Cobranca..CobrancaBV with(nolock) where id_filial_arquivo = 1584)		 
		) a
) b

group by
cd_processo,
nr_codigo_cliente,
cd_titulo

CREATE INDEX i1base ON #base (cd_processo);
UPDATE STATISTICS #base;


select *
,(select nr_atraso from congelado.dbo.basedistribuicao as bd where bd.dt_base = bs.dt_base_min and bd.cd_processo = bs.cd_processo) as nr_atraso 
,(select de_uf from congelado.dbo.basedistribuicao as bd where bd.dt_base = bs.dt_base_min and bd.cd_processo = bs.cd_processo) as de_uf

into #base2
from #base as bs

select *
, CASE
	WHEN de_uf = 'MG' THEN 'MINAS GERAIS'
	WHEN de_uf = 'SP' THEN 'SAO PAULO'
	WHEN de_uf IN ('MA', 'PI', 'CE', 'RN') THEN 'NORDESTE 1'
	WHEN de_uf IN ('PB', 'PE','AL','SE','BA') THEN 'NORDESTE 2'
	WHEN de_uf IN ('RS','SC','PR') THEN 'SUL' END Regiao,

CASE 
	WHEN nr_atraso < 31 THEN '0 a 30'
	WHEN nr_atraso < 61 THEN '31 a 60'
	WHEN nr_atraso < 91 THEN '61 a 90'	
	WHEN nr_atraso < 181 THEN '91 A 180'
	WHEN nr_atraso < 361 THEN '181 A 360'
ELSE 'PREJUÍZO' END 'Faixa'

into #base3
from #base2

                    
")

qry_funil_final = paste0("

SELECT  fichas.*

,(	SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock)
	inner join cobranca.dbo.AcionamentoResultado as ar on ar.cd_acionamento_resultado = AC.cd_acionamento_resultado and ar.fl_trabalhado = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data <= '",fim,"' and ac.cd_processo = fichas.cd_processo
) ACIONADO

,(  SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join cobranca.dbo.AcionamentoContatoResultado as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado and acr.fl_alo = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data <= '",fim,"' and ac.cd_processo = fichas.cd_processo
) ALO

,(  SELECT count(cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join (select cd_acionamento_contato,cd_acionamento_resultado from cobranca.dbo.AcionamentoContatoResultado where fl_cpc = 1 or (cd_acionamento_resultado in (16,108))) as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado 
	where  AC.dt_data > = '",ini,"' AND AC.dt_data <= '",fim,"' and ac.cd_processo = fichas.cd_processo

) CPC

,(  SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join cobranca.dbo.AcionamentoContatoResultado as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado and acr.fl_alo = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data < '",fim,"' and ac.cd_processo = fichas.cd_processo AND AC.cd_acionamento_contato = 6
) ALO_DIGITAL

,(  SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join cobranca.dbo.AcionamentoContatoResultado as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado and acr.fl_cpc = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data < '",fim,"' and ac.cd_processo = fichas.cd_processo AND AC.cd_acionamento_contato = 6
) CPC_DIGITAL

,(  SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join cobranca.dbo.AcionamentoContatoResultado as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado and acr.fl_alo = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data < '",fim,"' and ac.cd_processo = fichas.cd_processo AND AC.cd_acionamento_contato NOT IN (6)
) ALO_VOZ

,(  SELECT count(ac.cd_id) 
	FROM Congelado.DBO.Acionamento AS AC with(nolock) 
    inner join cobranca.dbo.AcionamentoContatoResultado as acr 
	on acr.cd_acionamento_contato = AC.cd_acionamento_contato and acr.cd_acionamento_resultado = AC.cd_acionamento_resultado and acr.fl_cpc = 1
	where  AC.dt_data > = '",ini,"' AND AC.dt_data < '",fim,"' and ac.cd_processo = fichas.cd_processo AND AC.cd_acionamento_contato NOT IN (6)
) CPC_VOZ

,(  SELECT count(BG.cd_boleto) 
	FROM Cobranca..BoletoGeral AS BG with(nolock) 
	where  
	BG.dt_cadastro >=  '",ini,"'
	and BG.dt_cadastro <=  '",fim,"'
	AND BG.cd_processo = fichas.cd_processo
    and bG.cd_alteracao not  in ('BOLETAGEM') 
) ACORDO

,(	
select 
count(cd_boleto)
from Cobranca..BoletoGeral bg with(nolock)
where
dt_cadastro = '",ini,"'
and dt_pagamento >= '",ini,"'
and dt_pagamento <= '",fim,"'
and bg.cd_processo = fichas.cd_processo
and bG.cd_alteracao not  in ('BOLETAGEM') 
) as PAGO 
FROM #base3 as fichas

")

# Carregando o dataset

df_funil_qry1 = sqlQuery(CON_SQLSERVER_COB, qry_funil_1)
df_funil_final = sqlQuery(CON_SQLSERVER_COB, qry_funil_final)

# Limpando colunas do dataset

df_funil_final = as.data.frame(df_funil_final [c(1,2,7:10)])

# Criando indicador unique

df_funil_final$acionado_unq = ifelse(df_funil_final$ACIONADO >= 1, 1,0)
df_funil_final$alo_unq = ifelse(df_funil_final$ALO >= 1, 1,0)
df_funil_final$cpc_unq = ifelse(df_funil_final$CPC >= 1, 1,0)

# Reordenando Dataset

df_funil_final = as.data.frame(df_funil_final [c(1:3,7:9)])
df_funil_final$BASE = 1
df_funil_final = as.data.frame(df_funil_final [c(1:3,7,4:6)])

# Renomeando colunas 

names(df_funil_final)[names(df_funil_final) == 'acionado_unq'] = "ACIONADO"
names(df_funil_final)[names(df_funil_final) == 'alo_unq'] = "ALO"
names(df_funil_final)[names(df_funil_final) == 'cpc_unq'] = "CPC"

# Carregando base congelado do mês e seu respectivo atraso

qry_contratos_1 = paste0("drop table if exists #macPreju
select
	distinct cd_processo, cd_marcacao_temporaria 
into #macPreju
from Congelado..BaseDistribuicao bd with(nolock) 
where cd_marcacao_temporaria = 605 and cd_grupo_cliente = 4 and dt_base between '",primeiro_dia_formatado,"' and '",ultimo_dia_formatado,"'
")

qry_contratos_2 = paste0("
                        
select
Contrato,
min_base,
cast (dt_jud as date) dt_judicial,
de_uf as UF,
Atraso  = (
	select  nr_atraso 
	from Congelado.dbo.BaseDistribuicao bd2 with(nolock)
	where a.cd_processo = bd2.cd_processo and a.min_base = bd2.dt_base
) ,
FL_tipo = (
Select  case when cluster like '%Veículo%' then 'Rastreador' else 'Veículo' end cluster 
from Cobranca..CobrancaDados cd 
where cd.cd_processo = a.cd_processo 
),
FL_Prejuizo = isnull((
select cd_marcacao_temporaria 
from #macPreju bd 
where bd.cd_processo = a.cd_processo 
),0)

from(
	SELECT
		DISTINCT cd_processo ,bd.cd_titulo as Contrato,min(dt_base) min_base, de_uf, max(dt_judicial) dt_jud	
	FROM Congelado.dbo.BaseDistribuicao bd with(nolock)
	WHERE
		cd_grupo_cliente  = 4 
		and dt_base  between '",primeiro_dia_formatado,"' and '",ultimo_dia_formatado,"'
	GROUP BY cd_processo, cd_titulo, de_uf
)a
                  
")

# Carregando dataset de Base de contratos do período

df_contratos = sqlQuery(CON_SQLSERVER_COB, qry_contratos_1)
df_contratos = sqlQuery(CON_SQLSERVER_COB, qry_contratos_2)

# Fechando conexão do banco de dados
odbcClose(CON_SQLSERVER_COB)

df_contratos = as.data.frame(df_contratos [c(1,6)])
names(df_contratos)[names(df_contratos) == 'Contrato'] = "cd_titulo"

# Criando coluna de contagem de contratos (verifica se há contrato em duplicado em cooperativas distintas)

df_contratos = df_contratos %>%
  group_by(cd_titulo) %>%
  mutate(CONTAGEM = row_number())

# Removendo contratos duplicados

df_contratos = df_contratos%>%dplyr::filter(CONTAGEM == 1)

# Segregando contratos entre veículo e reastreador

df_funil_final = merge(df_funil_final, df_contratos, by = c('cd_titulo', 'cd_titulo'))

# Tratando registros ausentes

df_funil_final[is.na(df_funil_final)] = "Veículo"
df_funil_final = as.data.frame(df_funil_final [c(1:8)])

# Carregando arquivo de efetevidade de pagamento 

dir_efic = paste0('J:/Santander/Gerencial/Rotinas/1 - Santander/1 - Conferência de Boletos/Efetividade/',ano,'/',nome_mes,'/EFICIÊNCIA E REBOLETAGEM ',nome_mes,'.xlsx')

dataset_efic = read_xlsx(dir_efic, sheet = 'ANALITICO')

acordos = as.data.frame(dataset_efic [c(2)])

acordos = acordos %>%
  group_by(CONTRATO) %>%
  mutate(CONTAGEM = row_number())

acordos = acordos%>%dplyr::filter(CONTAGEM == 1)

names(acordos)[names(acordos) == 'CONTRATO'] = "cd_titulo"

pagos = as.data.frame(dataset_efic [c(2,8)])

pagos = pagos %>%
  group_by(CONTRATO) %>%
  mutate(CONTAGEM = row_number())

pagos = pagos%>%dplyr::filter(CONTAGEM == 1)
pagos = pagos%>%dplyr::filter(PGTO == "SIM")
pagos = as.data.frame(pagos [c(1,3)])

(names(pagos)[names(pagos) == 'CONTRATO'] = "cd_titulo")

# Junções de Acordos e Pagamentos

df_funil_final = left_join(df_funil_final, acordos, by = c('cd_titulo'))

df_funil_final[is.na(df_funil_final)] = 0

names(df_funil_final)[names(df_funil_final) == 'CONTAGEM'] = "ACORDO"

df_funil_final = left_join(df_funil_final, pagos, by = c('cd_titulo'))

df_funil_final[is.na(df_funil_final)] = 0

names(df_funil_final)[names(df_funil_final) == 'CONTAGEM'] = "PAGOS"

names(df_funil_final)[names(df_funil_final) == 'FL_tipo'] = "Carteira"

# Criando insumo para o report door to door

#previa_dados = read_xlsx(path = paste0('J:/Santander/Gerencial/Rotinas/4 - Prévia Banco/1 - Santander/',ano,'/',nome_mes,'/ Previa Santander - Consolidado.xlsx'))

#bkp = previa_dados
#previa_dados = as.data.frame(previa_dados [c(1,3,4)])

#names(previa_dados)[names(previa_dados) == 'CONTRATO'] = "cd_titulo"

#previa_dados$cd_titulo = as.numeric(previa_dados$cd_titulo) 

#teste = merge(previa_dados, df_funil_final, by = 'cd_titulo')

# Criando sintético de Funil

df_funil_final$Data = ultimo_dia_formatado

sintetico_base = df_funil_final %>%
  group_by(Data, Carteira, Faixa) %>%
  summarize_at(vars(BASE, ACIONADO, ALO, CPC, ACORDO, PAGOS), ~sum(.))

sintetico_base$Escritorio = 'SERGIO SCHULZE & ADVOGADOS ASSOCIADOS'

# Renomeando indicadores

names(sintetico_base)[names(sintetico_base) == 'BASE'] = "Worklist"
names(sintetico_base)[names(sintetico_base) == 'ACIONADO'] = "Trabalhado"
names(sintetico_base)[names(sintetico_base) == 'ALO'] = "Completada"
names(sintetico_base)[names(sintetico_base) == 'ACORDO'] = "Promessa"
names(sintetico_base)[names(sintetico_base) == 'PAGOS'] = "Negócio"

# Transformando colunas em linhas

sintetico_base <- sintetico_base %>%
  pivot_longer(cols = c("Worklist", "Trabalhado", "Completada", "CPC", "Promessa","Negócio"),
               names_to = "Status",
               values_to = "Volume")
# Reordenando dataset

sintetico_base = as.data.frame(sintetico_base [c(1,5,6,4,2,3)])

dir_pasta = paste0('J:/Santander/Gerencial/Rotinas/7 - Funil/',ano,'/',nome_mes)
print(dir_pasta)

if (!dir_exists(dir_pasta)) {
  dir_create(dir_pasta)
  cat("Pasta criada:", dir_pasta, '\n')
} else {
  cat("A Pasta já existe. Não foi necessário criar o diretório: ", dir_pasta, '\n')
}

# Formatar data

dia = substr(ultimo_dia_formatado,9,10)
mes = substr(ultimo_dia_formatado,6,7)
data_formata = paste0(dia,'.',mes,'.',ano)

nome_arquivo = paste0('J:/Santander/Gerencial/Rotinas/7 - Funil/',ano,'/',nome_mes,'/Funil Leves ',data_formata,'.xlsx') 
print(nome_arquivo)

writexl::write_xlsx(x = sintetico_base, path = nome_arquivo )
