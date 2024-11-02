-- Exemplos de Joins com subconsulta

SELECT DISTINCT  a.cd_processo,
				b.cd_descricao_historico,
				cast(min(dt_historico) as date) as 'data historico'
FROM #BASE a
INNER JOIN (select cd_processo,cd_descricao_historico, dt_historico 
					   from cobranca.dbo.Historico 
					   where dt_historico between '2021-01-01' and '2024-10-30' and cd_descricao_historico in (801,154)) b
	ON a.cd_processo = b.cd_processo
group by a.cd_processo, b.cd_descricao_historico


SELECT max(b.dt_base), a.*, Cobranca.dbo.MostraMotivoAtraso(b.cd_motivo_atraso)  motivo_inadiplencia, Cobranca.dbo.MostraStatusCliente(cd_status_cliente) statusCliente FROM #BASE a inner join (     select dt_base, nr_codigo_cliente,cd_motivo_atraso,cd_status_cliente from Congelado..BaseDistribuicao where cd_grupo_cliente = 26 and dt_base >= '2024-06-01' ) b on a.nr_codigo_cliente collate SQL_Latin1_General_CP1_CI_AS = b.nr_codigo_cliente GROUP BY 