USE [Planejamento]
GO
/****** Object:  StoredProcedure [dbo].[SP_DW_Insert_Calendario]    Script Date: 30/10/2024 08:21:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[SP_DW_Insert_Calendario](@ano as int)

as

---- declaracoes

DECLARE @data_inicial DATETIME
SET @data_inicial = (CAST(((@ano * 100) + 1) As CHAR(6)) + '01')

DECLARE @data_final DATETIME
SET @data_final = (CAST(((@ano * 100) + 12) As CHAR(6)) + '31')


---- calendario total

drop table if exists #dw_calendario;
drop table if exists #base_final;

create table #dw_calendario (
dt_data datetime,
dt_ano int,
dt_mes int,
dt_dia int,
nr_dia_semana int,
nr_dia_ano int,
fl_dia_util int,

)


while (@data_inicial <= @data_final)

begin

	insert into #dw_calendario

	select 
		@data_inicial
		,year(@data_inicial)
		,month(@data_inicial)
		,day(@data_inicial)
		,DATEPART(weekday,@data_inicial)
		,DATEPART(DAYOFYEAR,@data_inicial)
		,null

	set @data_inicial = dateadd(day,+1,@data_inicial)
end


---- calendario feriado

drop table if exists #dw_feriado;


  CREATE TABLE #dw_feriado (
     dt_ano SMALLINT NOT NULL,
     dt_mes SMALLINT NOT NULL,
     dt_dia SMALLINT NOT NULL,
     Tp_Feriado CHAR(1) NULL,
     Ds_Feriado VARCHAR(100) NOT NULL,
     Sg_UF CHAR(2) NOT NULL)
     

    INSERT INTO #dw_feriado values

	-- Feriados nacionais

		(0, 1, 1, 1, 'Confraternização Universal', ''),
		(0, 4, 21, 1, 'Tiradentes', ''),
		(0, 5, 1, 1, 'Dia do Trabalhador', ''),
		(0, 9, 7, 1, 'Independência', '') ,
		(0, 10, 12, 1, 'Nossa Senhora Aparecida', ''),
		(0, 11, 2, 1, 'Finados', ''),
		(0, 11, 15, 1, 'Proclamação da República', ''),
		(0, 12, 25, 1, 'Natal', '')


    -- Feriados móveis

    DECLARE

        @seculo INT,
        @G INT,
        @K INT,
        @I INT,
        @H INT,
        @J INT,
        @L INT,
        @MesDePascoa INT,
        @DiaDePascoa INT,
        @pascoa DATETIME 

  
        SET @seculo = @ano / 100 
        SET @G = @ano % 19
        SET @K = (@seculo - 17) / 25
        SET @I = (@seculo - CAST(@seculo / 4 AS int) - CAST(( @seculo - @K ) / 3 AS int) + 19 * @G + 15) % 30
        SET @H = @I - CAST(@I / 28 AS int) * ( 1 * -CAST(@I / 28 AS int) * CAST(29 / ( @I + 1 ) AS int) ) * CAST(( ( 21 - @G ) / 11 ) AS int)
        SET @J = ( @ano + CAST(@ano / 4 AS int) + @H + 2 - @seculo + CAST(@seculo / 4 AS int) ) % 7
        SET @L = @H - @J
        SET @MesDePascoa = 3 + CAST(( @L + 40 ) / 44 AS int)
        SET @DiaDePascoa = @L + 28 - 31 * CAST(( @MesDePascoa / 4 ) AS int)
        SET @pascoa = CAST(@MesDePascoa AS varchar(2)) + '-' + CAST(@DiaDePascoa AS varchar(2)) + '-' + CAST(@ano AS varchar(4))

        
       INSERT INTO #dw_feriado
        SELECT YEAR(DATEADD(DAY , -2, @pascoa)), MONTH(DATEADD(DAY , -2, @pascoa)), DAY(DATEADD(DAY , -2, @pascoa)), 1, 'Paixão de Cristo', ''
        
       INSERT INTO #dw_feriado
        SELECT YEAR(DATEADD(DAY , -48, @pascoa)), MONTH(DATEADD(DAY , -48, @pascoa)), DAY(DATEADD(DAY , -48, @pascoa)), 1, 'Carnaval', ''
        
       INSERT INTO #dw_feriado
        SELECT YEAR(DATEADD(DAY , -47, @pascoa)), MONTH(DATEADD(DAY , -47, @pascoa)), DAY(DATEADD(DAY , -47, @pascoa)), 1, 'Carnaval', ''
        
       INSERT INTO #dw_feriado
        SELECT YEAR(DATEADD(DAY , 60, @pascoa)), MONTH(DATEADD(DAY , 60, @pascoa)), DAY(DATEADD(DAY , 60, @pascoa)), 1, 'Corpus Christi', '';
     
	    

with cal as (
select
dt_data
,dt_ano
,dt_mes
,dt_dia
,nr_dia_semana
,nr_dia_ano
,fl_dia_util = (
	
	iif(nr_dia_semana in (1,7),0,

		iif(
			(select tp_feriado from #dw_feriado f
			where 
				c.dt_mes = f.dt_mes
			and c.dt_dia = f.dt_dia
			and f.dt_ano =  0
			and f.Tp_Feriado = 1)>0,0,

				iif(
					(select tp_feriado from #dw_feriado f
						where 
							c.dt_mes = f.dt_mes
						and c.dt_dia = f.dt_dia
						and c.dt_ano = f.dt_ano
						and f.Tp_Feriado = 1)>0,0,1

			
		)
	)))

from #dw_calendario c)

select *,
nr_dia_util = (
	select 
		iif(sum(fl_dia_util) = 0 or sum(fl_dia_util) is null ,1,sum(fl_dia_util)) 
		from cal c2 
		where 
			c2.dt_ano = c1.dt_ano 
		and c2.dt_mes = c1.dt_mes 
		and c2.dt_data <= c1.dt_data
		)
into #base_final
from cal c1;

--select * from #base_final;


delete from Planejamento.dbo.dw_calendario
where dt_ano = @ano;

insert into Planejamento.dbo.dw_calendario
select * from #base_final;


