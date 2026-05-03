with 

parametros as (
    select
        48 as dias_para_atendimento_novo
        /* caso o paciente tenha um novo atendimento em um período de 48 horas será considerado continuidade do atendimento anterior*/
)
, simulacao as (
	select 
		99999 as pk_atendimento
		, 1855 as fk_paciente
		, 1 as fk_medico
		, 99999 as id_atendimento
		, 1855 as id_paciente
		, 1 as id_medico
		, strptime('2024-09-26 10:02:00', '%Y-%m-%d %H:%M:%S') + INTERVAL 72 HOUR as dat_atendimento
		, strptime('2024-09-26 10:02:00', '%Y-%m-%d %H:%M:%S') + INTERVAL (72*7) HOUR as dat_alta

)
, source_atendimento as (
	select * 
	from 
		{{ ref('stg__atendimento') }}
)
, stg_atendimento as (
	select * 
	from 
		source_atendimento
	union all 
	select * 
	from 
		simulacao
)
, stg_avaliacao as (
    select 
        *
    from {{ ref('stg__avaliacao') }}
)


, transf_paciente_atendimento as (

	select
		fk_paciente
		, pk_atendimento
		, dat_atendimento
		, dat_alta
		, lag(dat_alta,1) over(partition by fk_paciente order by dat_atendimento) as dat_alta_anterior
	from 
		stg_atendimento
)
, ind_atendimento_novo as (

	select 
	transf_paciente_atendimento.*
	, date_diff('hour',dat_alta_anterior,dat_atendimento) as tempo_entre_atendimentos_em_horas
	, case
		when date_diff('hour',dat_alta_anterior,dat_atendimento) <= dias_para_atendimento_novo then 0
		else 1  
	end ind_atendimento_novo
	
	from 
		transf_paciente_atendimento
		, parametros

)
, seq_paciente_atendimento as (

	select 
		fk_paciente
		, pk_atendimento
		, fk_paciente || sum(ind_atendimento_novo)over(partition by fk_paciente order by dat_atendimento) + 1000   as seq_paciente_atendimento
	from
	    ind_atendimento_novo

)

, transf_avaliacao_atendimento as (
    select 
        stg_avaliacao.*
        , seq_paciente_atendimento.fk_paciente
        , seq_paciente_atendimento.seq_paciente_atendimento
    from stg_avaliacao
    left join seq_paciente_atendimento on stg_avaliacao.fk_atendimento = seq_paciente_atendimento.pk_atendimento

)

, transf_avaliacao_atendimento_ind_caso_novo as (
    select 
        transf_avaliacao_atendimento.*
        , case
            when ind_marcacao_sepse = TRUE and lag(ind_marcacao_sepse,1) over(partition by seq_paciente_atendimento order by dat_avaliacao) = FALSE then 1
            when ind_marcacao_sepse = TRUE and lag(ind_marcacao_sepse,1) over(partition by seq_paciente_atendimento order by dat_avaliacao) is null then 1
            else 0
        end as ind_caso_novo
    from transf_avaliacao_atendimento
)

, transf_avaliacao_atendimento_seq_caso as (
    select 
        transf_avaliacao_atendimento_ind_caso_novo.*
        , sum(ind_caso_novo) over(partition by seq_paciente_atendimento order by dat_avaliacao) as seq_caso
    from transf_avaliacao_atendimento_ind_caso_novo
)

, transf_avaliacao_atendimento_ini_caso as (
    select 
        seq_paciente_atendimento
        , seq_caso
        , min(dat_avaliacao) as dat_ini_caso
    from transf_avaliacao_atendimento_seq_caso
    group by seq_paciente_atendimento, seq_caso
)
, transf_avaliacao_atendimento_fim_caso as (
    select 
        seq_paciente_atendimento
        , seq_caso
        , max(dat_avaliacao) as dat_fim_caso
    from transf_avaliacao_atendimento_seq_caso
    group by seq_paciente_atendimento, seq_caso
)

, int_avaliacao_sepse as (
    select 
        transf_avaliacao_atendimento_seq_caso.*
        , dat_ini_caso
        , dat_fim_caso
    from transf_avaliacao_atendimento_seq_caso
    left join transf_avaliacao_atendimento_ini_caso on transf_avaliacao_atendimento_seq_caso.seq_paciente_atendimento = transf_avaliacao_atendimento_ini_caso.seq_paciente_atendimento and transf_avaliacao_atendimento_seq_caso.seq_caso = transf_avaliacao_atendimento_ini_caso.seq_caso
    left join transf_avaliacao_atendimento_fim_caso on transf_avaliacao_atendimento_seq_caso.seq_paciente_atendimento = transf_avaliacao_atendimento_fim_caso.seq_paciente_atendimento and transf_avaliacao_atendimento_seq_caso.seq_caso = transf_avaliacao_atendimento_fim_caso.seq_caso

)
select 
    *   
from int_avaliacao_sepse