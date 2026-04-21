WITH source as (
    SELECT
        row_number() over (order by id_avaliacao) as pk_avaliacao
        , cast(id_avaliacao as integer) as id_avaliacao
        , cast(id_atendimento as integer) as id_atendimento
        , cast(id_medico as integer) as id_medico
        , cast(ind_marcacao_sepse as boolean) as ind_marcacao_sepse
        , cast(dat_avaliacao as timestamp) as dat_avaliacao
    FROM {{ source('prontuario_eletronico_paciente', 'tab_avaliacao') }}
)
, atendimento as (
    SELECT
        pk_atendimento,
        id_atendimento
    FROM {{ ref('stg__atendimento') }}
)
, medico as (
    SELECT
        pk_medico,
        id_medico
    FROM {{ ref('stg__medico') }}
)
, joined as (
    SELECT
        source.pk_avaliacao
        , atendimento.pk_atendimento as fk_atendimento
        , medico.pk_medico as fk_medico
        , source.id_avaliacao
        , source.id_atendimento
        , source.id_medico
        , source.ind_marcacao_sepse
        , source.dat_avaliacao

    FROM source
    LEFT JOIN atendimento ON source.id_atendimento = atendimento.id_atendimento
    LEFT JOIN medico ON source.id_medico = medico.id_medico
)
select *
from joined