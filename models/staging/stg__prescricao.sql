WITH source as (
    SELECT
        row_number() over (order by id_prescricao) as pk_prescricao
        , cast(id_prescricao as integer) as id_prescricao
        , cast(id_avaliacao as integer) as id_avaliacao
        , cast(id_atendimento as integer) as id_atendimento
        , cast(id_medico as integer) as id_medico
        , cast(dat_prescricao as timestamp) as dat_prescricao
    FROM {{ source('prontuario_eletronico_paciente', 'tab_prescricao') }}
)
, avaliacao as (
    SELECT
        pk_avaliacao,
        id_avaliacao
    FROM {{ ref('stg__avaliacao') }}
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
        source.pk_prescricao
        , avaliacao.pk_avaliacao as fk_avaliacao
        , atendimento.pk_atendimento as fk_atendimento
        , medico.pk_medico as fk_medico
        , source.id_prescricao
        , source.id_avaliacao
        , source.id_atendimento
        , source.id_medico
        , source.dat_prescricao
    FROM source
    LEFT JOIN avaliacao ON source.id_avaliacao = avaliacao.id_avaliacao
    LEFT JOIN atendimento ON source.id_atendimento = atendimento.id_atendimento
    LEFT JOIN medico ON source.id_medico = medico.id_medico
)
select *
from joined