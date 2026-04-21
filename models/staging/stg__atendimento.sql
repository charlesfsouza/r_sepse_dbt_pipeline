WITH source as (
    SELECT
        row_number() over (order by id_atendimento) as pk_atendimento
        , cast(id_atendimento as integer) as id_atendimento
        , cast(id_paciente as integer) as id_paciente
        , cast(id_medico as integer) as id_medico
        , cast(dat_atendimento as timestamp) as dat_atendimento
        , cast(dat_alta as timestamp) as dat_alta  
    FROM {{ source('prontuario_eletronico_paciente', 'tab_atendimento') }}
)
, paciente as (
    SELECT
        pk_paciente,
        id_paciente
    FROM {{ ref('stg__paciente') }}
)
, medico as (
    SELECT
        pk_medico,
        id_medico
    FROM {{ ref('stg__medico') }}
)
, joined as (
    SELECT
        source.pk_atendimento
        , paciente.pk_paciente as fk_paciente
        , medico.pk_medico as fk_medico
        , source.id_atendimento
        , source.id_paciente
        , source.id_medico
        , source.dat_atendimento
        , source.dat_alta
    FROM source
    LEFT JOIN paciente ON source.id_paciente = paciente.id_paciente
    LEFT JOIN medico ON source.id_medico = medico.id_medico
)
select *
from joined