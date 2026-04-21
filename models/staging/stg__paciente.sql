with source as (
    select
        row_number() over (order by id_paciente) as pk_paciente
        , cast(id_paciente as integer) as id_paciente
        , cast(id_faixa_etaria_paciente as integer) as id_faixa_etaria
        , cast(id_sexo_paciente as integer) as id_sexo
    from {{ source('prontuario_eletronico_paciente', 'tab_paciente') }}
)
, sexo as (
    select
        pk_sexo
        , id_sexo
    from {{ ref('stg__sexo') }}
)
, faixa_etaria as (
    select
        pk_faixa_etaria
        , id_faixa_etaria
    from {{ ref('stg__faixa_etaria') }}
)
, joined as (
    select
        source.pk_paciente
        , sexo.pk_sexo as fk_sexo
        , faixa_etaria.pk_faixa_etaria as fk_faixa_etaria
        , source.id_paciente
        , source.id_sexo
        , source.id_faixa_etaria
    from source
    left join sexo on source.id_sexo = sexo.id_sexo
    left join faixa_etaria on source.id_faixa_etaria = faixa_etaria.id_faixa_etaria
)
select
*   
from joined