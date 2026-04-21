with source as (
    select
        row_number() over (order by id_faixa_etaria) as pk_faixa_etaria
        , cast(id_faixa_etaria as integer) as id_faixa_etaria
        , cast(dsc_faixa_etaria as string) as dsc_faixa_etaria
    from {{ source('prontuario_eletronico_paciente', 'tab_faixa_etaria') }}
)
select
*
from source