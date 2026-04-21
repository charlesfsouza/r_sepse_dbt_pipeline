with source as (
    select
        row_number() over (order by id_sexo) as pk_sexo
        , cast(id_sexo as integer) as id_sexo
        , cast(dsc_sexo as string) as dsc_sexo
    from {{ source('prontuario_eletronico_paciente', 'tab_sexo') }}
)
select
*
from source