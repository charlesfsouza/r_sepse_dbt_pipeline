with source as (
    select
        row_number() over (order by id_medico) as pk_medico
        , cast(id_medico as integer) as id_medico
        , cast(nome_paciente as string) as nom_medico   
    from {{ source('prontuario_eletronico_paciente', 'tab_medico') }}
)
select
*
from source