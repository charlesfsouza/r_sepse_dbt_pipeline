WITH source as (
    SELECT
        row_number() over (order by id_item) as pk_item
        , cast(id_item as integer) as id_item
        , cast(tipo_item as string) as dsc_tipo_item
        , cast(nome_item as string) as dsc_item
    FROM {{ source('prontuario_eletronico_paciente', 'tab_item') }}
)
select
*
from source