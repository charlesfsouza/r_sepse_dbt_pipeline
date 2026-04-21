WITH source as (
    SELECT
        row_number() over (order by id_item_prescricao) as pk_item_prescricao
        , cast(id_item_prescricao as integer) as id_item_prescricao
        , cast(id_prescricao as integer) as id_prescricao
        , cast(id_item as integer) as id_item
    FROM {{ source('prontuario_eletronico_paciente', 'tab_item_prescricao') }}
)
, prescricao as (
    SELECT
        pk_prescricao,
        id_prescricao
    FROM {{ ref('stg__prescricao') }}
)
, item as (
    SELECT
        pk_item,
        id_item
    FROM {{ ref('stg__item') }}
)
, joined as (
    SELECT
        source.pk_item_prescricao
        , prescricao.pk_prescricao as fk_prescricao
        , item.pk_item as fk_item
        , source.id_item_prescricao
        , source.id_prescricao
        , source.id_item
    FROM source
    LEFT JOIN prescricao ON source.id_prescricao = prescricao.id_prescricao
    LEFT JOIN item ON source.id_item = item.id_item
)
select *
from joined