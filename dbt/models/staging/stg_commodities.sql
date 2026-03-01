with source as (

    select * from {{ source('commodity_lakehouse', 'published_commodities') }}

),

renamed as (

    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        source as data_source,
        published_at

    from source

)

select * from renamed
