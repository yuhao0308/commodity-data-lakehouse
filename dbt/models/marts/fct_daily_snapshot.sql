with commodities as (

    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        data_source,
        published_at
    from {{ ref('stg_commodities') }}

),

volatility as (

    select
        symbol,
        trade_date,
        sma_20,
        volatility,
        upper_band,
        lower_band,
        percent_b
    from {{ ref('int_volatility_metrics') }}

),

joined as (

    select
        c.symbol,
        c.trade_date,
        c.open_price,
        c.high_price,
        c.low_price,
        c.close_price,
        c.volume,
        c.high_price - c.low_price as daily_range,
        case
            when c.open_price > 0
            then (c.close_price - c.open_price) / c.open_price * 100
        end as daily_return_pct,
        v.sma_20,
        v.volatility,
        v.upper_band,
        v.lower_band,
        v.percent_b,
        c.data_source,
        c.published_at
    from commodities c
    left join volatility v
        on c.symbol = v.symbol
        and c.trade_date = v.trade_date

)

select * from joined
