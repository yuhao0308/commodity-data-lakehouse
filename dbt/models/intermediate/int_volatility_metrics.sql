with prices as (

    select
        symbol,
        trade_date,
        close_price
    from {{ ref('stg_commodities') }}

),

rolling_stats as (

    select
        symbol,
        trade_date,
        close_price,
        avg(close_price) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ) as sma_20,
        stddev_pop(close_price) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ) as volatility,
        count(*) over (
            partition by symbol
            order by trade_date
            rows between 19 preceding and current row
        ) as window_size
    from prices

),

bollinger as (

    select
        symbol,
        trade_date,
        close_price,
        sma_20,
        volatility,
        window_size,
        sma_20 + 2 * volatility as upper_band,
        sma_20 - 2 * volatility as lower_band,
        case
            when volatility > 0
            then (close_price - lower_band) / (upper_band - lower_band)
        end as percent_b
    from rolling_stats
    where window_size = 20

)

select * from bollinger
