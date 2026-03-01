-- Fails if any row has negative volatility.
-- Volatility (standard deviation) is mathematically non-negative;
-- a negative value indicates a computation bug.
select *
from {{ ref('int_volatility_metrics') }}
where volatility < 0
