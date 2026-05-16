-- In this file, we calculate various stock indicators using Vectorized SQL, which is
-- more efficient than recursive calculations.
-- Log Return
{% macro cal_log_return(current_value_col, previous_close_price_col) %}
    ln({{ current_value_col }} / {{ previous_close_price_col }})
{% endmacro %}


-- SMA
{% macro cal_sma(current_value_col, period_time, partition_by, order_by ) %}
    avg({{ current_value_col }}) over (
        partition by {{ partition_by }}
        order by
            {{ order_by }} rows between {{ period_time - 1 }} preceding and current row
    )
{% endmacro %}


-- EMA : EMA_today = Price_today * k + EMA_yesterday * (1 - k)
-- Vectorized EMA (No Recursion)
{% macro cal_ema_vectorized(close_price_col, row_num_col, period_time, partition_by, order_by) %}

    {% set k = 2.0 / (period_time + 1) %}
    {% set beta = 1.0 - k %}

    -- We use the sum of weighted prices divided by the sum of weights
    (
        sum({{ close_price_col }} * power({{ beta }}, - (({{ row_num_col }})))) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
            rows between unbounded preceding and current row
        )
    ) / (
        sum(power({{ beta }}, - (({{ row_num_col }})))) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
            rows between unbounded preceding and current row
        )
    )

{% endmacro %}


-- daily range (ATR)
-- true range (TR)
{% macro cal_true_range(day_high_col, day_low_col, close_price_col, previous_close_price_col, partition_by, order_by) %}
    greatest(
        {{ day_high_col }} - {{ day_low_col }},
        abs({{ day_high_col }} - {{ previous_close_price_col }}),
        abs({{ day_low_col }} - {{ previous_close_price_col }})
    )
{% endmacro %}


-- ATR
{% macro cal_atr(true_range_col, partition_by, order_by, period_time) %}
    avg({{ true_range_col }}) over (
        partition by {{ partition_by }}
        order by
            {{ order_by }} rows between {{ period_time - 1 }} preceding and current row
    )
{% endmacro %}


-- ATR with Wilder's Smoothing
{% macro cal_atr_wilder(tr_col, period_time, partition_by, order_by) %}

    {% set alpha = 1.0 / period_time %}
    {% set beta = 1.0 - alpha %}

    (
        sum(
            {{ tr_col }} * power(
                {{ beta }},
                - (
                    row_number() over (
                        partition by {{ partition_by }} order by {{ order_by }}
                    )
                )
            )
        ) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
            rows between unbounded preceding and current row
        )
    ) / (
        sum(
            power(
                {{ beta }},
                - (
                    row_number() over (
                        partition by {{ partition_by }} order by {{ order_by }}
                    )
                )
            )
        ) over (
            partition by {{ partition_by }}
            order by {{ order_by }}
            rows between unbounded preceding and current row
        )
    )

{% endmacro %}


-- Professional RSI (Wilder's Smoothing)
{% macro cal_rsi_wilder(gain_col, loss_col, row_num_col, period_time, partition_by, order_by) %}

    {% set alpha = 1.0 / period_time %}
    {% set beta = 1.0 - alpha %}

    {# 
       We replace the nested ROW_NUMBER() with the passed row_num_col.
       This prevents the "Analytic function cannot be an argument" error.
    #}
    {% set smoothed_gain = "SUM(" ~ gain_col ~ " * POWER(" ~ beta ~ ", - " ~ row_num_col ~ ")) OVER (PARTITION BY " ~ partition_by ~ " ORDER BY " ~ order_by ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)" %}
    {% set smoothed_loss = "SUM(" ~ loss_col ~ " * POWER(" ~ beta ~ ", - " ~ row_num_col ~ ")) OVER (PARTITION BY " ~ partition_by ~ " ORDER BY " ~ order_by ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)" %}

    case
        when ({{ smoothed_loss }}) = 0
        then 100
        else 100 - (100 / (1 + (({{ smoothed_gain }}) / ({{ smoothed_loss }}))))
    end

{% endmacro %}


-- bollinger Bands 
{% macro cal_bollinger_band(sma_col_20, stddev_col_20, multiplier=2, direction='upper') %}
    {% if direction == 'upper' %}
        {{ sma_col_20 }} + ({{ multiplier }} * {{ stddev_col_20 }})
    {% else %} {{ sma_col_20 }} - ({{ multiplier }} * {{ stddev_col_20 }})
    {% endif %}

{% endmacro %}


-- Cumulative Return (inverse the LN of log return)
{% macro cumulative_return_from_log(log_return_col, partition_by, order_by, period_time=None) %}
    {% if period_time is none %}
        exp(
            sum({{ log_return_col }}) over (
                partition by {{ partition_by }} order by {{ order_by }}
            )
        )
        - 1

    {% else %}
        exp(
            sum({{ log_return_col }}) over (
                partition by {{ partition_by }}
                order by
                    {{ order_by }} rows
                    between {{ period_time - 1 }} preceding and current row
            )
        )
        - 1

    {% endif %}
{% endmacro %}


-- Annualized Rolling Volatility
{% macro cal_volatility(log_return_col, period_time, partition_by, order_by) %}
    stddev({{ log_return_col }}) over (
        partition by {{ partition_by }}
        order by
            {{ order_by }} rows between {{ period_time - 1 }} preceding and current row
    )
    * sqrt(252)  -- 252 trading days in a year
{% endmacro %}
