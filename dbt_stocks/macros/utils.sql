{% macro get_lookback_date(reference_model, limit_count=210) %}
    (
        select min(quote_date)
        from
            (
                select quote_date
                from {{ reference_model }}
                where data_quality_status = 'VALID'
                order by quote_date desc
                limit {{ limit_count }}
            )
    )
{% endmacro %}
