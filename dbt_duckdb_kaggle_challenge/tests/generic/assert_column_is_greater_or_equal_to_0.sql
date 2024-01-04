-- to test column should NOT be smaller than 0
{% test assert_column_is_greater_or_equal_to_0(model, column_name) %}
SELECT
    {{column_name}}
from {{ model }}
where {{column_name}} < 0

{%endtest%}