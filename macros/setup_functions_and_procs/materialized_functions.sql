{%- macro materialized_count_agg() -%}
{{ target.database }}.{{ target.schema }}.materialized_count_agg
{%- endmacro -%}

{%- macro materialized_count_union_agg() -%}
{{ target.database }}.{{ target.schema }}.materialized_count_union_agg
{%- endmacro -%}

{%- macro materialized_array_agg() -%}
{{ target.database }}.{{ target.schema }}.array_agg
{%- endmacro -%}

{%- macro materialized_array_agg_distinct() -%}
object_keys
{%- endmacro -%}

{%- macro materialized_array_unique_agg() -%}
object_keys
{%- endmacro -%}

{%- macro materialized_count_add() -%}
{{ target.database }}.{{ target.schema }}.materialized_count_add
{%- endmacro -%}

{%- macro materialized_count_distinct() -%}
{{ target.database }}.{{ target.schema }}.materialized_count_distinct
{%- endmacro -%}

{%- macro materialized_listagg() -%}
{{ target.database }}.{{ target.schema }}.materialized_listagg
{%- endmacro -%}

{%- macro materialized_listagg_distinct() -%}
{{ target.database }}.{{ target.schema }}.materialized_listagg_distinct
{%- endmacro -%}

{%- macro materialized_max() -%}
{{ target.database }}.{{ target.schema }}.materialized_max
{%- endmacro -%}

{%- macro materialized_median() -%}
{{ target.database }}.{{ target.schema }}.materialized_median
{%- endmacro -%}

{%- macro materialized_min() -%}
{{ target.database }}.{{ target.schema }}.materialized_min
{%- endmacro -%}

{%- macro materialized_mode() -%}
{{ target.database }}.{{ target.schema }}.materialized_mode
{%- endmacro -%}

{%- macro materialized_sum() -%}
{{ target.database }}.{{ target.schema }}.materialized_sum
{%- endmacro -%}

{%- macro materialized_sum_distinct() -%}
{{ target.database }}.{{ target.schema }}.materialized_sum_distinct
{%- endmacro -%}

{%- macro create_materialized_count_agg(data_type) -%}
create aggregate function if not exists {{ materialized_count_agg() }}(x {{ data_type }}, row_count int default 1)
    returns object
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'CountAgg'
as '
class CountAgg:
    def __init__(self):
        self._counts = {}

    @property
    def aggregate_state(self):
        return self._counts

    def accumulate(self, x, row_count):
        if x is not None:
            self._counts[x] = self._counts.get(x, 0) + row_count

    def merge(self, counts):
        for x, row_count in counts.items():
            self.accumulate(x, row_count)

    def finish(self):
        return self._counts
'
{%- endmacro -%}

{%- macro create_materialized_count_union_agg() -%}
create aggregate function if not exists {{ materialized_count_union_agg() }}(counts object)
    returns object
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'CountUnionAgg'
as '
class CountUnionAgg:
    def __init__(self):
        self._counts = {}

    @property
    def aggregate_state(self):
        return self._counts

    def accumulate(self, counts):
        for x, row_count in counts.items():
            self._counts[x] = self._counts.get(x, 0) + row_count

    def merge(self, counts):
        self.accumulate(counts)

    def finish(self):
        return self._counts
'
{%- endmacro -%}

{%- macro create_materialized_array_agg() -%}
create function if not exists {{ materialized_array_agg() }}(counts object)
    returns array
    language python
    immutable
    runtime_version = 3.11
    packages = ()
    handler = 'materialized_array_agg'
as '
def materialized_array_agg(counts):
    result = []
    for k, v in counts.items():
        result.extend([k] * v)
    return result
'
{%- endmacro -%}

{%- macro create_materialized_count_add() -%}
create function if not exists {{ materialized_count_add() }}(counts_1 object, counts_2 object)
    returns object
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'count_add'
as '
def count_add(counts_1, counts_2):
    result = counts_1.copy()
    for x, row_count in counts_2.items():
        row_count += result.get(x, 0)
        if row_count == 0:
            del result[x]
        else:
            result[x] = row_count
    return result
'
{%- endmacro -%}

{%- macro create_materialized_count_distinct() -%}
create function if not exists {{ materialized_count_distinct() }}(counts object)
    returns int
as 'array_size(object_keys(counts))'
{%- endmacro -%}

{%- macro create_materialized_listagg() -%}
create function if not exists {{ materialized_listagg() }}(counts object, delimiter varchar default '')
    returns varchar
as 'array_to_string({{ materialized_array_agg() }}(counts), delimiter)'
{%- endmacro -%}

{%- macro create_materialized_listagg_distinct() -%}
create function if not exists {{ materialized_listagg_distinct() }}(counts object, delimiter varchar default '')
    returns varchar
as 'array_to_string(object_keys(counts), delimiter)'
{%- endmacro -%}

{%- macro create_materialized_max() -%}
create function if not exists {{ materialized_max() }}(counts object)
    returns variant
as 'array_max(object_keys(counts))'
{%- endmacro -%}

{%- macro create_materialized_median() -%}
create function if not exists {{ materialized_median() }}(counts object)
    returns variant
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'materialized_median'
as '
def materialized_median(counts):
    total = sum(counts.values())
    i = 0
    results = []
    for k, v in counts.items():
        left = total - (i << 1)
        i += v
        right = total - (i << 1)
        if left * right <= 0:
            results.append(k)
    if len(results) == 1:
        return results[0]
    return 0.5 * results[0] + 0.5 * results[1]
'
{%- endmacro -%}

{%- macro create_materialized_min() -%}
create function if not exists {{ materialized_min() }}(counts object)
    returns variant
as 'object_keys(counts)[0]'
{%- endmacro -%}

{%- macro create_materialized_mode() -%}
create function if not exists {{ materialized_mode() }}(counts object)
    returns variant
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'materialized_mode'
as '
def materialized_mode(counts):
    return max(counts, key=counts.get)
'
{%- endmacro -%}

{%- macro create_materialized_sum() -%}
create function if not exists {{ materialized_sum() }}(counts object)
    returns float
    language python
    runtime_version = 3.11
    packages = ()
    handler = 'materialized_sum'
as '
def materialized_sum(counts):
    flag = False
    total = 0.0
    for k, v in counts.items():
        try:
            flag = True
            total += float(k) * v
        except:
            pass
    return total if flag else None
'
{%- endmacro -%}

{%- macro create_materialized_sum_distinct() -%}
create function if not exists {{ materialized_sum_distinct() }}(counts object)
    returns float
as 'reduce(object_keys(counts)::array(varchar), 0::float, (acc, val) -> acc + zeroifnull(try_to_double(val)))'
{%- endmacro -%}

{%- macro create_materialized_functions() -%}
with create_all as procedure()
    returns int
as $$
begin

let r1 resultset := async ({{ create_materialized_array_agg() }});
let r2 resultset := async ({{ create_materialized_count_agg('binary') }});
let r3 resultset := async ({{ create_materialized_count_agg('boolean') }});
let r4 resultset := async ({{ create_materialized_count_agg('date') }});
let r5 resultset := async ({{ create_materialized_count_agg('float') }});
let r6 resultset := async ({{ create_materialized_count_agg('number') }});
let r7 resultset := async ({{ create_materialized_count_agg('time') }});
let r8 resultset := async ({{ create_materialized_count_agg('timestamp_ltz') }});
let r9 resultset := async ({{ create_materialized_count_agg('timestamp_ntz') }});
let r10 resultset := async ({{ create_materialized_count_agg('timestamp_tz') }});
let r11 resultset := async ({{ create_materialized_count_agg('varchar') }});
let r12 resultset := async ({{ create_materialized_count_agg('variant') }});
let r13 resultset := async ({{ create_materialized_count_union_agg() }});
let r14 resultset := async ({{ create_materialized_count_add() }});
let r15 resultset := async ({{ create_materialized_count_distinct() }});
let r16 resultset := async ({{ create_materialized_listagg_distinct() }});
let r17 resultset := async ({{ create_materialized_max() }});
let r18 resultset := async ({{ create_materialized_min() }});
let r19 resultset := async ({{ create_materialized_mode() }});
let r20 resultset := async ({{ create_materialized_sum() }});
let r21 resultset := async ({{ create_materialized_sum_distinct() }});

await r1;
let r22 resultset := async ({{ create_materialized_listagg() }});

{%- for i in range(2, 23) %}
await r{{ i }};
{%- endfor %}

return 1;

end
$$

call create_all()
{%- endmacro -%}

{% macro setup_materialized_functions() %}
    {% if execute and load_result('__CREATE_MATERIALIZED_FUNCTIONS__') is none %}
        {% call statement('__CREATE_MATERIALIZED_FUNCTIONS__') %}
            {{- create_materialized_functions() -}}
        {% endcall %}
    {% endif %}
    {{ return('select 1 as no_op') }}
{% endmacro %}
