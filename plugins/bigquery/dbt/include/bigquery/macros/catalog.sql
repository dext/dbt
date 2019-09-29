
{% macro bigquery__get_catalog(information_schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}
    {% for information_schema in information_schemas %}
      (
        with tables as (
            select
                project_id as table_database,
                dataset_id as table_schema,
                table_id as original_table_name,

                concat(project_id, '.', dataset_id, '.', table_id) as relation_id,

                row_count,
                size_bytes as size_bytes,
                case
                    when type = 1 then 'table'
                    when type = 2 then 'view'
                    else concat('unknown (', cast(type as string), ')')
                end as table_type,

                REGEXP_CONTAINS(table_id, '^.+[0-9]{8}$') and type = 1 as is_date_shard,
                REGEXP_EXTRACT(table_id, '^(.+)[0-9]{8}$') as shard_base_name,
                REGEXP_EXTRACT(table_id, '^.+([0-9]{8})$') as shard_name

            from {{ information_schema }}.__TABLES__

        ),

        extracted as (

            select *,
                case
                    when is_date_shard then shard_base_name
                    else original_table_name
                end as table_name

            from tables

        ),

        unsharded_tables as (

            select
                table_database,
                table_schema,
                table_name,
                table_type,
                is_date_shard,

                struct(
                    min(shard_name) as shard_min,
                    max(shard_name) as shard_max,
                    count(*) as shard_count
                ) as table_shards,

                sum(size_bytes) as size_bytes,
                sum(row_count) as row_count,

                max(relation_id) as relation_id

            from extracted
            group by 1,2,3,4,5

        ),

        info_schema_columns as (

            select *,
                concat(table_catalog, '.', table_schema, '.', table_name) as relation_id,
                table_catalog as table_database

            from {{ information_schema }}.INFORMATION_SCHEMA.COLUMNS
            where ordinal_position is not null

        ),

        column_stats as (

            select
                table_database,
                table_schema,
                table_name,
                max(relation_id) as relation_id,
                max(case when is_partitioning_column = 'YES' then 1 else 0 end) = 1 as is_partitioned,
                max(case when is_partitioning_column = 'YES' then column_name else null end) as partition_column,    
                max(case when clustering_ordinal_position is not null then 1 else 0 end) = 1 as is_clustered,
                ARRAY_TO_STRING(array_agg(case when clustering_ordinal_position is not null then column_name else null end ignore nulls order by clustering_ordinal_position), ', ') as clustering_columns

            from info_schema_columns
            group by 1,2,3

        ),

        columns as (

            select
                table_catalog as table_database,
                table_schema,
                table_name,
                column_name,
                relation_id,
                ordinal_position as column_index,
                data_type as column_type,
                cast(null as string) as column_comment

            from info_schema_columns

        )

        select
            unsharded_tables.table_database,
            unsharded_tables.table_schema,
            case
                when is_date_shard then concat(unsharded_tables.table_name, '*')
                else unsharded_tables.table_name
            end as table_name,
            unsharded_tables.table_type,

            columns.column_name,
            columns.column_index,
            columns.column_type,
            columns.column_comment,

            '# Date Shards' as `stats__date_shards__label`,
            table_shards.shard_count as `stats__date_shards__value`,
            'The number of date shards in this table' as `stats__date_shards__description`,
            is_date_shard as `stats__date_shards__include`,

            'Shard (min)' as `stats__date_shard_min__label`,
            table_shards.shard_min as `stats__date_shard_min__value`,
            'The first date shard in this table' as `stats__date_shard_min__description`,
            is_date_shard as `stats__date_shard_min__include`,

            'Shard (max)' as `stats__date_shard_max__label`,
            table_shards.shard_max as `stats__date_shard_max__value`,
            'The last date shard in this table' as `stats__date_shard_max__description`,
            is_date_shard as `stats__date_shard_max__include`,

            'Row Count' as `stats__row_count__label`,
            row_count as `stats__row_count__value`,
            'Approximate count of rows in this table' as `stats__row_count__description`,
            (row_count is not null) as `stats__row_count__include`,

            'Approximate Size' as `stats__bytes__label`,
            size_bytes as `stats__bytes__value`,
            'Approximate size of table as reported by BigQuery' as `stats__bytes__description`,
            (size_bytes is not null) as `stats__bytes__include`,

            'Partitioning Type' as `stats__partitioning_column__label`,
            partition_column as `stats__partitioning_column__value`,
            'The partitioning column for this table' as `stats__partitioning_column__description`,
            is_partitioned as `stats__partitioning_column__include`,

            'Clustering Columns' as `stats__clustering_columns__label`,
            clustering_columns as `stats__clustering_columns__value`,
            'The clustering columns for this table' as `stats__clustering_columns__description`,
            is_clustered as `stats__clustering_columns__include`

        -- join using relation_id (an actual relation, not a shard prefix) to make
        -- sure that column metadata is picked up through the join. This will only
        -- return the column information for the "max" table in a date-sharded table set
        from unsharded_tables
        left join columns using (relation_id)
        left join column_stats using (relation_id)
      )

      {% if not loop.last %} union all {% endif %}
    {% endfor %}
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}

{% endmacro %}
