create or refresh streaming live table ${catalog}.silver.silver_order_reviews
(
  constraint valid_review_id expect (review_id is not null and length(trim(review_id)) = 32) on violation drop row,
  constraint valid_order_id expect (order_id is not null and length(trim(order_id)) = 32) on violation drop row,
  constraint valid_review_score expect (review_score is not null and review_score between 1 and 5) on violation drop row
)
comment 'silver layer - cleansed order reviews data (1 row per review)'
tblproperties (
    'quality' = 'silver',
    'pipelines.autooptimize.managed' = 'true',
    'pipelines.autooptimize.zordercols' = 'order_id'
)
as
select
    review_id,
    order_id,
    cast(review_score as int) as review_score,
    trim(review_comment_title) as review_comment_title,
    trim(review_comment_message) as review_comment_message,
    cast(review_creation_date as timestamp) as review_creation_timestamp,
    cast(review_answer_timestamp as timestamp) as review_answer_timestamp,
    case
      when review_comment_message is not null and length(trim(review_comment_message)) > 0
        then true
        else false
    end as has_comment,
    _source_file,
    _file_modified_at,
    _ingested_at,
    _rescued_data,
    current_timestamp() as _processed_at

from stream live.bronze_order_reviews
where _rescued_data is null;
