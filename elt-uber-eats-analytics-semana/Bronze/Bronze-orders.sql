create or refresh streaming table bronze_orders
as
select *,
      _metadata.file_path AS source_file,
      _metadata.file_modification_time AS ingestion_time
FROM stream read_files('/Volumes/ubereats/default/vol-uber-eats-files/kafka_orders_*', format => 'json')