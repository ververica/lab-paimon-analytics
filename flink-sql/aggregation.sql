CREATE TABLE country_sales (
    country STRING,
    total INT,
    customers_count INT,
    PRIMARY KEY (country) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',    
    'fields.total.aggregate-function' = 'sum',
    'fields.customers_count.aggregate-function' = 'sum',
    'changelog-producer' = 'full-compaction',
    'snapshot.time-retained'='60s',
    'snapshot.num-retained.min'='1',
    'snapshot.num-retained.max'='5',
    'full-compaction.delta-commits' = '1'
);

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '4';

INSERT INTO country_sales /*+ LOOKUP('table'='c', 'retry-predicate'='lookup_miss', 'output-mode'='allow_unordered', 'retry-strategy'='fixed_delay', 'fixed-delay'='1s', 'max-attempts'='600') */
SELECT c.country, o.total, 1 as customers_count
FROM orders as o
JOIN customers /*+ OPTIONS('lookup.cache-rows'='100', 'lookup.async'='true', 'lookup.async-thread-number'='16') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;