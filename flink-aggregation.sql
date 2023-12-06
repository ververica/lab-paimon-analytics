ALTER TABLE test SET (
    'snapshot.time-retained'='5s',
    'snapshot.num-retained.min'='1',
    'snapshot.num-retained.max'='1',
    'full-compaction.delta-commits' = '1'
);

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);
USE CATALOG my_catalog;

CREATE TEMPORARY TABLE customers_gen (
    id INT PRIMARY KEY NOT ENFORCED,
    name STRING,
    country STRING,
    postal_code STRING
) WITH (
 'connector' = 'faker',
 'number-of-rows' = '100',   
 'fields.id.expression' = '#{number.numberBetween ''1'',''10''}',
 'fields.name.expression' = '#{harry_potter.character}',
 'fields.country.expression' = '#{Address.country}',
 'fields.postal_code.expression' = '#{number.numberBetween ''100001'',''699999''}'
);
CREATE TEMPORARY TABLE customers_sink(
      PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/my_app_db',   
   'table-name' = 'customers',
   'username' = 'root',
   'password' = 'root'
) LIKE customers_gen (EXCLUDING ALL);

SET 'pipeline.name' = 'Generate Customers into MySQL table customers';
insert into customers_sink select * from customers_gen;

CREATE TABLE customers (
   id INT PRIMARY KEY NOT ENFORCED,
   name STRING,
   country STRING,
   postal_code STRING
 ) WITH (
    'changelog-producer' = 'lookup'
 );


CREATE TEMPORARY TABLE orders_gen (
    order_id INT,
    total INT,
    customer_id INT
) WITH (
 'connector' = 'datagen',
 'fields.order_id.kind' = 'random',
 'fields.order_id.max' = '100',
 'fields.order_id.min' = '1',
 'fields.total.kind' = 'random',
 'fields.total.max' = '1000',
 'fields.total.min' = '10',
 'fields.customer_id.kind' = 'random',
 'fields.customer_id.max' = '10',
 'fields.customer_id.min' = '1',
 'rows-per-second' = '1'
);

CREATE TABLE orders (
    order_id INT,
    total INT,
    customer_id INT,
    proc_time AS PROCTIME()
) WITH (
    'bucket' = '-1',
    'snapshot.time-retained'='60s',
    'snapshot.num-retained.min'='1',
    'snapshot.num-retained.max'='5'
);

insert into orders select * from orders_gen;

SELECT o.order_id, o.total, c.id as customer_id, c.country, c.postal_code
FROM orders AS o
JOIN customers /*+ OPTIONS('lookup.cache-rows'='20000') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;

SELECT o.order_id, o.total, c.id as customer_id, c.country, c.postal_code, o.proc_time
FROM orders AS o
JOIN customers c
ON o.customer_id = c.id;

CREATE TABLE country_sales (
    country STRING,
    total INT,
    customers_count INT,
    PRIMARY KEY (country) NOT ENFORCED
) WITH (
    'merge-engine' = 'aggregation',    
    'fields.total.aggregate-function' = 'sum',
    'fields.customers_count.aggregate-function' = 'sum',
    'changelog-producer' = 'lookup',
    'snapshot.time-retained'='60s',
    'snapshot.num-retained.min'='1',
    'snapshot.num-retained.max'='5',
    'full-compaction.delta-commits' = '1'
);

SET 'execution.checkpointing.interval' = '10 s';

INSERT INTO country_sales
SELECT c.country, SUM(o.total) as total, COUNT(c.id) as customers_count
FROM Orders AS o
JOIN customers /*+ OPTIONS('lookup.cache-rows'='20000') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id
GROUP BY c.country, c.id;

INSERT INTO country_sales /*+ LOOKUP('table'='c', 'retry-predicate'='lookup_miss', 'output-mode'='allow_unordered', 'retry-strategy'='fixed_delay', 'fixed-delay'='1s', 'max-attempts'='600') */
SELECT c.country, o.total, 1 as customers_count
FROM orders as o
JOIN customers /*+ OPTIONS('lookup.cache-rows'='100', 'lookup.async'='true', 'lookup.async-thread-number'='16') */
FOR SYSTEM_TIME AS OF o.proc_time AS c
ON o.customer_id = c.id;

ALTER TABLE orders SET (
    'snapshot.time-retained'='5s',
    'snapshot.num-retained.min'='1',
    'snapshot.num-retained.max'='1'
  --  'full-compaction.delta-commits' = '1'
);

INSERT INTO customers VALUES (10, 'Archie', 'Germany', '111111');
DELETE FROM customers WHERE id=10;