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
    'changelog-producer' = 'input'
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

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '4';
insert into orders select * from orders_gen;