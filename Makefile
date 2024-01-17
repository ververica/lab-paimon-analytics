compact:
	flink run \
		-D snapshot.expire.limit=100 \
		-D snapshot.time-retained=10m \
		-D execution.runtime-mode=batch \ 
		-D execution.checkpointing.interval='10 s' \
		lib/paimon-flink-action-0.7-20231130.002027-8.jar \
		compact \
		--path /tmp/paimon/default.db/country_sales

merge:	
	flink run -D execution.checkpointing.interval='10 s' \
		lib/paimon-flink-action-0.7-20231130.002027-8.jar \
		merge-into  \
		--warehouse /tmp/paimon \
		--database default \
		--table country_sales \
		--target-as cs \
		--source-sql "CREATE CATALOG my_catalog WITH ('type'='paimon', 'warehouse'='/tmp/paimon')" \
		--source-sql "USE CATALOG my_catalog" \
		--source-sql "CREATE TEMPORARY VIEW S AS SELECT distinct country from customers" \
		--source-table S \
		--on "cs.country = S.country" \
		--merge-actions not-matched-insert,not-matched-by-source-delete \
		--not-matched-insert-values "S.country, 0, 0"

cdc:
	flink run -d \
		-D execution.checkpointing.interval='10 s' \
		lib/paimon-flink-action-0.6.0-incubating.jar \
		mysql-sync-table \
		--warehouse /tmp/paimon \
		--database default \
		--table customers \
		--primary-keys id \
		--mysql-conf hostname=localhost \
		--mysql-conf username=root \
		--mysql-conf password=root \
		--mysql-conf database-name='my_app_db' \
		--mysql-conf table-name='customers' \
		--table-conf changelog-producer=lookup \
		--table-conf sink.parallelism=4 \
		--catalog-conf metastore=filesystem \
		--catalog-conf warehouse=/tmp/paimon