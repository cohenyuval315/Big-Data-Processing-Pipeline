from .cassandra_service import CassandraService


class CassandraSink(CassandraService):

    def write_to_cassandra(self,df, epoch_id,keyspace,table):
        df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

    def write_stream(self,df,table,keyspace="btcusdt"):
        
        df.writeStream \
            .trigger(processingTime="5 seconds") \
            .foreachBatch(lambda df, epoch_id: self.write_to_cassandra(df, epoch_id, keyspace, table)) \
            .outputMode("update") \
            .start() \
            .awaitTermination()
        df.show()