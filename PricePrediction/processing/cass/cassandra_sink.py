class CassandraSink():

    FOMRAT="org.apache.spark.sql.cassandra"
    CHECKPOINT_ARG = "checkpointLocation"

    def __init__(self,checkpoint_location="checkpoints/") -> None:
        self.checkpoint = checkpoint_location

    def write_to_cassandra(self,batch_df, epoch_id,keyspace,table,mode="append"):
        batch_df.write \
        .format(self.FOMRAT) \
        .options(table=table, keyspace=keyspace) \
        .mode(mode) \
        .save()

    def write_stream(self,df,keyspace,table,mode="update",batchmode="append",processingTime="5 seconds",checkpoint=None):
        df.writeStream \
            .option(self.CHECKPOINT_ARG, checkpoint if checkpoint else self.checkpoint) \
            .foreachBatch(lambda df, epoch_id: self.write_to_cassandra(df, epoch_id, keyspace, table,batchmode)) \
            .trigger(processingTime=processingTime) \
            .outputMode(mode) \
            .start()

