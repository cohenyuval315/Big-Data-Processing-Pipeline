import dataclasses

@dataclasses.dataclass
class KafkaMetaData:
    offset:int
    partition:int


class CassandraKafka:
    def cql(self,metadata:KafkaMetaData):
        s = rf"""
            INSERT INTO ${CassandraDriver.namespace}.${CassandraDriver.kafkaMetadata} (partition, offset)
            VALUES(${metadata.partition}, ${metadata.offset})
            """        
        return s