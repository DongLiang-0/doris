CREATE TABLE IF NOT EXISTS set2 (
  `p_partkey` integer NOT NULL default '0'
)ENGINE=OLAP
DISTRIBUTED BY HASH(p_partkey) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
