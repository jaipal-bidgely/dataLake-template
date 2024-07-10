-> query: EXPLAIN ANALYZE SELECT * FROM hudi.hudi_sample_dedup;
Queued: 628.40us, Analysis: 16.85ms, Planning: 153.19ms, Execution: 10.72s

	On primary key :
 -> query: EXPLAIN ANALYZE SELECT * FROM hudi.hudi_sample_dedup WHERE uuid='a400f674-91d0-423f-873f-b632f5c093e8';
Queued: 184.16us, Analysis: 9.45ms, Planning: 126.05ms, Execution: 2.02s

 	On precombine key :
 -> query: EXPLAIN ANALYZE SELECT * FROM hudi.hudi_sample_dedup WHERE last_updated_timestamp=1665560838050;
Queued: 270.88us, Analysis: 10.36ms, Planning: 101.89ms, Execution: 1.41s

	On partition key :
 -> query: EXPLAIN ANALYZE SELECT * FROM hudi.hudi_sample_dedup WHERE partitionpath='2022-10';
Queued: 426.19us, Analysis: 8.19ms, Planning: 111.26ms, Execution: 1.72s

 	On event_id (no key):
 -> query: EXPLAIN ANALYZE SELECT * FROM hudi.hudi_sample_dedup WHERE event_id='6f185a69-fdc9-40d2-b171-7f9a8d279531';
Queued: 392.90us, Analysis: 7.69ms, Planning: 86.01ms, Execution: 1.34s