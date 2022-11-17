/*

This script has two sections.
First is to gather the data.  
1) Create a global temp table ##BPool and inserts sys.dm_os_buffer_discriptors there.
2) Loop through all databases, and try to join up the object name, index names, partition numbers and compression type.

The second section is the reporting section.
1) Most Granular Show all.
2) If more than one numa node, or more than one partition count then summarize all
3) Aggregated by Table, (not broken down by index)
4) Aggregated by Database
5) Aggregated by Numa Node


*/

/*
checkpoint;
go
dbcc dropcleanbuffers;
go
*/

-----------------
-- Gather Section
-----------------
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET DEADLOCK_PRIORITY LOW;

BEGIN TRY DROP TABLE ##BPool END TRY BEGIN CATCH END CATCH ;
GO

CREATE TABLE ##BPool( database_id			int		  not null
					, database_name			sysname		  null
					, [object_name]			nvarchar(512) null
					, object_id				int			  null
					, [index_id]			smallint	  null
					, index_name			sysname		  null
					, partition_number		int			  null
					, numa_node				tinyint	  not null
					, dirty_pages			bigint	  not null
					, total_pages			bigint    not null
					, bpe_pages				bigint        null
					, allocation_unit_id	bigint    not null
					, page_type				nvarchar(120)  not null
					, percent_dirty			numeric(7,2)   not null
					, buffers_mb			numeric(12,2)  not null
					, row_count				bigint		   not null
					, avg_rows_per_page		numeric(12,1)  not null
					, avg_prcnt_empty		numeric(12,2)  not null
					, avg_free_space_in_bytes numeric(8,1) not null
					, wasted_space_mb		numeric(12,2)  not null
					, [compression]			varchar(20)		null
					, capture_time			datetime	  not null
					, sql_instance			sysname		  not null
					) ;


INSERT INTO ##BPool WITH (TABLOCKX) 
	   (database_id, [database_name] , allocation_unit_id, page_type
	   , numa_node , dirty_pages, total_pages, bpe_pages
	   , percent_dirty, buffers_mb, row_count, avg_rows_per_page
	   , avg_prcnt_empty, avg_free_space_in_bytes, wasted_space_mb
	   , capture_time, sql_instance
	   ) 
SELECT database_id 
	 , CASE 
		 WHEN bd.database_id = 32767 Then 'ResourceDB'
		 ELSE DB_NAME(bd.database_id)
	   END as [database_name]
	 , allocation_unit_id 
	 , page_type
	 , numa_node
	 , SUM(CONVERT(bigint,is_modified)) as dirty_pages
	 , COUNT_BIG(*) as total_pages
	 , SUM(CONVERT(bigint, bd.is_in_bpool_extension)) as bpe_pages
	 , CONVERT(numeric(7,2) ,  100. * SUM(CONVERT(bigint,is_modified)) / COUNT_BIG(*) ) as percent_dirty
	 , COUNT_BIG(*) / 128. as [buffers_mb]
	 , SUM(CONVERT(bigint, row_count)) as row_count
	 , CONVERT(numeric(7,1) , SUM(CONVERT(bigint, 1.0 * row_count)) /  (COUNT_BIG(*) * 1.0))  as avg_rows_per_page
	 , AVG(CONVERT(numeric(10,1), bd.free_space_in_bytes) / 80.6 ) as avg_prcnt_empty  -- / 8060 (byte row limit) * 100 (for percent)
	 , AVG(CONVERT(numeric(10,1), bd.free_space_in_bytes) ) as avg_free_space_in_bytes
	 , SUM(CONVERT(bigint, bd.free_space_in_bytes) ) / 1024. / 1024. as wasted_space_mb
	 , GETDATE()
	 , @@SERVERNAME
FROM sys.dm_os_buffer_descriptors bd WITH (nolock)
GROUP BY  database_id
		, allocation_unit_id
		, page_type
	    , numa_node
  ;
GO


DECLARE @szSQL nvarchar(MAX);

SELECT @szSQL = N'	USE [?]; 

					UPDATE  ##BPool
					SET   [object_name]    = COALESCE(SCHEMA_NAME(so.schema_id) + ''.'', '''')  + COALESCE(object_name(si.object_id), bp.page_type)
						, [object_id]	   = p.object_id
						, index_name = CASE 
										WHEN si.object_id IS NOT NULL THEN COALESCE(si.name, si.type_desc) 
										ELSE NULL
									   END 
						, index_id = p.index_id
						, partition_number = p.partition_number
						, [compression]	   = LEFT(p.data_compression_desc, 20)
					FROM ##BPool bp 
						   LEFT JOIN
						   sys.allocation_units au WITH (nolock)	ON bp.allocation_unit_id = au.allocation_unit_id
						   LEFT JOIN
						   sys.partitions p WITH (nolock)			ON au.container_id = p.hobt_id
						   LEFT JOIN
						   sys.indexes si WITH (nolock)			ON p.object_id = si.object_id
															   AND p.index_id  = si.index_id
						   LEFT JOIN
						   [?].sys.objects so WITH (nolock)		ON si.object_id = so.object_id
					WHERE bp.database_id = db_id(''?'')';
exec sp_MSforeachdb @szSQL;


-----------------------------------------
--- REPORT SECTION:
--  Index Summary, biggest hogs first.
-----------------------------------------
SELECT bp.database_name, bp.object_name , bp.object_id
	 , bp.index_name, bp.partition_number, bp.numa_node
	 , bp.percent_dirty
	 , bp.buffers_mb
	 , bp.bpe_pages
	 , bp.row_count
	 , bp.avg_rows_per_page
	 , bp.avg_prcnt_empty
	 , bp.avg_free_space_in_bytes
	 , bp.wasted_space_mb
	 , bp.[compression]
	 , bp.capture_time, bp.sql_instance
FROM ##BPool bp
WHERE bp.object_id IS NOT NULL
ORDER BY buffers_mb desc

--If more than one numa node, or more than one partition count then summarize all
IF EXISTS (SELECT 1 FROM ##BPool bp WHERE numa_node > 0  OR partition_number > 1) 
BEGIN 
	SELECT bp.database_name, bp.object_name
		 , index_name, index_id
		 , COUNT(DISTINCT partition_number) as [partitions]
		 , SUM(bp.buffers_mb) as buffers_mb
		 , AVG(bp.avg_prcnt_empty) as avg_prcnt_empty
		 --, bp.avg_free_space_in_bytes
		 , SUM(bp.wasted_space_mb) as wasted_space_mb
		 , [compression]
		 , capture_time, sql_instance
	FROM ##BPool bp
	WHERE bp.object_id IS NOT NULL
	GROUP BY bp.database_name, bp.object_name, index_name, index_id, [compression], capture_time, sql_instance
	ORDER BY SUM(bp.buffers_mb) desc;
END

--Aggregated by Table, (not broken down by index)
SELECT bp.database_name, bp.object_name
		 , COUNT(DISTINCT partition_number) as [partitions]
		 , SUM(bp.buffers_mb) as buffers_mb
		 , AVG(bp.avg_prcnt_empty) as avg_prcnt_empty
		 , SUM(bp.wasted_space_mb) as wasted_space_mb
		 , capture_time, sql_instance
FROM ##BPool bp
WHERE bp.object_id IS NOT NULL
GROUP BY bp.database_name, bp.object_name, capture_time, sql_instance
ORDER BY SUM(bp.buffers_mb) desc;

-- Aggregated by Database
;WITH cte_BPool as (
			SELECT database_name
					, SUM(bp.buffers_mb) as total_buffer_mb
					, SUM(bp.buffers_mb) / (SELECT SUM(bp.buffers_mb) FROM ##BPool bp) * 100. as percent_bpool
					, SUM(bp.wasted_space_mb) as total_wasted_space_mb
					, CONVERT(numeric(7,4), SUM(dirty_pages) * 1.0 / SUM(total_pages) ) * 100. as percent_dirty
			FROM ##BPool bp
			WHERE bp.object_id IS NOT NULL
			GROUP BY database_name
			
			UNION

			--Total SUMMARY
			SELECT '***TOTAL***' as 'database_name'
					, SUM(bp.buffers_mb) as total_buffer_mb
					, 100.00 as percent_bpool
					, SUM(bp.wasted_space_mb) as total_wasted_space_MB
					, CONVERT(numeric(7,4), SUM(dirty_pages) * 1.0 / SUM(total_pages) ) * 100. as percent_dirty
			FROM ##BPool bp
			WHERE bp.object_id IS NOT NULL
)
SELECT *
FROM cte_BPool
ORDER BY total_buffer_mb DESC;


---------------------------------
-- Aggregated by numa node 
---------------------------------
;WITH numa_BPool as 
(
	SELECT	 CONVERT(varchar(25) , numa_node) as numa_node
		   , SUM(bp.buffers_mb) as total_bpool_mb
		   , SUM(bp.buffers_mb) / (SELECT SUM(buffers_mb) FROM ##BPool) * 100.	as [total_bpool_%]
		   , SUM(wasted_space_mb) as total_wasted_space_mb
		   , CONVERT(numeric(7,4), SUM(dirty_pages) * 1.0 / SUM(total_pages) ) * 100. as percent_dirty
	FROM ##BPool bp
	GROUP BY numa_node
	
	UNION 

	SELECT '***TOTAL***' as numa_node
			, SUM(bp.buffers_mb) as total_bpool_mb
			, 100.00 as  [total_bpool_%]
			, SUM(bp.wasted_space_mb) as total_wasted_space_mb
			, CONVERT(numeric(7,4), SUM(dirty_pages) * 1.0 / SUM(total_pages) ) * 100. as percent_dirty
	FROM ##BPool bp
	
)
SELECT *
FROM numa_BPool
ORDER BY total_bpool_mb DESC

