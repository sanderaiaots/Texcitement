using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SqlPerf {
	public class SqlInsertExample3 {
		public string TableName = "ProductWhQty"; 
		public void InsertTest(List<DbProductWhQty> data) {
			Stopwatch sw = Stopwatch.StartNew();
			using (SqlConnection conn = new SqlConnection(Program.ConnectionString)) {
				conn.Open();
				using (SqlTransaction tran = conn.BeginTransaction()) {
					//start on clean sheet
					DbProductWhQty.Truncate(conn, tran, TableName);
					Console.WriteLine($"{TableName}> cleanup done, start test");
					//CASE 1
					sw.Restart();
					int insCount = BulkInsert(conn, tran, data);
					Console.WriteLine($"{TableName}> inserted {insCount} rows in {sw.ElapsedMilliseconds}ms. That makes: {insCount*1000m/sw.ElapsedMilliseconds:0} rows per second.");
					sw.Restart();
					tran.Commit();
					Console.WriteLine($"{TableName}> Commit elapsed {sw.ElapsedMilliseconds}ms");
				}
			}
		}
		
		public int BulkInsert(SqlConnection conn, SqlTransaction tran, List<DbProductWhQty> data, Action<DbProductWhQty,int> afterInsert = null) {
			using (SqlBulkCopy bulkInsert = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)) {
				bulkInsert.BulkCopyTimeout = 900;
				using (ObjectDataReader<DbProductWhQty> dataReader = new ObjectDataReader<DbProductWhQty>(data)) {
					bulkInsert.DestinationTableName = TableName;
					/*
					bulkInsert.SqlRowsCopied += (sender, args) => {
						rowsCopied += (int)args.RowsCopied;
						Log(2, "Loaded " + args.RowsCopied + " rows, elapsed " + sw.ElapsedMilliseconds + "ms");
					};
					bulkInsert.NotifyAfter = 1000;
					*/
					bulkInsert.WriteToServer(dataReader);
					//log.Log(0, "Network data bulk insert into #HwTempData table. elapsed " + sw.ElapsedMilliseconds + "ms");
					//sw.Restart();
				}
			}
			return data.Count;
		}
	}
}