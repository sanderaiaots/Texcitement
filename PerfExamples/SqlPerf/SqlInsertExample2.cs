using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SqlPerf {
	public class SqlInsertExample2 {
		
		public static void InsertTest(List<DbProductWhQty> data) {
			Stopwatch sw = Stopwatch.StartNew();
			using (SqlConnection conn = new SqlConnection(Program.ConnectionString)) {
				conn.Open();
				using (SqlTransaction tran = conn.BeginTransaction()) {
					//start on clean sheet
					DbProductWhQty.Truncate(conn, tran, "ProductWhQty");
					//CASE 1
					sw.Restart();
					int insCount = BatchInsert(conn, tran, data);
					Console.WriteLine($"inserted {insCount} rows in {sw.ElapsedMilliseconds}ms. That makes: {insCount*1000m/sw.ElapsedMilliseconds:0} rows per second.");
					sw.Restart();
					tran.Commit();
					Console.WriteLine($"Commit elapsed {sw.ElapsedMilliseconds}ms");
				}
			}
		}
		
		public static int BatchInsert(SqlConnection conn, SqlTransaction tran, List<DbProductWhQty> data, Action<DbProductWhQty,int> afterInsert = null) {
			string sql = "INSERT INTO ProductWhQty (ProductCode, StockCode, Qty) VALUES(@ProductCode, @StockCode, @Qty)";
			using (SqlCommand cmd = conn.CreateCommand()) {
				cmd.Transaction = tran;
				SqlBatchExecuter.ExecuteBatch(cmd, sql, data, 100);
			}
			return data.Count;
		}
	}
}