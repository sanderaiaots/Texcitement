using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SqlPerf {
	public class DbProductWhQty {
		public string ProductCode { get; set; }
		public string StockCode { get; set; }
		public int Qty { get; set; }

		public static List<DbProductWhQty> GenerateData(int prdCount, int stockCount) {
			List<string> prdCodes = Enumerable.Repeat("a", prdCount).Select(s => Guid.NewGuid().ToString()).ToList();
			List<string> stockCodes = Enumerable.Repeat("a", stockCount).Select(s => Guid.NewGuid().ToString()).ToList();
			List<DbProductWhQty> genData = new List<DbProductWhQty>(prdCount * stockCount);
			Random rnd = new Random((int) DateTime.Now.Ticks);
			foreach (string stockCode in stockCodes) {
				foreach (string prdCode in prdCodes) {
					DbProductWhQty row = new DbProductWhQty() { StockCode = stockCode, ProductCode = prdCode, Qty = rnd.Next(0,1000)};
					genData.Add(row);
				}
			}

			return genData;
		}

		public static int Insert(SqlConnection conn, SqlTransaction tran, DbProductWhQty row) {
			using (SqlCommand command = conn.CreateCommand()) {
				command.Transaction = tran;
				command.CommandText = "INSERT INTO ProductWhQty (ProductCode, StockCode, Qty) VALUES(@ProductCode, @StockCode, @Qty)";
				command.Parameters.AddWithValue("ProductCode", row.ProductCode);
				command.Parameters.AddWithValue("StockCode", row.StockCode);
				command.Parameters.AddWithValue("Qty", row.Qty);
				return command.ExecuteNonQuery();
			}
		}

		public static int Truncate(SqlConnection conn, SqlTransaction tran, string tableName) {
			using (SqlCommand command = conn.CreateCommand()) {
				command.Transaction = tran;
				command.CommandText = "TRUNCATE TABLE " + tableName;
				return command.ExecuteNonQuery();
			}
		}
	}
}