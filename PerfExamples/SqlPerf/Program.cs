using System.Diagnostics;

namespace SqlPerf {
	internal class Program {
		public static string ConnectionString = "Server=server1;Database=TechNation2019.1;User=perf;password=perf;";
		
		public static void Main(string[] args) {
			Stopwatch sw = Stopwatch.StartNew();
			List<DbProductWhQty> data = DbProductWhQty.GenerateData(1000, 10);
			Console.WriteLine($"Generated {data.Count} rows of test data in {sw.ElapsedMilliseconds}ms");
			for (int i = 0; i < 1; i++) {
				Console.WriteLine("----------------------------------------------");
				Console.WriteLine("  CASE 1");
				Console.WriteLine("----------------------------------------------");
				SqlInsertExample1.InsertTest(data);
				Console.WriteLine("----------------------------------------------");
				Console.WriteLine("  CASE 2");
				Console.WriteLine("----------------------------------------------");
				SqlInsertExample2.InsertTest(data);
				Console.WriteLine("----------------------------------------------");
				Console.WriteLine("  CASE 3");
				Console.WriteLine("----------------------------------------------");
				new SqlInsertExample3() .InsertTest(data);
			
			
				sw.Restart();
				/*
				Task[] tasks = {
					new Task(() => { new SqlInsertExample3() {TableName = "ProductWhQtyC1"}.InsertTest(data);}),
					new Task(() => { new SqlInsertExample3() {TableName = "ProductWhQtyC2"}.InsertTest(data);}),
					new Task(() => { new SqlInsertExample3() {TableName = "ProductWhQtyC3"}.InsertTest(data);}),
					new Task(() => { new SqlInsertExample3() {TableName = "ProductWhQtyC4"}.InsertTest(data);}),
				};
				foreach (Task task in tasks) {
					task.Start();
				}

				Task.WaitAll(tasks);
				Console.WriteLine($"inserted {data.Count * tasks.Length} rows in {sw.ElapsedMilliseconds}ms. That makes: {(data.Count * tasks.Length)*1000m/sw.ElapsedMilliseconds:0} rows per second.");
				*/
			}
		}

		
	}
}