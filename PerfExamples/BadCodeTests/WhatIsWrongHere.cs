using System.Diagnostics;

namespace BadCodeTests;

public class Tests {
	[SetUp]
	public void Setup() {
	}

	[Test]
	public void Test1() {
		Stopwatch sw = Stopwatch.StartNew();
		MyPlanCalculator cal = new MyPlanCalculator();
		List<MyWorkPlan> plan = cal.CaculatePlan();
		Console.WriteLine($"Loaded {plan.Count} items in {sw.ElapsedMilliseconds}");
		Assert.Less(sw.ElapsedMilliseconds, 200);
	}
}

public class MyWorkPlan {
	public DateTime Day;
	public int WorkToDo;
}

public class MyPlanCalculator {
	private static Random random = new Random();

	public int GetDaysAhead() {
		return int.Parse(MyConfDb.GetSetting("DaysAhead"));
	}

	public List<MyWorkPlan> CaculatePlan() {
		List<MyWorkPlan> plan = new List<MyWorkPlan>();
		for (int i = 0; i < GetDaysAhead(); i++) {
			MyWorkPlan item = new MyWorkPlan() { Day = DateTime.Today.AddDays(i), WorkToDo = FindWork() };
			plan.Add(item);
		}

		return plan;
	}
	
	private int FindWork() {
		return random.Next(0, 100);
	}
}