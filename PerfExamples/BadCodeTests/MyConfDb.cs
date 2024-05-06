namespace BadCodeTests;

public class MyConfDb {
	public static string GetSetting(string key) {
		Thread.Sleep(50);
		return "10";
	}
}