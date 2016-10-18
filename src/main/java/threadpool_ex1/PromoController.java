package threadpool_ex1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PromoController {
	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/pricing";
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "";

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();
	private static final ExecutorService execServ = new ThreadPoolExecutor(
			2, // pool
			10, // max thread pool
			30, // timeout in seconds
			TimeUnit.SECONDS, 
			new ArrayBlockingQueue<Runnable>(200), // queue 20 requests 
			Executors.defaultThreadFactory(), 
			new ThreadPoolExecutor.CallerRunsPolicy());

	private static class SomeIOWork implements Callable {
		public Object call() throws Exception {
			Thread.sleep(100);
			System.out.println("######### Using thread " + Thread.currentThread().getName() + "#########");
			String userid = insertRecordIntoTable();
			return userid;
		}
	}
	
	@RequestMapping("/Promo")
	public Promo greeting(@RequestParam(value="name", defaultValue="CitiPromo") String name) {
		Future futurePromoTask = execServ.submit(new SomeIOWork());
		while (!futurePromoTask.isDone()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		String userid = "";
		try {
			userid = (String) futurePromoTask.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Promo(counter.incrementAndGet(), String.format(template, userid));			
	}

	private static Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}
		try {
			dbConnection = DriverManager.getConnection(
					DB_CONNECTION, DB_USER,DB_PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return dbConnection;
	}

	private static String insertRecordIntoTable() throws SQLException {
		String userid = "";
		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		String insertTableSQL = "INSERT INTO Promo"
				+ "(userid, promocode) VALUES"
				+ "(?,?)";
		try {
			dbConnection = getDBConnection();
			preparedStatement = dbConnection.prepareStatement(insertTableSQL);
			userid="k.sharma" + randomWithRange(1,10000);
			preparedStatement.setString(1, userid);
			preparedStatement.setString(2, "citi20diwali");
			preparedStatement.executeUpdate();
			System.out.println("Record is inserted into Promo table!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}
		}
		return userid;
	}		

	private static int randomWithRange(int min, int max)
	{
		int range = (max - min) + 1;     
		return (int)(Math.random() * range) + min;
	}
}