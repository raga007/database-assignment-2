import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class AssignmentOne {


	public static void main(String[] args) throws ClassNotFoundException, SQLException,Exception{
		Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
		Connection con = DriverManager.getConnection("jdbc:cassandra://localhost:9160/dev?consistency=QUORUM");
		truncate(con);
		generateInRandomOrder(con,500);
	}

	public static int getUniqueRandomNumber(Set<Integer> keySet,Random random){
		int min = 0;
		int max = Integer.MAX_VALUE;
		int key = random.nextInt(max - min) + min;
		while(keySet.contains(key)) key = random.nextInt(max - min) + min;
		return key;
	}

	public static void generateInRandomOrder(Connection conn,int num) throws SQLException{
		long start = System.currentTimeMillis();
		Random random = new Random();
		Set<Integer> keySet = new HashSet<Integer>();
		RandomString randomString = new RandomString(247);
		int min = 1;
		int max = 50000;

		for(int i=1; i<=num;i++){
			String query = "insert into benchmark (thekey,columna,columnb,filler) values (?,?,?,?);";
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, getUniqueRandomNumber(keySet,random));
			statement.setInt(2, random.nextInt(max - min) + min);
			statement.setInt(3, random.nextInt(max - min) + min);
			statement.setString(4, randomString.nextString());
			statement.executeUpdate();
			statement.close();
		}

		long end = System.currentTimeMillis();
		System.out.println("Random order time in millis -> " + (end-start));
	}


	public static void generateInSortedOrder(Connection conn,int num) throws SQLException{
		long start = System.currentTimeMillis();
		Random random = new Random();
		RandomString randomString = new RandomString(247);
		int min = 1;
		int max = 50000;

		for(int i=1; i<=num;i++){
			String query = "insert into benchmark (thekey,columna,columnb,filler) values (?,?,?,?);";
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, i);
			statement.setInt(2, random.nextInt(max - min) + min);
			statement.setInt(3, random.nextInt(max - min) + min);
			statement.setString(4, randomString.nextString());
			statement.executeUpdate();
			statement.close();
		}
		long end = System.currentTimeMillis();
		System.out.println("Sorted order time in millis -> " + (end-start));
	}

	public static void truncate(Connection conn) throws SQLException{
		String query = "truncate benchmark;";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
	}



}
