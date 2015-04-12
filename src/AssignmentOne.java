import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class AssignmentOne {

	public static final int NUMBER_OF_ROWS = 5000000;
	public static final int PRINT_INTERVAL = 100000;
	public static int[] seedArrayOne = new int[10];
	public static int[] seedArrayTwo = new int[10];
	

	public static void main(String[] args) throws ClassNotFoundException, SQLException,Exception{
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/dev?user=raga&password=pass");
		initializeSeedArrays(); //should always be there
		long[][] resultInMilliseconds = new long[4][];
		resultInMilliseconds[0] = physicalOrganizationTypeOne(conn);
		resultInMilliseconds[1] = physicalOrganizationTypeTwo(conn);
		resultInMilliseconds[2] = physicalOrganizationTypeThree(conn);
		resultInMilliseconds[3] = physicalOrganizationTypeFour(conn);
		
		//Print results
		System.out.println("--------- RESULTS ------------");
		System.out.println();
		for(int i=0; i< resultInMilliseconds.length; i++){
			for(int j=0; j<resultInMilliseconds[i].length;j++){
				if(j == 7) System.out.print(resultInMilliseconds[i][j]);
				else System.out.print(resultInMilliseconds[i][j] + " | ");
			}
			System.out.println();
		}
	}

	public static long[] physicalOrganizationTypeOne(Connection conn) throws SQLException{
		System.out.println("Physical Organization Type 1");
		createTablePhysicalOrgTypeOne(conn);
		grantTablePrivileges(conn);
		long[] timesInSeconds = new long[8];
		truncateTable(conn);
		System.out.println("-------- VARIATION ONE -------");
		timesInSeconds[0] = loadVariationOneSortedOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[2] = queryOne(conn);
		System.out.println();
		timesInSeconds[4] = queryTwo(conn);
		System.out.println();
		timesInSeconds[6] = queryThree(conn);
		System.out.println("-----------------------------");
		truncateTable(conn);
		System.out.println("-------- VARIATION TWO -------");
		timesInSeconds[1] = loadVariationTwoInRandomOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[3] = queryOne(conn);
		System.out.println();
		timesInSeconds[5] = queryTwo(conn);
		System.out.println();
		timesInSeconds[7] = queryThree(conn);
		System.out.println("-----------------------------");
		dropTable(conn);
		return timesInSeconds;
	}
	
	
	public static long[] physicalOrganizationTypeTwo(Connection conn) throws SQLException{
		System.out.println("Physical Organization Type 2");
		createTablePhysicalOrgTypeOne(conn);
		createColumnAIndex(conn);
		grantTablePrivileges(conn);
		long[] timesInSeconds = new long[8];
		truncateTable(conn);
		System.out.println("-------- VARIATION ONE -------");
		timesInSeconds[0] = loadVariationOneSortedOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[2] = queryOne(conn);
		System.out.println();
		timesInSeconds[4] = queryTwo(conn);
		System.out.println();
		timesInSeconds[6] = queryThree(conn);
		System.out.println("-----------------------------");
		truncateTable(conn);
		System.out.println("-------- VARIATION TWO -------");
		timesInSeconds[1] = loadVariationTwoInRandomOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[3] = queryOne(conn);
		System.out.println();
		timesInSeconds[5] = queryTwo(conn);
		System.out.println();
		timesInSeconds[7] = queryThree(conn);
		System.out.println("-----------------------------");
		dropTable(conn);
		return timesInSeconds;
	}
	
	public static long[] physicalOrganizationTypeThree(Connection conn) throws SQLException{
		System.out.println("Physical Organization Type 3");
		createTablePhysicalOrgTypeOne(conn);
		createColumnBIndex(conn);
		grantTablePrivileges(conn);
		long[] timesInSeconds = new long[8];
		truncateTable(conn);
		System.out.println("-------- VARIATION ONE -------");
		timesInSeconds[0] = loadVariationOneSortedOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[2] = queryOne(conn);
		System.out.println();
		timesInSeconds[4] = queryTwo(conn);
		System.out.println();
		timesInSeconds[6] = queryThree(conn);
		System.out.println("-----------------------------");
		truncateTable(conn);
		System.out.println("-------- VARIATION TWO -------");
		timesInSeconds[1] = loadVariationTwoInRandomOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[3] = queryOne(conn);
		System.out.println();
		timesInSeconds[5] = queryTwo(conn);
		System.out.println();
		timesInSeconds[7] = queryThree(conn);
		System.out.println("-----------------------------");
		dropTable(conn);
		return timesInSeconds;
	}
	
	public static long[] physicalOrganizationTypeFour(Connection conn) throws SQLException{
		System.out.println("Physical Organization Type 4");
		createTablePhysicalOrgTypeOne(conn);
		createColumnAIndex(conn);
		createColumnBIndex(conn);
		grantTablePrivileges(conn);
		long[] timesInSeconds = new long[8];
		truncateTable(conn);
		System.out.println("-------- VARIATION ONE -------");
		timesInSeconds[0] = loadVariationOneSortedOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[2] = queryOne(conn);
		System.out.println();
		timesInSeconds[4] = queryTwo(conn);
		System.out.println();
		timesInSeconds[6] = queryThree(conn);
		System.out.println("-----------------------------");
		truncateTable(conn);
		System.out.println("-------- VARIATION TWO -------");
		timesInSeconds[1] = loadVariationTwoInRandomOrder(conn,NUMBER_OF_ROWS);
		System.out.println();
		timesInSeconds[3] = queryOne(conn);
		System.out.println();
		timesInSeconds[5] = queryTwo(conn);
		System.out.println();
		timesInSeconds[7] = queryThree(conn);
		System.out.println("-----------------------------");
		dropTable(conn);
		return timesInSeconds;
	}
	
	public static long queryTwo(Connection conn) throws SQLException{
		System.out.println("RUNNING QUERY TWO");
		long start = System.currentTimeMillis();
		for(int i=0;i<seedArrayTwo.length;i++){
			System.out.println("Running query two with columnb as " + seedArrayTwo[i]);
			String query = "select * from benchmark where benchmark.columnb=?;";
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, seedArrayTwo[i]);
			statement.execute();
			statement.close();
		}
		long end = System.currentTimeMillis();
		System.out.println("FINISHED QUERY TWO -> " + (end-start)/seedArrayTwo.length);
		return ((end-start)/(seedArrayOne.length));
	}

	public static long queryOne(Connection conn) throws SQLException{
		System.out.println("RUNNING QUERY ONE");
		long start = System.currentTimeMillis();
		for(int i=0;i<seedArrayOne.length;i++){
			System.out.println("Running query one with columna as " + seedArrayOne[i]);
			String query = "select * from benchmark where benchmark.columna=?;";
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, seedArrayOne[i]);
			statement.execute();
			statement.close();
		}
		long end = System.currentTimeMillis();
		System.out.println("FINISHED QUERY ONE -> " + (end-start)/seedArrayOne.length);
		return ((end-start)/(seedArrayOne.length));
	}
	
	public static long queryThree(Connection conn) throws SQLException{
		System.out.println("RUNNING QUERY THREE");
		long start = System.currentTimeMillis();
		for(int i=0;i<seedArrayOne.length;i++){
			System.out.println("Running query three with columna as " + seedArrayOne[i] + " and columnb as " + seedArrayTwo[i]);
			String query = "select * from benchmark where benchmark.columna=? and benchmark.columnb=?;";
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, seedArrayOne[i]);
			statement.setInt(2, seedArrayTwo[i]);
			statement.execute();
			statement.close();
		}
		long end = System.currentTimeMillis();
		System.out.println("FINISHED QUERY THREE -> " + (end-start)/seedArrayOne.length);
		return ((end-start)/(seedArrayOne.length));
	}
	
	
	public static long loadVariationTwoInRandomOrder(Connection conn,int num) throws SQLException{
		System.out.println("GENERATING RANDOM ORDER FOR " + num + " ROWS.");
		long start = System.currentTimeMillis();
		Random random = new Random();
		Set<Integer> keySet = new HashSet<Integer>();
		RandomString randomString = new RandomString(247);
		int min = 1;
		int max = 50000;

		for(int i=1; i<=num;i++){
			if((i % PRINT_INTERVAL) == 0) System.out.println("Finished loading " + i + "/" + num + " rows.");
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
		System.out.println("FINISHED RANDOM ORDER time taken in millis -> " + (end-start));
		return end-start;
	}


	public static long loadVariationOneSortedOrder(Connection conn,int num) throws SQLException{
		System.out.println("GENERATING SORTED ORDER FOR " + num + " ROWS.");
		long start = System.currentTimeMillis();
		Random random = new Random();
		RandomString randomString = new RandomString(247);
		int min = 1;
		int max = 50000;

		for(int i=1; i<=num;i++){
			if((i % PRINT_INTERVAL) == 0) System.out.println("Finished loading " + i + "/" + num + " rows.");
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
		System.out.println("FINISHED SORTED ORDER time taken in millis -> " + (end-start));
		return end-start;
	}
	
	public static void initializeSeedArrays(){
		System.out.println("Initializing seed arrays.");
		Random random = new Random();
		int min = 1;
		int max = 50000;
		for(int i=1; i<seedArrayOne.length;i++){
			seedArrayOne[i] = random.nextInt(max - min) + min;
		}
		for(int i=1; i<seedArrayTwo.length;i++){
			seedArrayTwo[i] = random.nextInt(max - min) + min;
		}
	}

	//
	
	public static void createTablePhysicalOrgTypeOne(Connection conn) throws SQLException{
		String query = "create table benchmark ( thekey integer primary key,columna integer,columnb integer,filler varchar(247) );";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Created table with physical organization type I");
	}
	
	public static void createColumnAIndex(Connection conn) throws SQLException{
		String query = "create index columnaindex on benchmark(columna);";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Created Index on column A");
	}
	
	public static void createColumnBIndex(Connection conn) throws SQLException{
		String query = "create index columnbindex on benchmark(columnb);";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Created Index on column B");
	}
	
	public static void grantTablePrivileges(Connection conn) throws SQLException{
		String query = "grant all privileges on table benchmark to raga;";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Granted table privileges.");
	}
	
	public static void truncateTable(Connection conn) throws SQLException{
		String query = "truncate benchmark;";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Truncated table.");
	}
	
	public static void dropTable(Connection conn) throws SQLException{
		String query = "drop table benchmark;";
		PreparedStatement statement = conn.prepareStatement(query);
		statement.executeUpdate();
		statement.close();
		System.out.println("Dropped table.");
	}

	public static int getUniqueRandomNumber(Set<Integer> keySet,Random random){
		int min = 0;
		int max = Integer.MAX_VALUE;
		int key = random.nextInt(max - min) + min;
		while(keySet.contains(key)) key = random.nextInt(max - min) + min;
		keySet.add(key);
		return key;
	}


}
