import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Iteration 3 Assignment ICS 370 3/4/21
 * 
 * MovieDriver class uses the JBconnector to acesss the omdb database and
 * display some information from it into eclipse output
 * 
 * @author Jason Yang
 *
 */

public class MovieDriver {
	
	// Iteration 2
	public static void selectAllMovies() { 

		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";

			con = DriverManager.getConnection(dbURL, username, password);
			Statement myStmt = con.createStatement();
			ResultSet myRs = myStmt.executeQuery("Select * From Movies");
			while (myRs.next()) {
				int id = myRs.getInt(1);
				String native_name = myRs.getString(2);
				String english_name = myRs.getString(3);
				int year = myRs.getInt(4);
				System.out.println("ID: " + id + "\t" + "Native name: " + native_name + "\t" + "English name: "
						+ english_name + "\t" + "Year: " + year);
			}
			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	// Iteration 3
	public static void createMovie(String native_name, String english_name, int year_made) { 

		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);

			String sql = "Insert INTO Movies (native_name, english_name, year_made) VALUES (?, ?, ?)";

			PreparedStatement statement = con.prepareStatement(sql);
			statement.setString(1, native_name);
			statement.setString(2, english_name);
			statement.setInt(3, year_made);

			int rowsInserted = statement.executeUpdate();
			if (rowsInserted > 0) {
				System.out.println("A new movie was inserted successfully.");
			}

			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	// Iteration 3
	public static void readMovie() { 
		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);

			String sql = "SELECT movies.movie_id, movies.native_name, movies.english_name, movies.year_made, movie_data.tag_line, movie_data.language,"
					+ "movie_data.country, movie_data.genre, movie_data.plot FROM movies, movie_data WHERE movies.movie_id = movie_data.movie_id";

			PreparedStatement statement = con.prepareStatement(sql);
			ResultSet myRs = statement.executeQuery(sql);
			while (myRs.next()) {
				int id = myRs.getInt(1);
				String native_name = myRs.getString(2);
				String english_name = myRs.getString(3);
				int year = myRs.getInt(4);
				String tag_line = myRs.getString(5);
				String language = myRs.getString(6);
				String country = myRs.getString(7);
				String genre = myRs.getString(8);
				String plot = myRs.getString(9);

				System.out.println("ID: " + id + "\t" + "Native name: " + native_name + "\t" + "English name: "
						+ english_name + "\t" + "Year: " + year + "\t" + "Tag Line: " + tag_line + "\t" + "Language: "
						+ language + "\t" + "Country: " + country + "\t" + "Genre: " + genre + "\t" + "Plot: " + plot);
			}

			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	//Iteration 3
	public static void updateMovie(int id, String new_Native_Name, String new_English_Name, int new_Year_Made) { 

		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);

			String sql = "UPDATE Movies SET native_name =?, english_name=?, year_made=? WHERE movie_id=?";

			PreparedStatement statement = con.prepareStatement(sql);
			statement.setString(1, new_Native_Name);
			statement.setString(2, new_English_Name);
			statement.setInt(3, new_Year_Made);
			statement.setInt(4, id);

			int rowsUpdated = statement.executeUpdate();
			if (rowsUpdated > 0) {
				System.out.println("A new movie was updated successfully.");
			}

			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	// Iteration 3
	public static void deleteMovie(int id) {

		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);

			String sql = "Delete FROM Movies WHERE movie_id =?";

			PreparedStatement statement = con.prepareStatement(sql);
			statement.setInt(1, id);

			int rowsDeleted = statement.executeUpdate();
			if (rowsDeleted > 0) {
				System.out.println("A new movie was deleted successfully.");
			}

			con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public static boolean processMovieSongs() {
		boolean state = false;
		Connection con = null;

		try {
			String dbURL = "jdbc:mysql://localhost:3306/omdb2";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);

			String movieStatement = "";
			String movieStatement2 = "";
			String movieStatement3 = "";
			
			PreparedStatement stmt7 = con.prepareStatement("Select * From ms_test_data WHERE execution_status =?");
			stmt7.setString(1, "[1] M created [3] S created [5] MS created");
			ResultSet myRs7 = stmt7.executeQuery();
			while (myRs7.next()) {
				String sql5 = "UPDATE ms_test_data SET execution_status=?";

				PreparedStatement statement7 = con.prepareStatement(sql5);
				statement7.setString(1, "[2] M ignored [4] S ignored [6] MS ignored");
				int rowsUpdated = statement7.executeUpdate();
			}

			PreparedStatement stmt6 = con.prepareStatement("SELECT * FROM ms_test_data WHERE execution_status=?");
			stmt6.setString(1, "to be processed");
			ResultSet myRs = stmt6.executeQuery();
			while (myRs.next()) {
				int id = myRs.getInt(1);
				String native_name = myRs.getString(2);
				int year = myRs.getInt(3);
				String title = myRs.getString(4);
				String status = myRs.getString(5);

				PreparedStatement stmt = con.prepareStatement("Select * From Movies WHERE native_name =? AND year_made =?");
				stmt.setString(1, native_name);
				stmt.setInt(2, year);
				ResultSet myRs2 = stmt.executeQuery();

				if (myRs2.next()) {
					System.out.println("M ignored");
				}
				else {
					String sql = "Insert INTO Movies (native_name, english_name, year_made) VALUES (?, ?, ?)";

					PreparedStatement statement = con.prepareStatement(sql);
					statement.setString(1, native_name);
					statement.setString(2, native_name);
					statement.setInt(3, year);
					int rowsInserted = statement.executeUpdate();
					if (rowsInserted > 0) {
						movieStatement = "[1] M created ";
						System.out.println("M created");
					}
				}
				
				PreparedStatement stmt2 = con.prepareStatement("Select * From Songs WHERE title=?");
				stmt2.setString(1, title);
				ResultSet myRs3 = stmt2.executeQuery();
				if (myRs3.next()) {
					movieStatement2 = "[4] S ignored ";
					System.out.println("S ignored");
				} 
				else {
					String sql2 = "Insert INTO Songs (title, lyrics, theme) VALUES (?, ?, ?)";

					PreparedStatement statement2 = con.prepareStatement(sql2);
					statement2.setString(1, title);
					statement2.setString(2, "none");
					statement2.setString(3, "none");
					int rowsInserted2 = statement2.executeUpdate();
					if (rowsInserted2 > 0) {
						movieStatement2 = "[3] S created ";
						System.out.println("S created");
					}
				}

				PreparedStatement stmt3 = con.prepareStatement("Select * From Movies WHERE native_name=?");
				stmt3.setString(1, native_name);
				ResultSet myRs4 = stmt3.executeQuery();
				while (myRs4.next()) {
					int movie_id = myRs4.getInt(1);
					String native_name3 = myRs4.getString(2);
					String english_name = myRs4.getString(3);
					int year3 = myRs4.getInt(4);
					PreparedStatement stmt4 = con.prepareStatement("Select * from Songs WHERE title=?");
					stmt4.setString(1, title);
					ResultSet myRs5 = stmt4.executeQuery();
					while (myRs5.next()) {
						int song_id = myRs5.getInt(1);
						String song_title = myRs5.getString(2);
						String lyrics2 = myRs5.getString(3);
						String theme2 = myRs5.getString(4);
						PreparedStatement stmt5 = con.prepareStatement("Select * From Movie_song WHERE movie_id=? AND song_id=?");
						stmt5.setInt(1, movie_id);
						stmt5.setInt(2, song_id);
						ResultSet myRs6 = stmt5.executeQuery();
						String sql3 = "Insert INTO Movie_song (movie_id, song_id) VALUES (?, ?)";

						PreparedStatement statement3 = con.prepareStatement(sql3);
						statement3.setInt(1, movie_id);
						statement3.setInt(2, song_id);
						int rowsInserted3 = statement3.executeUpdate();
						if (rowsInserted3 > 0) {
							movieStatement3 = "[5] MS created";
							System.out.println("MS created");
						}
						
					}
				}
				
				String sql4 = "UPDATE ms_test_data SET execution_status=?";

				PreparedStatement statement4 = con.prepareStatement(sql4);
				statement4.setString(1, "[1] M created [3] S created [5] MS created");
				int rowsUpdated2 = statement4.executeUpdate();
			}
			
			con.close();
			state = true;
			return state;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
			try {
				if (con != null)
					con.close();
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		}
	}

	public static void main(String args[]) {
		// selectAllMovies();
		// createMovie("Transformers", "Transformers", 2008);
		// updateMovie(20120, "Wall-E", "Wall-E", 2009);
		// deleteMovie(20120);
		// readMovie();
		 processMovieSongs();
	}
}
