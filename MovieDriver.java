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
			String dbURL = "jdbc:mysql://localhost:3306/omdb";
			String username = "root";
			String password = "1234";
			con = DriverManager.getConnection(dbURL, username, password);
			
			String movieStatement = "";
				
			Statement myStmt2 = con.createStatement();
			ResultSet myRs2 = myStmt2.executeQuery("Select * From ms_test_data");
			while (myRs2.next()) {
				int id2 = myRs2.getInt(1);
				String native_name2 = myRs2.getString(2);
				int year2 = myRs2.getInt(3);
				String title = myRs2.getString(4);
				String status = myRs2.getString(5);
				Statement myStmt = con.createStatement();
				ResultSet myRs = myStmt.executeQuery("Select * From Movies");
				while (myRs.next()) {
					int id = myRs.getInt(1);
					String native_name = myRs.getString(2);
					String english_name = myRs.getString(3);
					int year = myRs.getInt(4);
					if (native_name.equals(native_name2) && year == year2) {
						movieStatement += "[2] M ignored ";
					}
					else {
						String sql = "Insert INTO Movies (native_name, english_name, year_made) VALUES (?, ?, ?)";

						PreparedStatement statement = con.prepareStatement(sql);
						statement.setString(1, native_name2);
						statement.setString(2, native_name2);
						statement.setInt(3, year2);
						movieStatement += "[1] M created ";
					}
				}
			}
			
			Statement myStmt7 = con.createStatement();
			ResultSet myRs7 = myStmt7.executeQuery("Select * From ms_test_data");
			while (myRs7.next()) {
				int id2 = myRs7.getInt(1);
				String native_name2 = myRs7.getString(2);
				int year2 = myRs7.getInt(3);
				String title = myRs7.getString(4);
				String status = myRs7.getString(5);
				Statement myStmt3 = con.createStatement();
				ResultSet myRs3 = myStmt3.executeQuery("Select * From Songs");
				while (myRs3.next()) {
					int song_id = myRs3.getInt(1);
					String song_Title = myRs3.getString(2);
					String lyrics = myRs3.getString(3);
					String theme = myRs3.getString(4);
					if (song_Title.equals(title)) {
						movieStatement += "[4] S ignored ";
					}
					else {
						String sql = "Insert INTO Songs (title) VALUES (?)";

						PreparedStatement statement = con.prepareStatement(sql);
						statement.setString(1, title);
						movieStatement += "[3] S created ";
					}
				}
			}
			
			Statement myStmt8 = con.createStatement();
			ResultSet myRs8 = myStmt8.executeQuery("Select * From ms_test_data");
			while (myRs8.next()) {
				int id2 = myRs8.getInt(1);
				String native_name2 = myRs8.getString(2);
				int year2 = myRs8.getInt(3);
				String title = myRs8.getString(4);
				String status = myRs8.getString(5);
				Statement myStmt4 = con.createStatement();
				ResultSet myRs4 = myStmt4.executeQuery("Select * From Movie_song");
				while (myRs4.next()) {
					int movie_id = myRs4.getInt(1);
					int song_id = myRs4.getInt(2);
					Statement myStmt5 = con.createStatement();
					ResultSet myRs5 = myStmt5.executeQuery("Select * From Movies");
					while (myRs5.next()) {
						int movie_id2 = myRs5.getInt(1);
						String native_name = myRs5.getString(2);
						String english_name = myRs5.getString(3);
						int year = myRs5.getInt(4);
						Statement myStmt6 = con.createStatement();
						ResultSet myRs6 = myStmt6.executeQuery("Select * from Songs");
						while (myRs6.next()) {
							int song_id2 = myRs6.getInt(1);
							String title2 = myRs6.getString(2);
							String lyrics = myRs6.getString(3);
							String theme = myRs6.getString(4);
							if (movie_id == movie_id2 && song_id == song_id2) {
								movieStatement += "[6] MS ignored";
							}
							else {
								String sql = "Insert INTO Movie_song (movie_id, song_id) VALUES (?, ?)";
		
								PreparedStatement statement = con.prepareStatement(sql);
								statement.setInt(1, movie_id2);
								statement.setInt(2, song_id2);
								movieStatement += "[5] MS created";
							}
						}
					}
				}
			}
			
			Statement myStmt9 = con.createStatement();
			ResultSet myRs9 = myStmt9.executeQuery("Select * From ms_test_data");
			while (myRs9.next()) {
				
				String sql = "UPDATE ms_test_data SET execution_status=?";

				PreparedStatement statement = con.prepareStatement(sql);
				statement.setString(1, movieStatement);
				
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