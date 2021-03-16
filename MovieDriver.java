import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class MovieDriver2 {

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

			Statement myStmt = con.createStatement();
			ResultSet myRs = myStmt.executeQuery("Select * From ms_test_data");
			while (myRs.next()) {
				int id = myRs.getInt(1);
				String native_name = myRs.getString(2);
				int year = myRs.getInt(3);
				String title = myRs.getString(4);
				String status = myRs.getString(5);

				PreparedStatement stmt = con
						.prepareStatement("Select * From Movies WHERE native_name =? AND year_made =?");
				stmt.setString(1, native_name);
				stmt.setInt(2, year);
				ResultSet myRs2 = stmt.executeQuery();

				if (myRs2.next()) {
					movieStatement = "[2] M ignored ";
					System.out.println("M ignored");
				} else {
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
				} else {
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
						PreparedStatement stmt5 = con
								.prepareStatement("Select * From Movie_song WHERE movie_id=? AND song_id=?");
						stmt5.setInt(1, movie_id);
						stmt5.setInt(2, song_id);
						ResultSet myRs6 = stmt5.executeQuery();
						if (myRs6.next()) {
							movieStatement3 = "[6] MS ignored";
							System.out.println("MS ignored");
						} else {
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
				}
				String sql4 = "UPDATE ms_test_data SET execution_status=?";

				PreparedStatement statement4 = con.prepareStatement(sql4);
				statement4.setString(1, movieStatement + movieStatement2 + movieStatement3);
				int rowsUpdated = statement4.executeUpdate();
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

	public static void main(String[] args) {
		processMovieSongs();
	}
}
