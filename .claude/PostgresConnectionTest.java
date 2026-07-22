import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresConnectionTest {

    private static final String URL =
            "jdbc:postgresql://localhost:5432/conductor?currentSchema=conductor";

    private static final String USERNAME = "conductor_user";
    private static final String PASSWORD = "password";
    private static final String DRIVER = "org.postgresql.Driver";

    @Test
    void shouldConnectToDatabase() throws Exception {
        Class.forName(DRIVER);

        try (Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD)) {
            assertTrue(connection.isValid(5));

            try (ResultSet rs = connection.createStatement().executeQuery("""
                    SELECT
                        current_user,
                        session_user,
                        current_schema(),
                        current_setting('search_path')
                    """)) {

                assertTrue(rs.next());

                System.out.println("current_user   = " + rs.getString(1));
                System.out.println("session_user   = " + rs.getString(2));
                System.out.println("current_schema = " + rs.getString(3));
                System.out.println("search_path    = " + rs.getString(4));

                assertEquals("conductor", rs.getString(3));
            }
        }
    }
}