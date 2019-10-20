using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace DatabaseController
{
    public class MySQLClient
    {
        public MySqlConnection connection { get; set; }
        private string connectionString { get; set; }


        public void init(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public bool isConnected()
        {
            return ((this.connection.State == ConnectionState.Open) ? true : false);
        }


        public static MySQLClient create(string connectionName)
        {
            MySQLClient dbClient = new MySQLClient();
            dbClient.init(connectionName);
            dbClient.connect();
            return dbClient;
        }

        public static MySQLClient createWithConnect(string connectionString)
        {
            MySQLClient dbClient = new MySQLClient();
            dbClient.init(connectionString);
            dbClient.connect();
            return dbClient;
        }

        public void connect()
        {
            this.connection = new MySqlConnection(this.connectionString);
            try
            {
                this.connection.Open();
            }
            catch (Exception exception)
            {
                System.Diagnostics.Debug.WriteLine(exception.ToString());
            }

        }

        public MySqlDataReader query(MySqlCommand command)
        {
            MySqlDataReader result = null;
            try
            {
                result = command.ExecuteReader();

            }
            catch (Exception exception)
            {
                System.Diagnostics.Debug.WriteLine(exception.ToString());
            }
            return result;
        }

        public int update(MySqlCommand command)
        {
            int result = 0;
            try
            {
                result = command.ExecuteNonQuery();

            }
            catch (Exception exception)
            {
                System.Diagnostics.Debug.WriteLine(exception.ToString());
            }
            return result;
        }

        public int delete(MySqlCommand command)
        {
            int result = 0;
            try
            {
                result = command.ExecuteNonQuery();

            }
            catch(Exception exception)
            {
                System.Diagnostics.Debug.WriteLine(exception.ToString());
            }
            return result;
        }

        ///<summary>
        /// Performs insert and returns number of rows.
        ///</summary>
        public int insert(MySqlCommand command)
        {
            int result = 0;
            try
            {
                command.ExecuteNonQuery();
                result = (int)command.LastInsertedId;
            }
            catch (Exception exception)
            {
                System.Diagnostics.Debug.WriteLine(exception.ToString());
            }
            return result;
        }

        public int getLastInsertId(MySqlCommand command)
        {
            return (int)command.LastInsertedId;
        }


        public static DateTime? parseDateTime(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return (DateTime?)null;
            }
            return (DateTime?)reader[fieldName];
        }

        public void close()
        {
            try
            {
                this.connection.Close();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.ToString());
            }
        }

        public static object parseParameter(object parameter)
        {
            return ((parameter == null) ? DBNull.Value : parameter);
        }


        public static int parseInteger(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return -1;
            }
            return Convert.ToInt32(reader[fieldName]);
        }

        public static decimal parseDecimal(MySqlDataReader reader, string fieldName)
        {
            if(DBNull.Value.Equals(reader[fieldName]))
            {
                return -1;
            }
            return Convert.ToDecimal(reader[fieldName]);
        }

        public static string parseString(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return "";
            }
            return Convert.ToString(reader[fieldName]);
        }


        public static bool parseBoolean(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return false;
            }
            return Convert.ToBoolean(reader[fieldName]);
        }

    }
}
