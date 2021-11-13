using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace DatabaseController
{
    public class MySqlClient
    {
        public MySqlConnection connection { get; set; }
        private string connectionString { get; set; }


        public void Init(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public bool IsConnected()
        {
            return ((this.connection.State == ConnectionState.Open) ? true : false);
        }


        public static MySqlClient Create(string connectionName)
        {
            MySqlClient dbClient = new MySqlClient();
            dbClient.Init(connectionName);
            dbClient.Connect();
            return dbClient;
        }

        public static MySqlClient CreateWithConnect(string connectionString)
        {
            MySqlClient dbClient = new MySqlClient();
            dbClient.Init(connectionString);
            dbClient.Connect();
            return dbClient;
        }

        public void Connect()
        {
            this.connection = new MySqlConnection(this.connectionString);
            try
            {
                this.connection.Open();
            }
            catch (Exception exception)
            {
                SystemConsole.WriteLine(exception.ToString());
            }

        }

        public MySqlDataReader Query(MySqlCommand command)
        {
            MySqlDataReader result = null;
            try
            {
                result = command.ExecuteReader();

            }
            catch (Exception exception)
            {
                SystemConsole.WriteLine(exception.ToString());
            }
            return result;
        }

        public int Update(MySqlCommand command)
        {
            int result = 0;
            try
            {
                result = command.ExecuteNonQuery();

            }
            catch (Exception exception)
            {
                SystemConsole.WriteLine(exception.ToString());
            }
            return result;
        }

        public int Delete(MySqlCommand command)
        {
            int result = 0;
            try
            {
                result = command.ExecuteNonQuery();

            }
            catch(Exception exception)
            {
                SystemConsole.WriteLine(exception.ToString());
            }
            return result;
        }

        ///<summary>
        /// Performs insert and returns number of rows.
        ///</summary>
        public int Insert(MySqlCommand command)
        {
            int result = 0;
            try
            {
                command.ExecuteNonQuery();
                result = (int)command.LastInsertedId;
            }
            catch (Exception exception)
            {
                System.Console.WriteLine(exception.ToString());
            }
            return result;
        }

        public int GetLastInsertId(MySqlCommand command)
        {
            return (int)command.LastInsertedId;
        }


        public static DateTime? ParseDateTime(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return (DateTime?)null;
            }
            return (DateTime?)reader[fieldName];
        }

        public void Close()
        {
            try
            {
                this.connection.Close();
            }
            catch (Exception ex)
            {
                SystemConsole.WriteLine(ex.ToString());
            }
        }

        public static object ParseParameter(object parameter)
        {
            return ((parameter == null) ? DBNull.Value : parameter);
        }


        public static int ParseInteger(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return 0;
            }
            return Convert.ToInt32(reader[fieldName]);
        }

        public static long ParseLong(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return 0;
            }
            return Convert.ToInt64(reader[fieldName]);
        }

        public static int? ParseNullableInteger(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return null;
            }
            return Convert.ToInt32(reader[fieldName]);
        }

        public static long? ParseNullableLong(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return null;
            }
            return Convert.ToInt64(reader[fieldName]);
        }

        public static decimal ParseDecimal(MySqlDataReader reader, string fieldName)
        {
            if(DBNull.Value.Equals(reader[fieldName]))
            {
                return 0;
            }
            return Convert.ToDecimal(reader[fieldName]);
        }

        public static string ParseString(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return "";
            }
            return Convert.ToString(reader[fieldName]);
        }


        public static bool ParseBoolean(MySqlDataReader reader, string fieldName)
        {
            if (DBNull.Value.Equals(reader[fieldName]))
            {
                return false;
            }
            return Convert.ToBoolean(reader[fieldName]);
        }

    }
}
