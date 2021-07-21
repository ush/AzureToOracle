using System;
using Microsoft.Data.SqlClient;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureToOracle
{
    class Program
    {
        static (List<int> id, List<string> TIMESTAMP, List<string> id_source, List<string> type_source,
         List<string> link, List<string> description, List<string> photo, List<string> address, List<int> price) dataLists =
            (new List<int>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(),
            new List<string>(), new List<string>(), new List<string>(), new List<int>());
        static int cnt = 0;

        static int ToSqlServer()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "178.154.196.130";
            builder.UserID = "ucinema";
            builder.Password = "P@sswordCinema";
            builder.InitialCatalog = "Cinema";

            SqlConnection connection = new SqlConnection(builder.ConnectionString);
            try
            {
                String sql = "SELECT * FROM [Cinema].[dbo].[tToOracle]";

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string address = "";
                            cnt++;
                            dataLists.id.Add(reader.GetInt32(0));
                            dataLists.TIMESTAMP.Add(reader.GetDateTime(1).ToString());
                            dataLists.id_source.Add(reader.GetString(2));
                            dataLists.type_source.Add(reader.GetString(3));
                            dataLists.link.Add(reader.GetString(4));
                            dataLists.description.Add(reader.GetString(5));
                            dataLists.photo.Add(reader.GetString(6));
                            try
                            {
                                address = reader.GetString(7);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Error: " + e);
                                Console.WriteLine(e.StackTrace);
                            }
                            finally
                            {
                                dataLists.address.Add(address);
                            }

                            dataLists.price.Add(reader.GetInt32(8));
                            /*string msg = "";
                            msg += reader.GetInt32(0) + " | ";
                            msg += reader.GetDateTime(1) + " | ";
                            for (int i = 2; i < n - 2; i++)
                            {
                                msg += reader.GetString(i) + " | ";
                            }
                            msg += reader.GetInt32(n - 1);
                            Console.WriteLine(msg);*/
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //Console.WriteLine("Error: " + e);
                //Console.WriteLine(e.StackTrace);
                connection.Close();
                connection.Dispose();
                Console.ReadLine();
                return -1;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
            /*Console.WriteLine("LIST FROM SQL");
            Console.WriteLine("=================================================");
            for (int i = 0; i < cnt; i++)
            {
                Console.WriteLine("( " + dataLists.id[i] + ", '" + dataLists.TIMESTAMP[i] + "', " + " '" + dataLists.id_source[i]
                    + "', " + " '" + dataLists.type_source[i] + "', " + " '" + dataLists.link[i] + "', " + " '" + dataLists.description[i] + "', " +
                    " '" + dataLists.photo[i] + "', " + " '" + dataLists.address[i] + "', " + dataLists.price[i] + " )");
            }
            Console.ReadLine();*/
            return 0;
        }

        static int ToOracle()
        {
            string connectionstring = "Data Source = (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=20.86.129.225)(PORT=1521))" +
            "(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = KinoOracle.2cdvt5pw30uuzekap1jl54b4ke.ax.internal.cloudapp.net)))" +
            ";User Id = kino; password = Kino_password";
            OracleConnection con = new OracleConnection();
            con.ConnectionString = connectionstring;
            con.Open();
            try
            {
                OracleCommand drop = new OracleCommand("DROP TABLE TEST", con);
                int drop_ret = drop.ExecuteNonQuery();
                Console.WriteLine("truncate returned with exit code: " + drop_ret);

                OracleCommand create = new OracleCommand("CREATE TABLE TEST ( id int, TS TIMESTAMP, id_source NVARCHAR2(2000), type_source NVARCHAR2(2000), " +
                " link NVARCHAR2(2000), description NVARCHAR2(2000), photo NVARCHAR2(2000), address NVARCHAR2(2000), price int)", con);
                int create_ret = create.ExecuteNonQuery();
                Console.WriteLine("create returned with exit code: " + create_ret);

                for (int i = 0; i < cnt; i++)
                {
                    OracleCommand insert = con.CreateCommand();
                    insert.CommandText = "INSERT INTO TEST VALUES ( " + dataLists.id[i] + ", DATE '" + dataLists.TIMESTAMP[i] +  "', " + " '" + dataLists.id_source[i] 
                    + "', "+ " '" + dataLists.type_source[i] + "', " + " '" + dataLists.link[i] + "', " + " '" + dataLists.description[i] + "', " +
                    " '" + dataLists.photo[i] + "', " + " '" + dataLists.address[i] + "', " + dataLists.price[i] + " )";
                    Console.WriteLine(insert.CommandText);
                    Console.ReadLine();
                    int insert_ret = insert.ExecuteNonQuery();
                    Console.WriteLine("insert returned with exit code: " + insert_ret);
                }
                
                OracleCommand cmd = con.CreateCommand();
                cmd.CommandText = "select * from TEST";

                OracleDataReader Orcl_reader = cmd.ExecuteReader();
                if (Orcl_reader.HasRows)
                {
                    Console.WriteLine("Command returned something");
                }
                else
                {
                    Console.WriteLine("Command returned nothing");
                }
                while (Orcl_reader.Read())
                {
                    Console.WriteLine(Orcl_reader.GetString(0) + " " + Orcl_reader.GetString(1));
                }
                Console.WriteLine("Connected to Oracle!\n");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e);
                Console.WriteLine(e.StackTrace);
                con.Close();
                con.Dispose();
                Console.ReadLine();
                return -1;
            }
            finally
            {
                con.Close();
                con.Dispose();
            }
            return 0;
        }

        static void Main(string[] args)
        {
            int sql_return = ToSqlServer();
            if (sql_return == -1)
            {
                System.Environment.Exit(-1);
            }
            int orcl_return = ToOracle();
            if (orcl_return == -1)
            {
                System.Environment.Exit(-1);
            }
            Console.ReadLine();

            System.Environment.Exit(1);
        }
    }
}
