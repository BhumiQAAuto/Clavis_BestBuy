using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.IO;
using System.Windows.Forms;

namespace BestBuy_CA_ClassLibrary
{
    public class MySqlDB
    {
        string ConnectionString = File.ReadAllText(Application.StartupPath + "\\ConnectionString.txt");
        MySqlConnection con;
        DataTable dt = new DataTable();
        FileReadWrite ReadWrite = new FileReadWrite();
        public Boolean IsDBError = false;

        public int sqlExecuteNonQuery(string queryString)
        {
            con = new MySqlConnection(ConnectionString);
            con.ConnectionString = ConnectionString;
            int Irows = 0;
            try
            {
                using (MySqlCommand command = new MySqlCommand(queryString, con))
                {
                    con.Open();
                    Irows = command.ExecuteNonQuery();
                }
            }
            catch (Exception Ex)
            {
                IsDBError = true;
                ReadWrite.WriteLog("Record Not Affected " + Ex.Message);
            }
            finally
            {
                con.Close();
            }
            return Irows;
        }

        public DataTable sqlDataAdapter(string queryString)
        {
            con = new MySqlConnection(ConnectionString);
            con.ConnectionString = ConnectionString;
            DataTable dt = new DataTable();
            try
            {
                using (MySqlCommand cmd = new MySqlCommand(queryString, con))
                {
                    cmd.Connection = con;
                    using (MySqlDataAdapter sda = new MySqlDataAdapter(cmd))
                    {
                        sda.Fill(dt);
                    }
                }
            }
            catch (Exception Ex)
            {
                IsDBError = true;
                ReadWrite.WriteLog("Error in Datatable " + Ex.Message);
            }
            finally
            {
                con.Close();
            }
            return dt;
        }

        public string SQLExecuteScalar(string queryString)
        {
            con = new MySqlConnection(ConnectionString);
            con.ConnectionString = ConnectionString;
            string Result = string.Empty;
            int Retry = 0;
            using (MySqlCommand cmd = new MySqlCommand(queryString, con))
            {
                while (Retry < 3)
                {
                RunAgain_DataSet:
                    try
                    {
                        if (ConnectionState.Open != con.State)
                            con.Open();
                        Result = cmd.ExecuteScalar().ToString();
                    }
                    catch (MySqlException Ex)
                    {
                        if (Retry > 3)
                        {
                            IsDBError = true;
                            break;
                        }
                        Retry++;
                        goto RunAgain_DataSet;
                    }
                    finally
                    {
                        con.Close();
                    }
                    if (ConnectionState.Closed == con.State)
                        break;
                }
                Retry = 0;
                return Result;
            }
        }

        public bool boolExecuteScalar(string queryString)
        {
            con = new MySqlConnection(ConnectionString);
            con.ConnectionString = ConnectionString;
            Boolean IsExist = true;
            using (MySqlCommand command = new MySqlCommand(queryString, con))
            {
                try
                {
                    con.Open();
                    IsExist = Convert.ToBoolean(command.ExecuteScalar());

                }
                catch (Exception Ex)
                {
                    IsDBError = true;
                    ReadWrite.WriteLog("DataBase not checked " + Ex.Message);
                }
                finally
                {
                    con.Close();
                }
            }
            return IsExist;
        }
    }
}
