using System.Data;
using Microsoft.Data.SqlClient;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;

namespace IntegraCs;
public static class ComExtract
{
    // TODO: Deverá ser remodelado caso ser usado outro banco de DW
    public static void LimpaTabela(
        string nomeTab,
        string conStr,
        string? nomeCol,
        string tipoTab,
        string sistema,
        string servidor,
        int? inclusao)
    {
        if (servidor == "SQLSERVER")
        {      
            using SqlConnection connection = new() {
                ConnectionString = conStr
            };
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT"); // Deverá ser usado no Extract
            using SqlCommand commandCont = new($"SELECT COUNT(*) FROM {sistema}_{nomeTab} WITH(NOLOCK);", connection);
            commandCont.CommandTimeout = 100;
            var exec = commandCont.ExecuteScalar();
            
            int linhas = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);

            SqlCommand command = new("", connection);

            switch ((linhas, tipoTab))
            {
                case (> 0, ConstInfo.INCREMENTAL):
                    command.CommandText = 
                        @$" DELETE FROM {sistema}_{nomeTab} 
                            WHERE [{nomeCol}] >= GETDATE() - {inclusao};";
                    command.ExecuteNonQuery();
                    break;
                case (0, ConstInfo.INCREMENTAL):
                    command.CommandText = $"TRUNCATE TABLE {sistema}_{nomeTab};";
                    command.ExecuteNonQuery();
                    break;            
                case (_, ConstInfo.TOTAL):
                    command.CommandText = $"TRUNCATE TABLE {sistema}_{nomeTab};";
                    command.ExecuteNonQuery();
                    break;
                default:
                    break;
            }

            connection.Close();
            connection.Dispose();
        }
        else if (servidor == "CLICKHOUSE")
        {
            using ClickHouseConnection connection = new(conStr);
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT"); // Deverá ser usado no Extract
            using ClickHouseCommand commandCont = new() {
                CommandText = $"SELECT COUNT(*) FROM {sistema}_{nomeTab};",
                Connection = connection
            };
            commandCont.CommandTimeout = 100;
            var exec = commandCont.ExecuteScalar();
            
            int linhas = Convert.ToInt32(exec == DBNull.Value? 0 : exec);

            ClickHouseCommand command = new() {Connection = connection};

            switch ((linhas, tipoTab))
            {
                case (> 0, ConstInfo.INCREMENTAL):
                    command.CommandText = 
                        @$" DELETE FROM {sistema}_{nomeTab} 
                            WHERE [{nomeCol}] >= Now64() - {inclusao};";
                    command.ExecuteNonQuery();
                    break;
                case (0, ConstInfo.INCREMENTAL):
                    command.CommandText = $"TRUNCATE TABLE {sistema}_{nomeTab};";
                    command.ExecuteNonQuery();
                    break;            
                case (_, ConstInfo.TOTAL):
                    command.CommandText = $"TRUNCATE TABLE {sistema}_{nomeTab};";
                    command.ExecuteNonQuery();
                    break;
                default:
                    break;
            }

            connection.Close();
            connection.Dispose();
        } else {
            throw new Exception("Servidor inválido");
        }
    }

    public static int ContaLinhas(string NomeTab, string conStr, string servidor)
    {
        int count = 0;
        
        if (servidor == "SQLSERVER") {
            using SqlConnection connection = new(conStr);
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT");

            using SqlCommand command = new($"SELECT COUNT(*) FROM {NomeTab} WITH(NOLOCK);", connection);
            command.CommandTimeout = 100;
            var exec = command.ExecuteScalar();

            count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
            connection.Close();
        } else if (servidor == "CLICKHOUSE") {
            using ClickHouseConnection connection = new(conStr);
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT");

            using ClickHouseCommand command = new() {
                CommandText = $"SELECT COUNT(*) FROM {NomeTab};",
                Connection = connection
            };
            command.CommandTimeout = 100;
            var exec = command.ExecuteScalar();
            count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        } else {
            throw new Exception("Servidor inválido");
        }

        return count;
    }
    // SQL SERVER
    public static async Task InserirDadosBulk(DataTable dados, string conStr, string servidor)
    {
        if (servidor == "SQLSERVER")
        {
            using SqlConnection connection = new() {
                ConnectionString = conStr
            };
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT");

            using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock
                                                         | SqlBulkCopyOptions.UseInternalTransaction, null);
            bulkCopy.BulkCopyTimeout = 1000;
            bulkCopy.DestinationTableName = dados.TableName;
            await bulkCopy.WriteToServerAsync(dados);
            connection.Close();
            connection.Dispose();
        } 
        else if (servidor == "CLICKHOUSE")
        {
            using ClickHouseConnection connection = new(conStr);
            connection.Open();
            connection.ChangeDatabase("DW_EXTRACT");

            using ClickHouseBulkCopy bulkCopy = new(connection) 
            {
                DestinationTableName = dados.TableName
            };

            await bulkCopy.WriteToServerAsync(dados.CreateDataReader());
            connection.Close();
            connection.Dispose();
        }
        else {
            throw new Exception("Servidor inválido");
        }
    }
}