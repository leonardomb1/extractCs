using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;
public static class ComExtract
{
    public static void LimpaTabela(
        string nomeTab,
        string conStr,
        string? nomeCol,
        string tipoTab,
        string sistema,
        int? inclusao)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };
        connection.Open();
        connection.ChangeDatabase("DWExtract"); // Deverá ser usado no Extract
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

    public static int ContaLinhas(string NomeTab, string conStr)
    {      
        using SqlConnection connection = new(conStr);
        connection.Open();
        connection.ChangeDatabase("DWExtract");

        using SqlCommand command = new($"SELECT COUNT(*) FROM {NomeTab} WITH(NOLOCK);", connection);
        command.CommandTimeout = 100;
        var exec = command.ExecuteScalar();

        int count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        connection.Close();

        return count;
    }

    public static async Task InserirDadosBulk(DataTable dados, string conStr)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };
        connection.Open();
        connection.ChangeDatabase("DWExtract");

        using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock
                                                        | SqlBulkCopyOptions.UseInternalTransaction, null);
        bulkCopy.BulkCopyTimeout = 1000;
        bulkCopy.DestinationTableName = dados.TableName;
        await bulkCopy.WriteToServerAsync(dados);
        connection.Close();
        connection.Dispose();
    }
}