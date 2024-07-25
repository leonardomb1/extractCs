using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;
public static class ComExtract
{
    public static async Task LimpaTabela(
        string nomeTab,
        string conStr,
        string? nomeCol,
        string tipoTab,
        string sistema,
        int idSistema,
        List<ConsultaInfo> infoConsulta,
        string nomeInd,
        int? inclusao)
    {
        string consulta = infoConsulta
                        .Where(x => x.SistemaTipo == idSistema && x.ConsultaTipo == ConstInfo.DELETE)
                        .Select(x => x.Consulta)
                        .FirstOrDefault() ?? "N/A";
        
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };

        await connection.OpenAsync();
        await connection.ChangeDatabaseAsync("DWExtract"); // Deverá ser usado no Extract
        using SqlCommand commandCont = new($"SELECT COUNT(*) FROM {sistema}.{nomeTab} WITH(NOLOCK);", connection);
        commandCont.CommandTimeout = 100;
        var exec = commandCont.ExecuteScalarAsync().Result;
        
        int linhas = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);

        SqlCommand command = new("", connection);

        switch ((linhas, tipoTab))
        {
            case (> 0, ConstInfo.INCREMENTAL):
                command.CommandText = consulta;
                command.Parameters.AddWithValue("@NOMEIND", nomeInd);
                command.Parameters.AddWithValue("@TABELA", nomeTab);
                command.Parameters.AddWithValue("@VL_CORTE", inclusao.ToString());
                command.Parameters.AddWithValue("@COL_DT", nomeCol);
                await command.ExecuteNonQueryAsync();
                break;
            case (0, ConstInfo.INCREMENTAL):
                command.CommandText = $"TRUNCATE TABLE {sistema}.{nomeTab};";
                await command.ExecuteNonQueryAsync();
                break;            
            case (_, ConstInfo.TOTAL):
                command.CommandText = $"TRUNCATE TABLE {sistema}.{nomeTab};";
                await command.ExecuteNonQueryAsync();
                break;
            default:
                break;
        }

        await connection.CloseAsync();
        await connection.DisposeAsync();
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