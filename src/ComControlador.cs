using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;
public static class ComControlador
{    
    public static int BuscaIdExec(string conStr, int agenda)
    {
        using SqlConnection connection = new(conStr);
        connection.Open();  
        connection.ChangeDatabase("DW_CONTROLLER");  // Controlador de comunicação
        Console.WriteLine(connection.Database);
        using SqlCommand buscaId = new() {
            CommandText = 
                @$"
                    INSERT INTO DW_EXECUCAO (ID_DW_AGENDADOR)
                    OUTPUT INSERTED.ID_DW_EXECUCAO
                    VALUES ({agenda})
                ", // Retorna ID da execução da agenda atual
            Connection = connection
        };
        var ret = buscaId.ExecuteScalar();

        int idExec = Convert.ToInt32(ret == DBNull.Value ? 0 : ret); // Caso receber NULL do BD considerar como id 0.

        connection.Close();
        connection.Dispose();
        buscaId.Dispose();
        return idExec;
    }

    public static DataTable BuscaDados(string consulta,
                                     string conStr,
                                     string database = "DW_CONTROLLER" // Banco padrão é o Controller
                                    )
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };
        connection.Open();
        connection.ChangeDatabase(database); // Irá mudar o banco de dados para aquele definido em parâmetro

        SqlCommand comando = new(consulta, connection);
        DataTable dados = new();
    
        SqlDataAdapter adapter = new(comando);
        adapter.Fill(dados);

        connection.Close();

        connection.Dispose();      
        adapter.Dispose();
        return dados;
    }

    public static void AtualizaTempoFinalExe(string conStr, int numExec, int status)
    {
        using SqlConnection updater = new(conStr);
        updater.Open();
        updater.ChangeDatabase("DW_CONTROLLER");
        using SqlCommand update = new() {
            CommandText = 
                $@"
                    UPDATE DW_EXECUCAO SET VF_STATUS = {status} 
                    WHERE ID_DW_EXECUCAO = {numExec};

                    UPDATE DW_EXECUCAO SET DT_FIM_EXEC = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Atlantic Standard Time'
                    WHERE ID_DW_EXECUCAO = {numExec};
                ",
                Connection = updater
        };
        update.ExecuteNonQuery();
        updater.Close();
        update.Dispose();
        updater.Dispose();
    }

    public static async Task Log(int exec,
                                 int cdLog,
                                 string logging,
                                 string conStr,
                                 int sucesso = ConstInfo.SUCESSO, // Considera que por padrão operações são logadas quando há sucesso.
                                 int? idTabela = null)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };
        

        string idExec = exec.ToString() ?? "NULL";
        string idOp = cdLog.ToString() ?? "NULL";
        string idTab = idTabela == null ? "NULL" : idTabela.ToString() ?? "NULL";

        await connection.OpenAsync();
        await connection.ChangeDatabaseAsync("DW_CONTROLLER");
        Console.WriteLine(logging);
        SqlCommand log = new() {
            Connection = connection,
            CommandText = 
                $@"INSERT INTO DW_LOG (ID_DW_EXECUCAO, ID_DW_OPERACAO, DS_LOG, VF_SUCESSO, ID_DW_EXTLIST)
                VALUES({idExec}, {idOp}, '{logging}', {sucesso}, {idTab})"
        };

        await log.ExecuteNonQueryAsync();

        await connection.CloseAsync();
        await connection.DisposeAsync();
        await log.DisposeAsync();
    }

    public static DataTable BuscaAgenda(string orquestConStr)
    {
        Console.WriteLine("Resgantando agendas de execucao...");
        using SqlConnection connection = new(orquestConStr);
        connection.Open();
        connection.ChangeDatabase("DW_CONTROLLER");

        using SqlCommand command = new("SELECT * FROM DW_AGENDADOR", connection);
        SqlDataAdapter adapter = new(command);
        DataTable tabela = new();
        adapter.Fill(tabela);

        Console.WriteLine("Resgatado.");
        return tabela;
    }

}