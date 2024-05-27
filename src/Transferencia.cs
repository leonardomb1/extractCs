using System.Configuration;
using System.Data;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Data.SqlClient;
using Salaros.Configuration;

namespace IntegraCs;


public class TransferenciaDados : IDisposable
{
    private const string TOTAL = "T";
    private const string INCREMENTAL = "I";
    private const int SUCESSO = 1;
    private const int FALHA = 0;
    private string _connectionStringDW;
    private int _tamPacote;
    private bool disposed = false;

    public TransferenciaDados()
    {
        string root;
        if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) { 
            root = @".\app\"; 
        }
        else {
            root = "./app/";
        } 

        var config = new ConfigParser(Path.Combine(root, "config.ini"), new ConfigParserSettings 
            {
                MultiLineValues = 
                    MultiLineValues.Simple | 
                    MultiLineValues.AllowValuelessKeys | 
                    MultiLineValues.QuoteDelimitedValues, Culture = new CultureInfo("en-US")
            });
        _tamPacote = int.Parse(config.GetValue("Packet", "pacoteSql"));
        _connectionStringDW = ConfigurationManager.ConnectionStrings["DataWarehouse"].ConnectionString;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!disposed) {
            if (disposing) {
                Dispose();
            }
        }
        disposed = true;
    }
    ~TransferenciaDados()
    {
        Dispose(disposing: false);
    }

    public async Task Transferir(int agenda)
    {
        List<DataRow> listaExec = [];
        List<Task> tarefas = [];

        DataTable consultas = Buscador(@$"SELECT * FROM DW_CONSULTA", _connectionStringDW);
        int exec = InitExec(_connectionStringDW, agenda);

        try
        {
            await LogOperation(exec, Operador.BUSCA_AGENDA, "Resgatando Lista de Extração...", _connectionStringDW);
            listaExec = BuscaAgenda(agenda, _connectionStringDW);

            await LogOperation(exec, Operador.ITERAR, "Començando Extração...", _connectionStringDW);

            listaExec.ForEach(async linhaExec => {
                int? corte = linhaExec.Field<int?>("VL_INC_TABELA");
                string? coluna = linhaExec.Field<string?>("NM_COLUNA");
                string tabela = linhaExec.Field<string>("NM_TABELA") ?? "";
                string conStr = linhaExec.Field<string>("DS_CONSTRING") ?? "";  
                string sistema = linhaExec.Field<string>("NM_SISTEMA") ?? "";

                string consulta =
                    (
                        from list in consultas.AsEnumerable()
                        where 
                            list.Field<int>("ID_DW_SISTEMA") == linhaExec.Field<int>("ID_DW_SISTEMA") &&
                            list.Field<string>("TP_CONSULTA") == linhaExec.Field<string>("TP_TABELA")
                        select list.Field<string>("DS_CONSULTA")
                    ).FirstOrDefault() ?? "";


                await LogOperation(exec, Operador.INIC_LIMPA_TABELA, $"Limpando Tabela: {sistema}_{tabela}...", _connectionStringDW);
                LimpaTabela(linhaExec, _connectionStringDW, sistema);


                switch (linhaExec.Field<string>("TP_TABELA"))
                {
                    case TOTAL:
                        tarefas.Add(Task.Run(async () => {
                            await BuscadorPacotes(exec, TOTAL, sistema, conStr, consulta, tabela);
                            await LogOperation(exec, Operador.FINAL_LEITURA_PACOTE, $"Concluído extração para: {tabela}", _connectionStringDW);
                        }));
                        break;
                    case INCREMENTAL:
                        tarefas.Add(Task.Run(async () => {
                            await BuscadorPacotes(exec, INCREMENTAL, sistema, conStr, consulta, tabela, corte, coluna);
                            await LogOperation(exec ,Operador.FINAL_LEITURA_PACOTE, $"Concluído extração para: {tabela}", _connectionStringDW);
                        }));
                        break;
                    default:
                        await LogOperation(exec, Operador.FINAL_LEITURA_PACOTE, $"Erro SQL: Não foi definido tipo de extração, para tabela {tabela}", _connectionStringDW, FALHA);
                        break;
                }
                await Task.WhenAll(tarefas);
            });
        }
        catch (SqlException ex)
        {
            await LogOperation(exec ,Operador.ERRO_SQL, $"Erro de Geral de SQL: {ex}", _connectionStringDW, FALHA);
            Updater(_connectionStringDW, exec, FALHA);
        }
        finally
        {

            await LogOperation(exec, Operador.FINAL_SQL, "Extração Realizada.", _connectionStringDW);
            Updater(_connectionStringDW, exec, SUCESSO);

            tarefas.ForEach(async tarefa => {
                await LogOperation(exec, Operador.LIBERA_RECURSO, "Liberando Threads...", _connectionStringDW);
                tarefa.Dispose();
            });
            
            await LogOperation(exec, Operador.LIBERA_RECURSO, "Liberando Recursos...", _connectionStringDW);
            tarefas.Clear();
            listaExec.Clear();
        }
    }

    private static List<DataRow> BuscaAgenda(int agenda, string conStr) 
    {
        DataTable retorno = Buscador(
            @$" SELECT 
                    *
                FROM DW_EXTLIST AS LIS WITH(NOLOCK)
                INNER JOIN DW_SISTEMAS AS SIS WITH(NOLOCK)
                    ON  SIS.ID_DW_SISTEMA = LIS.ID_DW_SISTEMA;", conStr
            );
        
        var result = 
            from lin in retorno.AsEnumerable()
            where lin.Field<int>("ID_DW_AGENDADOR") == agenda
            select lin;

        return result.ToList();
    }

    private async Task BuscadorPacotes(int exec, string Tipo, string sistema, string conStr, string consulta, string? NomeTab = null, int? ValorIncremental = null, string? NomeCol = null)
    {     
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };
        connection.Open();

        int? linhas = ContaLinhas($"{sistema}_{NomeTab}", _connectionStringDW);

        try
        {
            await LogOperation(exec, Operador.INIC_SQL, $"Criando tabela temporaria: ##T_{NomeTab}_DW_SEL...", _connectionStringDW);
            using SqlCommand criarTabelaTemp = new() {
                Connection = connection
            };
            switch ((linhas, Tipo))
            {
                case (_, TOTAL):
                    await LogOperation(exec, Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...", _connectionStringDW);
                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    break;
                case (0, INCREMENTAL):
                    await LogOperation(exec, Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...", _connectionStringDW);
                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    break;
                case (> 0, INCREMENTAL):
                    await LogOperation(exec, Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Incremental da tabela: {NomeTab}...", _connectionStringDW);
                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    criarTabelaTemp.Parameters.AddWithValue("@VL_CORTE", ValorIncremental.ToString());
                    criarTabelaTemp.Parameters.AddWithValue("@COL_DT", NomeCol);
                    break;
                default:
                    await LogOperation(exec, Operador.ABRIR_CONEXAO, $"Conexão Aberta, mas sem tipo definido para a tabela: {NomeTab}...", _connectionStringDW, FALHA);
                    break;
            }
            criarTabelaTemp.CommandTimeout = 1000;
            await criarTabelaTemp.ExecuteNonQueryAsync();
        }
        catch (SqlException ex)
        {
            await LogOperation(exec, Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {NomeTab} ao tentar criar tabela temporária.", _connectionStringDW, FALHA);
        }


        using SqlCommand consultarTabelaTemp = new() {
            Connection = connection,
            CommandText = $"SELECT *, GETDATE(), GETDATE() FROM ##T_{NomeTab}_DW_SEL WITH(NOLOCK);",
            CommandTimeout = 6000
        };

        using SqlCommand deletarTabelaTemp = new() {
            Connection = connection,
            CommandText = $"DROP TABLE IF EXISTS ##T_{NomeTab}_DW_SEL;",
            CommandTimeout = 100
        };

        DataTable pacote = new() { TableName = $"{sistema}_{NomeTab}" };
        await LogOperation(exec, Operador.INIC_SQL, $"Iniciando consulta da tabela: {NomeTab}...", _connectionStringDW);
        
        using SqlDataReader reader = await consultarTabelaTemp.ExecuteReaderAsync();
        
        try
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                pacote.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
            }

            while (reader.ReadAsync().Result)
            {
                DataRow row = pacote.NewRow();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.GetValue(i);
                }
                pacote.Rows.Add(row);

                if (pacote.Rows.Count >= _tamPacote)
                {
                    await LogOperation(exec, Operador.INIC_INSERT_BULK, $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count} Linhas.", _connectionStringDW);
                    InserirDadosBulk(pacote, _connectionStringDW);
                    pacote.Clear();
                    await LogOperation(exec, Operador.INIC_INSERT_BULK, $"Finalizado BULK Insert da tabela: {NomeTab}", _connectionStringDW);
                }
            }

            if (pacote.Rows.Count > 0)
            {
                await LogOperation(exec, Operador.INIC_INSERT_BULK, $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count}", _connectionStringDW);
                InserirDadosBulk(pacote, _connectionStringDW);
                await LogOperation(exec, Operador.INIC_INSERT_BULK, $"Finalizado BULK Insert da tabela: {NomeTab}", _connectionStringDW);
            }


        }
        catch (SqlException ex)
        {
            await LogOperation(exec, Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {NomeTab} ao tentar executar inserção de dados", _connectionStringDW, FALHA);
        }
        finally
        {
            if (reader!= null &&!reader.IsClosed)
            {
                reader.Close();
            }
        }

        await LogOperation(exec, Operador.INIC_SQL, $"Deletando tabela temporaria: ##T_{NomeTab}_DW_SEL...", _connectionStringDW);
        pacote.Dispose();
        await deletarTabelaTemp.ExecuteNonQueryAsync();
        
        
        await reader.DisposeAsync();
        await connection.CloseAsync();
        await connection.DisposeAsync();
    }

    private static void Updater(string conStr, int numExec, int status)
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

    private static int InitExec(string conStr, int agenda)
    {
        using SqlConnection connection = new(conStr);
        connection.Open();  
        connection.ChangeDatabase("DW_CONTROLLER");  
        using SqlCommand execState = new() {
            CommandText = 
                @$"
                    INSERT INTO DW_EXECUCAO (ID_DW_SISTEMA, ID_DW_AGENDADOR)
                    OUTPUT INSERTED.ID_DW_EXECUCAO
                    VALUES ({1}, {agenda})
                ",
            Connection = connection
        };
        var ret = execState.ExecuteScalar();
        int exec = Convert.ToInt32(ret == DBNull.Value ? 0 : ret);

        connection.Close();
        connection.Dispose();
        execState.Dispose();
        return exec;
    }

    private static DataTable Buscador(string consulta, string conStr, string database = "DW_CONTROLLER", string? sistema = null, string? Tipo = null, string? NomeTab = null, int? ValorIncremental = null, string? NomeCol = null)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };
        connection.Open();
        connection.ChangeDatabase(database);
        SqlCommand comando = new(consulta, connection);
        DataTable dados = new() { TableName = $"{sistema}_{NomeTab}" };

        switch (Tipo)
        {
            case TOTAL:
                comando.Parameters.AddWithValue("@TABELA", NomeTab);
                break;
            case INCREMENTAL:
                comando.Parameters.AddWithValue("@TABELA", NomeTab);
                comando.Parameters.AddWithValue("@VL_CORTE", ValorIncremental.ToString());
                comando.Parameters.AddWithValue("@COL_DT", NomeCol);
                break;
            default:
                break;
        }


        comando.CommandTimeout = 1000;
        SqlDataAdapter adapter = new(comando);
        adapter.Fill(dados);

        connection.Close();
        connection.Dispose();
       
        adapter.Dispose();
        return dados;
    }

    private static void InserirDadosBulk(DataTable dados, string conStr)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };
        connection.Open();
        connection.ChangeDatabase("DW_EXTRACT");

        using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null);
        bulkCopy.BulkCopyTimeout = 1000;
        bulkCopy.DestinationTableName = dados.TableName;
        bulkCopy.WriteToServer(dados);
        connection.Close();
        connection.Dispose();
    }

    private static void LimpaTabela(DataRow retorno, string conStr, string sistema)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };
        connection.Open();
        connection.ChangeDatabase("DW_EXTRACT");
        using SqlCommand commandCont = new($"SELECT COUNT(1) FROM {sistema}_{retorno.Field<string>("NM_TABELA")} WITH(NOLOCK);", connection);
        commandCont.CommandTimeout = 100;
        var exec = commandCont.ExecuteScalar();

        int linhas = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);

        SqlCommand command = new("", connection);

        switch ((linhas, retorno.Field<string>("TP_TABELA")))
        {
            case (> 0, INCREMENTAL):
                command.CommandText = 
                    @$" DELETE FROM {sistema}_{retorno.Field<string>("NM_TABELA")} 
                        WHERE [{retorno.Field<string>("NM_COLUNA")}] >= GETDATE() - {retorno.Field<int>("VL_INC_TABELA")};";
                command.ExecuteNonQuery();
                break;
            case (_, TOTAL):
                command.CommandText = $"TRUNCATE TABLE {sistema}_{retorno.Field<string>("NM_TABELA")};";
                command.ExecuteNonQuery();
                break;
            default:
                break;
        }

        connection.Close();
        connection.Dispose();
    }

    private static async Task LogOperation(int exec, int cdLog, string logging, string conStr, int sucesso = SUCESSO)
    {
        SqlConnection connection = new() {
            ConnectionString = conStr
        };
        await connection.OpenAsync();
        await connection.ChangeDatabaseAsync("DW_CONTROLLER");
        Console.WriteLine(logging);
        SqlCommand log = new() {
            Connection = connection,
            CommandText = 
                $@"INSERT INTO DW_LOG (ID_DW_EXECUCAO, ID_DW_OPERACAO, DS_LOG, VF_SUCESSO)
                VALUES({exec}, {cdLog}, '{logging}', {sucesso})"
        };

        await log.ExecuteNonQueryAsync();

        await connection.CloseAsync();
        await connection.DisposeAsync();
        await log.DisposeAsync();
    }

    private static int? ContaLinhas(string NomeTab, string conStr)
    {
        using SqlConnection connection = new(conStr);
        connection.Open();
        connection.ChangeDatabase("DW_EXTRACT");

        using SqlCommand command = new($"SELECT COUNT(1) FROM {NomeTab} WITH(NOLOCK);", connection);
        command.CommandTimeout = 100;
        var exec = command.ExecuteScalar();

        int? count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        connection.CloseAsync();
        return count;
    }
}
