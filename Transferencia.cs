using System.Configuration;
using System.Data;
using System.Globalization;
using Microsoft.Data.SqlClient;
using Salaros.Configuration;

namespace IntegraCs;


public class TransferenciaDados
{
    private const string PROTHEUS_TOTAL = "T";
    private const string PROTHEUS_INC = "I";
    private const int SUCESSO = 1;
    private const int FALHA = 0;
    private string _connectionStringOrigin ;
    private string _connectionStringDestination;
    private string _consultaTotal;
    private int _tamPacote;
    private string _consultaIncremental;

    public TransferenciaDados()
    {
        var config = new ConfigParser(@".\config.ini", new ConfigParserSettings 
            {
                MultiLineValues = 
                    MultiLineValues.Simple | 
                    MultiLineValues.AllowValuelessKeys | 
                    MultiLineValues.QuoteDelimitedValues, Culture = new CultureInfo("en-US")
            });
        _tamPacote = int.Parse(config.GetValue("Packet", "pacoteSql"));
        
        _connectionStringOrigin = ConfigurationManager.ConnectionStrings["Protheus"].ConnectionString;
        _connectionStringDestination = ConfigurationManager.ConnectionStrings["DataWarehouse"].ConnectionString;
        _consultaTotal = File.ReadAllText(@".\consulta_total.sql");
        _consultaIncremental = File.ReadAllText(@".\consulta_incremental.sql");
    }
    public async Task Transferir(int agenda)
    {
        List<DataRow> listaExec = [];
        SqlConnection connectionLk = new(_connectionStringDestination);
        List<Task> tarefas = [];

        try
        {
            listaExec = BuscaAgenda(agenda, _connectionStringDestination);
            await LogOperation(Operador.BUSCA_AGENDA, "Resgatado Agendas.", _connectionStringDestination, SUCESSO);

            
            connectionLk.Open();
            foreach (DataRow row in listaExec)
            {
                await LogOperation(Operador.INIC_LIMPA_TABELA, $"Limpando Tabela: PROTH_{row.Field<string>("NM_TABELA")}...", _connectionStringDestination, SUCESSO);
                LimpaTabela(row, connectionLk);
            }
            connectionLk.Close();
            
            await LogOperation(Operador.ITERAR, "Començando Extração...", _connectionStringDestination, SUCESSO);
            foreach(DataRow linhaExec in listaExec)
            {
                string tabela = linhaExec.Field<string>("NM_TABELA") ?? "";
                string? coluna = linhaExec.Field<string?>("NM_COLUNA");
                int? corte = linhaExec.Field<int?>("VL_INC_TABELA");

                await LogOperation(Operador.ENTRAR_VERIF_LOG, "Verificando tipo de extração...", _connectionStringDestination, SUCESSO);
                switch (linhaExec.Field<string>("TP_TABELA"))
                {
                    case PROTHEUS_TOTAL:
                        tarefas.Add(Task.Run(async () => {
                            try
                            {
                                await BuscadorPacotes(PROTHEUS_TOTAL, tabela);
                                await LogOperation(Operador.FINAL_LEITURA_PACOTE, $"Concluído extração para: {tabela}", _connectionStringDestination, SUCESSO);
                            }
                            catch (SqlException ex)
                            {
                                await LogOperation(Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {tabela}", _connectionStringDestination, FALHA);
                            } 
                        }));
                        break;
                    case PROTHEUS_INC:
                        tarefas.Add(Task.Run(async () => {
                            try 
                            {
                                await BuscadorPacotes(PROTHEUS_INC, tabela, corte, coluna);
                                await LogOperation(Operador.FINAL_LEITURA_PACOTE, $"Concluído extração para: {tabela}", _connectionStringDestination, SUCESSO);
                            }
                            catch (SqlException ex)
                            {
                                await LogOperation(Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {tabela}", _connectionStringDestination, FALHA);
                            }
                        }));
                        break;
                    default:
                        await LogOperation(Operador.FINAL_LEITURA_PACOTE, $"Erro SQL: Não foi definido tipo de extração, para tabela {tabela}", _connectionStringDestination, FALHA);
                        break;
                }
            }
            
            await Task.WhenAll(tarefas);
        }
        catch (SqlException ex)
        {
            await LogOperation(Operador.ERRO_SQL, $"Erro de Geral de SQL: {ex}", _connectionStringDestination, FALHA);
        }
        finally
        {
            foreach(Task tarefa in tarefas)
            {
                await LogOperation(Operador.LIBERA_RECURSO, "Liberando Threads...", _connectionStringDestination, SUCESSO);
                tarefa.Dispose();
            }
            
            await LogOperation(Operador.LIBERA_RECURSO, "Liberando Conexão e Recursos...", _connectionStringDestination, SUCESSO);
            tarefas.Clear();
            listaExec.Clear();
            connectionLk.Dispose();
            await Task.CompletedTask;
        }
    }

    private static List<DataRow> BuscaAgenda(int agenda, string conStr) {
        DataTable retorno = Buscador(
            @$" SELECT 
                    LIS.*, 
                    AG.VL_RECORRENCIA 
                FROM PROTH_EXTLIST AS LIS WITH(NOLOCK)
                INNER JOIN DW_AGENDADOR AS AG WITH(NOLOCK)
                    ON AG.ID_DW_AGENDADOR = LIS.ID_DW_AGENDADOR", conStr
            );
        
        var result = 
            from lin in retorno.AsEnumerable()
            where lin.Field<int>("ID_DW_AGENDADOR") == agenda
            select lin;

        return result.ToList();
    }

    private async Task BuscadorPacotes(string Tipo, string? NomeTab = null, int? ValorIncremental = null, string? NomeCol = null)
    {     
        using SqlConnection connection = new() {
            ConnectionString = _connectionStringOrigin,
        };
        await connection.OpenAsync();

        int? linhas = await ContaLinhas($"PROTH_{NomeTab}", _connectionStringDestination);

        try
        {
            await LogOperation(Operador.INIC_SQL, $"Criando tabela temporaria: ##T_{NomeTab}_DW_SEL...", _connectionStringDestination, SUCESSO);
            using SqlCommand criarTabelaTemp = new() {
                Connection = connection
            };
            switch ((linhas, Tipo))
            {
                case (_, PROTHEUS_TOTAL):
                    await LogOperation(Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...", _connectionStringDestination, SUCESSO);
                    criarTabelaTemp.CommandText = _consultaTotal;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                    break;
                case (0, _):
                    await LogOperation(Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...", _connectionStringDestination, SUCESSO);
                    criarTabelaTemp.CommandText = _consultaTotal;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                    break;
                case (> 0, PROTHEUS_INC):
                    await LogOperation(Operador.ABRIR_CONEXAO, $"Conexão aberta para extração do tipo Incremental da tabela: {NomeTab}...", _connectionStringDestination, SUCESSO);
                    criarTabelaTemp.CommandText = _consultaIncremental;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                    criarTabelaTemp.Parameters.AddWithValue("@VL_CORTE", ValorIncremental.ToString());
                    criarTabelaTemp.Parameters.AddWithValue("@COL_DT", NomeCol);
                    break;
                default:
                    await LogOperation(Operador.ABRIR_CONEXAO, $"Conexão Aberta, mas sem tipo definido para a tabela: {NomeTab}...", _connectionStringDestination, FALHA);
                    break;
            }
            criarTabelaTemp.CommandTimeout = 1000;
            await criarTabelaTemp.ExecuteNonQueryAsync();
        }
        catch (SqlException ex)
        {
            await LogOperation(Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {NomeTab} ao tentar criar tabela temporária.", _connectionStringDestination, FALHA);
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

        DataTable pacote = new() { TableName = $"PROTH_{NomeTab}" };
        await LogOperation(Operador.INIC_SQL, $"Iniciando consulta da tabela: {NomeTab}...", _connectionStringDestination, SUCESSO);
        
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
                    await LogOperation(Operador.INIC_INSERT_BULK, $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count}", _connectionStringDestination, SUCESSO);
                    InserirDadosBulk(pacote, _connectionStringDestination);
                    pacote.Clear();
                    await LogOperation(Operador.INIC_INSERT_BULK, $"Finalizado BULK Insert da tabela: {NomeTab}", _connectionStringDestination, SUCESSO);
                }
            }

            if (pacote.Rows.Count > 0)
            {
                await LogOperation(Operador.INIC_INSERT_BULK, $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count}", _connectionStringDestination, SUCESSO);
                InserirDadosBulk(pacote, _connectionStringDestination);
                await LogOperation(Operador.INIC_INSERT_BULK, $"Finalizado BULK Insert da tabela: {NomeTab}", _connectionStringDestination, SUCESSO);
            }
        }
        catch (SqlException ex)
        {
            await LogOperation(Operador.ERRO_SQL, $"Erro SQL: {ex} na tabela {NomeTab} ao tentar executar inserção de dados", _connectionStringDestination, FALHA);
        }
        finally
        {
            if (reader!= null &&!reader.IsClosed)
            {
                reader.Close();
            }
        }

        pacote.Dispose();
        await LogOperation(Operador.INIC_SQL, $"Deletando tabela temporaria: ##T_{NomeTab}_DW_SEL...", _connectionStringDestination, SUCESSO);
        await reader.DisposeAsync();
        await deletarTabelaTemp.ExecuteNonQueryAsync();
        await connection.CloseAsync();
        await connection.DisposeAsync();
    }

    private static DataTable Buscador(string consulta, string conStr, string? Tipo = null, string? NomeTab = null, int? ValorIncremental = null, string? NomeCol = null)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };
        connection.Open();
        SqlCommand comando = new(consulta, connection);
        DataTable dados = new() { TableName = $"PROTH_{NomeTab}" };

        switch (Tipo)
        {
            case PROTHEUS_TOTAL:
                comando.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                break;
            case PROTHEUS_INC:
                comando.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
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

        using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null);
        bulkCopy.BulkCopyTimeout = 1000;
        bulkCopy.DestinationTableName = dados.TableName;
        bulkCopy.WriteToServer(dados);
        connection.Close();
        connection.Dispose();
    }

    private static void LimpaTabela(DataRow retorno, SqlConnection connection)
    {
        using SqlCommand commandCont = new($"SELECT COUNT(1) FROM PROTH_{retorno.Field<string>("NM_TABELA")} WITH(NOLOCK);", connection);
        commandCont.CommandTimeout = 100;
        var exec = commandCont.ExecuteScalar();

        int linhas = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);

        SqlCommand command = new("", connection);

        switch ((linhas, retorno.Field<string>("TP_TABELA")))
        {
            case (> 0, PROTHEUS_INC):
                command.CommandText = 
                    @$" DELETE FROM PROTH_{retorno.Field<string>("NM_TABELA")} 
                        WHERE [{retorno.Field<string>("NM_COLUNA")}] >= GETDATE() - {retorno.Field<int>("VL_INC_TABELA")};";
                command.ExecuteNonQuery();
                break;
            case (_, PROTHEUS_TOTAL):
                command.CommandText = $"TRUNCATE TABLE PROTH_{retorno.Field<string>("NM_TABELA")};";
                command.ExecuteNonQuery();
                break;
            default:
                break;
        }
    }

    private static async Task LogOperation(int cdLog, string logging,  string conStr, int sucesso = 0)
    {
        SqlConnection connection = new() {
            ConnectionString = conStr
        };
        await connection.OpenAsync();
        Console.WriteLine(logging);
        SqlCommand log = new() {
            Connection = connection,
            CommandText = 
                $@"INSERT INTO DW_LOG (ID_DW_SISTEMA, ID_DW_OPERACAO, DS_LOG, VF_SUCESSO)
                VALUES({1}, {cdLog}, '{logging}', {sucesso})"
        };

        await log.ExecuteNonQueryAsync();

        await connection.CloseAsync();
        await connection.DisposeAsync();
        await log.DisposeAsync();
    }

    private static async Task<int?> ContaLinhas(string NomeTab, string conStr)
    {
        using SqlConnection connection = new(conStr);
        await connection.OpenAsync();

        using SqlCommand command = new($"SELECT COUNT(1) FROM {NomeTab} WITH(NOLOCK);", connection);
        command.CommandTimeout = 100;
        var exec = command.ExecuteScalar();

        int? count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        await connection.CloseAsync();
        return count;
    }
}