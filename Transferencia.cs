using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;


public class TransferenciaDados
{
    private const string PROTHEUS_TOTAL = "T";
    private const string PROTHEUS_INC = "I";
    private const int SUCESSO = 0;
    public string? logging;
    private string _connectionStringOrigin;
    private string _connectionStringDestination;
    private string _consultaTotal;
    private string _consultaIncremental;

    public TransferenciaDados()
    {
        
        _connectionStringOrigin = ConfigurationManager.ConnectionStrings["Protheus"].ConnectionString;
        _connectionStringDestination = ConfigurationManager.ConnectionStrings["DataWarehouse"].ConnectionString;
        _consultaTotal = File.ReadAllText(@".\consulta_total.sql");
        _consultaIncremental = File.ReadAllText(@".\consulta_incremental.sql");
    }

    public async Task<int> Transferir()
    {
        DataTable listaExec = new();

        try
        {  
            listaExec = LerListaTab(_connectionStringDestination);
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Erro Interno Servidor, {ex}");
            throw;
        }
        
        SqlConnection connectionLk = new(_connectionStringDestination);
        
        try
        {
            connectionLk.Open();
            foreach (DataRow row in listaExec.Rows)
            {
                LimpaTabela(row, connectionLk);
            }
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Erro Interno Servidor, {ex}");
            throw;
        }
        finally
        {
            connectionLk.Close();
            connectionLk.Dispose();
        }
        
        DataSet conjuntoDados = new();

        try
        {
            conjuntoDados = await ExtrairDados(listaExec);
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Erro Interno Servidor, {ex}");
            throw;
        }


        string log4 = "Abrindo conexao com servidor de destino...";
        logging += "<br>" + log4;
        Console.WriteLine(log4);
            
        List<Task> tarefas = [];

        try
        {
            foreach (DataTable tabela in conjuntoDados.Tables)
            {
                tarefas.Add(Task.Run(() => {
                    string log5 = $"Inserindo em modo BULK os dados da tabela {tabela.TableName}, com quantidade: {tabela.Rows.Count} linhas";

                    logging += "<br>" + log5; 
                    Console.WriteLine(log5);
                    InserirDadosBulk(tabela,_connectionStringDestination);
                }));
            }

            await Task.WhenAll(tarefas);
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"Erro Interno Servidor, {ex}");
            throw;
        }
        finally
        {
            foreach (Task tarefa in tarefas)
            {
                tarefa.Dispose();
            }
        }

        listaExec.Clear();
        conjuntoDados.Clear();
        tarefas.Clear();
        return SUCESSO;
    }

    private async Task<DataSet> ExtrairDados(DataTable retorno)
    {
        DataSet dadosProtheus = new();

        List<Task> tarefas = [];
        
        foreach(DataRow linhaExec in retorno.Rows)
        {
            tarefas.Add(Task.Run(() => {
                string log1 = 
                    linhaExec.Field<string>("TP_TABELA") == "T" ? 
                    $"Extraindo Tabela: {linhaExec.Field<string>("NM_TABELA")}, do tipo Extracao Total" : 
                    $"Extraindo Tabela: {linhaExec.Field<string>("NM_TABELA")}, do tipo Extracao Incremental, com coluna {linhaExec.Field<string>("NM_COLUNA")} filtrando {linhaExec.Field<int>("VL_INC_TABELA")}";
                logging += "<br>" + log1;            
                Console.WriteLine(log1);

                DataTable dados = LerDadosTab(_consultaTotal, _consultaIncremental, _connectionStringOrigin, linhaExec.Field<string?>("NM_TABELA"), linhaExec.Field<int?>("VL_INC_TABELA"), linhaExec.Field<string?>("NM_COLUNA"), linhaExec.Field<string?>("TP_TABELA"));

                string log2 = $"Resgatado: {dados.Rows.Count} linhas\nAdicionando ao Dataset...";
                logging += "<br>" + log2;
                Console.WriteLine(log2);

                dadosProtheus.Tables.Add(dados);
            }));
        }
        
        await Task.WhenAll(tarefas);

        foreach(Task tarefa in tarefas)
        {
            tarefa.Dispose();
        }

        tarefas.Clear();
        dadosProtheus.Clear();

        return dadosProtheus;
    }

    private DataTable LerListaTab(string conStr)
    {
        string log4 = "Resgatando Lista de extracao";
        logging += "<br>" + log4;
        Console.WriteLine(log4);
        return Buscador("SELECT * FROM PROTH_EXTLIST;", conStr);
    }

    private DataTable LerDadosTab(string consultaTotal, string consultaIncremental, string conStr, string? NomeTab = null, int? ValorIncremental = null , string? NomeCol = null, string? TipoTab = null)
    {
        if (NomeTab == "")
            throw new Exception("Sem entrada de dados para leitura!");

        if (TipoTab == PROTHEUS_TOTAL || ContaLinhas($"PROTH_{NomeTab}") == 0)
        {
            return Buscador(consultaTotal, conStr, PROTHEUS_TOTAL, NomeTab);
        }
        else
        {
            return Buscador(consultaIncremental, conStr, PROTHEUS_INC, NomeTab, ValorIncremental, NomeCol);
        }
    }

    private DataTable Buscador(string consulta, string conStr, string? Tipo = null, string? NomeTab = null, int? ValorIncremental = null, string? NomeCol = null)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };

        string log1 = "Abrindo conexao com servidor de Origem...";
        logging += "<br>" + log1;
        Console.WriteLine(log1);

        connection.Open();

        SqlCommand comando = new(consulta, connection);
        DataTable dados = new() { TableName = $"PROTH_{NomeTab}" };
        string log6 = NomeTab == null ? $"Armazenando lista de execucao em memoria..." : $"Criando tabela em memoria: {dados.TableName}...";

        logging += "<br>" + log6;
        Console.WriteLine(log6);

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

    private void InserirDadosBulk(DataTable dados, string conStr)
    {
        using SqlConnection connection = new() {
            ConnectionString = conStr
        };

        string log2 = "Abrindo conexao com servidor de destino...";
        logging += "<br>" + log2;
        Console.WriteLine(log2);

        connection.Open();

        using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null);
        bulkCopy.BulkCopyTimeout = 1000;
        bulkCopy.DestinationTableName = dados.TableName;
        bulkCopy.WriteToServer(dados);
        connection.Close();
        connection.Dispose();
    }

    private void LimpaTabela(DataRow retorno, SqlConnection connection)
    {
        using SqlCommand commandCont = new($"SELECT COUNT(1) FROM PROTH_{retorno.Field<string>("NM_TABELA")} WITH(NOLOCK);", connection);
        commandCont.CommandTimeout = 100;
        var exec = commandCont.ExecuteScalar();

        int? linhas = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);

        SqlCommand command = new("", connection);

        switch ((linhas, retorno.Field<string>("TP_TABELA")))
        {
            case (> 0, PROTHEUS_INC):
                string log8 = $"Excluindo linhas recentes da tabela PROTH_{retorno.Field<string>("NM_TABELA")}...";
                logging += "<br>" + log8;
                Console.WriteLine(log8);
                command.CommandText = $"DELETE FROM PROTH_{retorno.Field<string>("NM_TABELA")} WHERE {retorno.Field<string>("NM_COLUNA")} >= GETDATE() - {retorno.Field<int>("VL_INC_TABELA")};";
                command.ExecuteNonQuery();
                break;
            case (_, PROTHEUS_TOTAL):
                string log9 = $"Truncando tabela PROTH_{retorno.Field<string>("NM_TABELA")}...";
                logging += "<br>" + log9;
                Console.WriteLine(log9);
                command.CommandText = $"TRUNCATE TABLE PROTH_{retorno.Field<string>("NM_TABELA")}";
                command.ExecuteNonQuery();
                break;
            default:
                break;
        }
    }

    private int? ContaLinhas(string NomeTab)
    {
        using SqlConnection connection = new()
        {
            ConnectionString = _connectionStringDestination
        };

        string log2 = "Abrindo conexao com servidor de destino para validacao...";
        logging += "<br>" + log2;
        Console.WriteLine(log2);

        connection.Open();

        using SqlCommand command = new($"SELECT COUNT(1) FROM {NomeTab} WITH(NOLOCK);", connection);
        command.CommandTimeout = 100;
        var exec = command.ExecuteScalar();

        int? count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        connection.Close();
        connection.Dispose();
        return count;
    }
}