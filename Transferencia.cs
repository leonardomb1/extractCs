using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;

public class InfoTab
{
    public required string NomeTab { get; set; }
    public int? ValorIncremental { get; set; }
    public string? NomeCol { get; set; }
    public required string TipoTab { get; set; }
}

public class TransferenciaDados
{
    private const int PROTHEUS_TOTAL = 10;
    private const int PROTHEUS_INC = 12;
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

    public int Transferir()
    {
        DataTable listaExec = LerListaTab();
        DataSet dadosProtheus = new();

        foreach (DataRow row in listaExec.Rows)
        {
            InfoTab infoTab = new()
            {
                NomeTab = row.Field<string>("NM_TABELA") ?? "",
                ValorIncremental = row.Field<int?>("VL_INC_TABELA"),
                NomeCol = row.Field<string?>("NM_COLUNA"),
                TipoTab = row.Field<string>("TP_TABELA") ?? ""
            };
            string log1 = 
                infoTab.TipoTab == "T" ? 
                $"Extraindo Tabela: {infoTab.NomeTab}, do tipo Extracao Total" : 
                $"Extraindo Tabela: {infoTab.NomeTab}, do tipo Extracao Incremental, com coluna {infoTab.NomeCol} filtrando {infoTab.ValorIncremental}";
            logging += "<br>" + log1;

            Console.WriteLine(log1);
            DataTable dados = LerDadosTab(infoTab, _consultaTotal, _consultaIncremental);

            string log2 = $"Resgatado: {dados.Rows.Count} linhas\nAdicionando ao Dataset...";

            logging += "<br>" + log2;
            Console.WriteLine(log2);
            dadosProtheus.Tables.Add(dados);
        }

        foreach (DataTable tabela in dadosProtheus.Tables)
        {
            string log3 = $"Inserindo em modo BULK os dados da tabela {tabela.TableName}, com quantidade: {tabela.Rows.Count} linhas";

            logging += "<br>" + log3; 
            Console.WriteLine(log3);
            InserirDadosBulk(tabela);
        }

        listaExec.Clear();
        dadosProtheus.Clear();
        return SUCESSO;
    }

    private DataTable LerListaTab()
    {
        string log4 = "Resgatando Lista de extracao";
        logging += "<br>" + log4;
        Console.WriteLine(log4);
        return Buscador("SELECT * FROM PROTH_EXTLIST;", null, null, null, null, _connectionStringDestination);
    }

    private DataTable LerDadosTab(InfoTab infoTab, string consultaTotal, string consultaIncremental)
    {
        if (infoTab.NomeTab == "")
            throw new Exception("Sem entrada de dados para leitura!");

        if (infoTab.TipoTab == "T" || ContaLinhas($"PROTH_{infoTab.NomeTab}") == 0)
        {
            return Buscador(consultaTotal, infoTab.NomeTab, null, null, PROTHEUS_TOTAL, _connectionStringOrigin);
        }
        else
        {
            return Buscador(consultaIncremental, infoTab.NomeTab, infoTab.ValorIncremental, infoTab.NomeCol, PROTHEUS_INC, _connectionStringOrigin);
        }
    }

    private DataTable Buscador(string consulta, string? NomeTab, int? ValorIncremental, string? NomeCol, int? Tipo, string conStr)
    {
        string log5 = "Abrindo conexao com servidor de origem...";
        logging += "<br>" + log5;
        Console.WriteLine(log5);

        using SqlConnection connection = new(conStr);
        connection.Open();
        Console.WriteLine($"Conectado.");
        SqlCommand comando = new(consulta, connection);
        DataTable dados = new() { TableName = $"PROTH_{NomeTab}" };
        string log6 = NomeTab == null ? $"Armazenando lista de execucao em memoria..." : $"Criando tabela em memoria: {dados.TableName}...";

        logging += "<br>" + log6;
        Console.WriteLine(log6);

        switch (Tipo)
        {
            case PROTHEUS_TOTAL:
                comando.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                dados.ExtendedProperties.Add("Tipo", Tipo);
                break;
            case PROTHEUS_INC:
                comando.Parameters.AddWithValue("@TABELA_PROTHEUS", NomeTab);
                comando.Parameters.AddWithValue("@VL_CORTE", ValorIncremental.ToString());
                comando.Parameters.AddWithValue("@COL_DT", NomeCol);
                dados.ExtendedProperties.Add("Tipo", Tipo);
                dados.ExtendedProperties.Add("Col", NomeCol);
                dados.ExtendedProperties.Add("Corte", ValorIncremental);
                string log7 = $"Adicionado os parametros Tipo:{dados.ExtendedProperties["Tipo"]}, Coluna:{dados.ExtendedProperties["Col"]}, Corte:{dados.ExtendedProperties["Corte"]}";
                
                logging += "<br>" + log7;
                Console.WriteLine(log7);
                break;
            default:
                break;
        }


        comando.CommandTimeout = 1000;
        SqlDataAdapter adapter = new(comando);
        adapter.Fill(dados);

        connection.Close();
        return dados;
    }

    private void InserirDadosBulk(DataTable dados)
    {
        using SqlConnection connection = new(_connectionStringDestination);
        connection.Open();
        int? linhas = ContaLinhas(dados.TableName);
        int tipo = Convert.ToInt16(dados.ExtendedProperties["Tipo"]?.ToString() ?? "");

        SqlCommand command = new()
        {
            CommandTimeout = 100,
            Connection = connection
        };

        switch ((linhas, tipo))
        {
            case (> 0, PROTHEUS_INC):
                string log8 = $"Excluindo linhas recentes da tabela {dados.TableName}...";
                logging += "<br>" + log8;
                Console.WriteLine(log8);
                command.CommandText = $"DELETE FROM {dados.TableName} WHERE {dados.ExtendedProperties["Col"]} >= GETDATE() - {dados.ExtendedProperties["Corte"]};";
                command.ExecuteNonQuery();
                break;
            case (_, PROTHEUS_TOTAL):
                string log9 = $"Truncando tabela {dados.TableName}...";
                logging += "<br>" + log9;
                Console.WriteLine(log9);
                command.CommandText = $"TRUNCATE TABLE {dados.TableName}";
                command.ExecuteNonQuery();
                break;
        }

        using SqlBulkCopy bulkCopy = new(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction, null);
        bulkCopy.BulkCopyTimeout = 1000;
        bulkCopy.DestinationTableName = dados.TableName;
        bulkCopy.WriteToServer(dados);
        connection.Close();
    }

    private int? ContaLinhas(string NomeTab)
    {
        using SqlConnection connection = new(_connectionStringDestination);
        connection.Open();

        using SqlCommand command = new($"SELECT COUNT(1) FROM {NomeTab} WITH(NOLOCK);", connection);
        command.CommandTimeout = 100;
        var exec = command.ExecuteScalar();

        int? count = Convert.ToInt32(exec == DBNull.Value ? 0 : exec);
        connection.Close();
        return count;
    }
}