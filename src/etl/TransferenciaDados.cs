using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;
public class TransferenciaDados : IDisposable
{
    private bool _disposed = false;
    private string _connectionStringDW;
    private string _connectionStringOrquest;
    private int _packetSize;
    private DataTable _tabelaConsultas;
    private DataTable _tabelaExec;
    private List<ConsultaInfo> _consultas = []; 

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (!_disposed) {
            if (disposing) {
                Dispose();
                _tabelaConsultas.Dispose();
                _tabelaExec.Dispose();
                _consultas.Clear();
            }
        }
        _disposed = true;
    }
    public TransferenciaDados(
        string orquestConStr,
        string dataWarehouseConStr,
        int packetSize)
    {
        _connectionStringOrquest = orquestConStr;
        _connectionStringDW = dataWarehouseConStr;
        _packetSize = packetSize;
        _tabelaConsultas = ComControlador.BuscaDados(
            @$"SELECT * FROM DW_CONSULTA",
            orquestConStr
        );
        _tabelaExec = ComControlador.BuscaDados(
            @$"SELECT 
                *
            FROM DW_EXTLIST AS LIS WITH(NOLOCK)
            INNER JOIN DW_SISTEMAS AS SIS WITH(NOLOCK)
                ON  SIS.ID_DW_SISTEMA = LIS.ID_DW_SISTEMA;",
                orquestConStr,
                "DWController"
        );

        foreach (DataRow lin in _tabelaConsultas.Rows)
        {
            _consultas.Add(
                new ConsultaInfo(
                    lin.Field<int>("ID_DW_SISTEMA"),
                    lin.Field<string>("TP_CONSULTA") ?? "N/A",
                    lin.Field<string>("DS_CONSULTA") ?? "N/A"
                )
            );
        }
    }
    ~TransferenciaDados()
    {
        Dispose(disposing: false);
    }

    public async Task Transferir(int agenda)
    {
        List<Task> tarefas = [];
        int idExecAtual = 0;

        foreach(DataRow linExt in _tabelaExec.Rows)
        {
            int? corte = linExt.Field<int?>("VL_INC_TABELA");
            string? coluna = linExt.Field<string?>("NM_COLUNA");
            int idSistema =  linExt.Field<int>("ID_DW_SISTEMA");
            int idTabela = linExt.Field<int>("ID_DW_EXTLIST");
            string tabela = linExt.Field<string>("NM_TABELA") ?? "N/A";
            string conStr = linExt.Field<string>("DS_CONSTRING") ?? "N/A";  
            string sistema = linExt.Field<string>("NM_SISTEMA") ?? "N/A";
            string tipoTabela = linExt.Field<string>("TP_TABELA") ?? "N/A";

            int idExec = ComControlador.BuscaIdExec(
                _connectionStringOrquest,
                agenda
            ); // Resgata id de execução
            
            await ComControlador.Log(
                idExec,
                LogInfo.INIC_LIMPA_TABELA,
                $"Limpando Tabela: {sistema}.{tabela}...",
                _connectionStringOrquest
            );
            
            ComExtract.LimpaTabela(
                tabela,
                _connectionStringDW,
                coluna,
                tipoTabela,
                sistema,
                corte
            );

            tarefas.Add(Task.Run(async () => {
                try
                {
                    await BuscadorPacotes(
                        idExec,
                        tipoTabela,
                        idTabela,
                        idSistema,
                        sistema,
                        conStr,
                        _connectionStringOrquest,
                        _packetSize,
                        _consultas,
                        tabela,
                        corte,
                        coluna
                    );

                    await ComControlador.Log(
                        idExec,
                        LogInfo.FINAL_LEITURA_PACOTE,
                        $"Concluído extração para: {tabela}",
                        _connectionStringOrquest,
                        ConstInfo.SUCESSO,
                        idTabela
                    );
                }
                catch (Exception ex)
                {
                    await ComControlador.Log(
                        idExec,
                        LogInfo.FINAL_LEITURA_PACOTE,
                        $"Erro na extração para: {tabela}, com erro : {ex}",
                        _connectionStringOrquest,
                        ConstInfo.FALHA,
                        idTabela
                    );
                }
            }));
        }
        await Task.WhenAll(tarefas);
    
        ComControlador.AtualizaTempoFinalExe(
            _connectionStringOrquest,
            idExecAtual,
            ConstInfo.SUCESSO
        );

        await ComControlador.Log(
            idExecAtual,
            LogInfo.LIBERA_RECURSO,
            "Liberando Recursos...",
            _connectionStringOrquest
        );

        tarefas.ForEach(t => t.Dispose());
        tarefas.Clear();
    }

    private static async Task BuscadorPacotes(int exec,
                                       string Tipo,
                                       int idTabela,
                                       int idSistema,
                                       string sistema,
                                       string conStr,
                                       string conStrDw,
                                       int packetSize,
                                       List<ConsultaInfo> infoConsulta,
                                       string NomeTab,
                                       int? ValorIncremental,
                                       string? NomeCol)
    {     
        using SqlConnection connection = new() {
            ConnectionString = conStr,
        };

        connection.Open();

        string consulta = "";

        int? linhas = 
            ComExtract.ContaLinhas(
                $"{sistema}.{NomeTab}",
                conStrDw
            );

        try
        {
            await ComControlador.Log(
                exec,
                LogInfo.INIC_SQL,
                $"Criando tabela temporaria: ##T_{NomeTab}_DW_SEL...",
                conStrDw,
                ConstInfo.SUCESSO,
                idTabela
            );

            using SqlCommand criarTabelaTemp = new() {
                Connection = connection,
                CommandTimeout = 3600
            };

            switch ((linhas, Tipo))
            {
                case (_, ConstInfo.TOTAL):
                    consulta =  infoConsulta
                        .Where(x => x.SistemaTipo == idSistema && x.ConsultaTipo == ConstInfo.TOTAL)
                        .Select(x => x.Consulta)
                        .FirstOrDefault() ?? "N/A";

                    await ComControlador.Log(
                        exec,
                        LogInfo.ABRIR_CONEXAO,
                        $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...",
                        conStrDw,
                        ConstInfo.SUCESSO,
                        idTabela
                    );
                    
                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    await criarTabelaTemp.ExecuteNonQueryAsync();
                    break;
                case (0, ConstInfo.INCREMENTAL):
                    consulta =  infoConsulta
                        .Where(x => x.SistemaTipo == idSistema && x.ConsultaTipo == ConstInfo.TOTAL)
                        .Select(x => x.Consulta)
                        .FirstOrDefault() ?? "N/A";
                    
                    await ComControlador.Log(
                        exec,
                        LogInfo.ABRIR_CONEXAO,
                        $"Conexão aberta para extração do tipo Total da tabela: {NomeTab}...",
                        conStrDw,
                        ConstInfo.SUCESSO,
                        idTabela
                    );
                    
                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    await criarTabelaTemp.ExecuteNonQueryAsync();
                    break;
                case (> 0, ConstInfo.INCREMENTAL):
                    consulta =  infoConsulta
                        .Where(x => x.SistemaTipo == idSistema && x.ConsultaTipo == ConstInfo.INCREMENTAL)
                        .Select(x => x.Consulta)
                        .FirstOrDefault() ?? "N/A";

                    await ComControlador.Log(
                        exec,
                        LogInfo.ABRIR_CONEXAO,
                        $"Conexão aberta para extração do tipo Incremental da tabela: {NomeTab}...",
                        conStrDw,
                        ConstInfo.SUCESSO,
                        idTabela
                    );

                    criarTabelaTemp.CommandText = consulta;
                    criarTabelaTemp.Parameters.AddWithValue("@TABELA", NomeTab);
                    criarTabelaTemp.Parameters.AddWithValue("@VL_CORTE", ValorIncremental.ToString());
                    criarTabelaTemp.Parameters.AddWithValue("@COL_DT", NomeCol);
                    await criarTabelaTemp.ExecuteNonQueryAsync();
                    break;
                default:
                    await ComControlador.Log(
                        exec,
                        LogInfo.ABRIR_CONEXAO,
                        $"Conexão Aberta, mas sem tipo definido para a tabela: {NomeTab}...",
                        conStrDw,
                        ConstInfo.FALHA,
                        idTabela
                    );
                    break;
            }
        }
        catch (SqlException ex)
        {
            await ComControlador.Log(
                exec,
                LogInfo.ERRO_SQL,
                $"Erro SQL: na tabela {NomeTab} ao tentar criar tabela temporária. {ex}",
                conStrDw,
                ConstInfo.FALHA,
                idTabela
            );
        }

        using SqlCommand consultarTabelaTemp = new() {
            Connection = connection,
            CommandText = $"SELECT *, GETDATE() FROM ##T_{NomeTab}_DW_SEL WITH(NOLOCK);",
            CommandTimeout = 6000
        };

        using SqlCommand deletarTabelaTemp = new() {
            Connection = connection,
            CommandText = $"DROP TABLE IF EXISTS ##T_{NomeTab}_DW_SEL;",
            CommandTimeout = 100
        };

        DataTable pacote = new() { TableName = $"{sistema}.{NomeTab}" };
        await ComControlador.Log(
            exec,
            LogInfo.INIC_SQL,
            $"Iniciando consulta da tabela: {NomeTab}...",
            conStrDw,
            ConstInfo.SUCESSO,
            idTabela
        );
        
        using SqlDataReader reader = consultarTabelaTemp.ExecuteReader();
        
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

                if (pacote.Rows.Count >= packetSize)
                {
                    await ComControlador.Log(
                        exec,
                        LogInfo.INIC_INSERT_BULK,
                        $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count} Linhas.",
                        conStrDw,
                        ConstInfo.SUCESSO,
                        idTabela
                    );

                    await ComExtract.InserirDadosBulk(pacote, conStrDw);
                    pacote.Clear();
                    await ComControlador.Log(
                        exec,
                        LogInfo.INIC_INSERT_BULK,
                        $"Finalizado BULK Insert da tabela: {NomeTab}",
                        conStrDw,
                        ConstInfo.SUCESSO,
                        idTabela
                    );
                }
            }

            if (pacote.Rows.Count > 0)
            {
                await ComControlador.Log(
                    exec,
                    LogInfo.INIC_INSERT_BULK,
                    $"Iniciando BULK Insert da tabela: {NomeTab} com {pacote.Rows.Count}",
                    conStrDw,
                    ConstInfo.SUCESSO,
                    idTabela
                );

                await ComExtract.InserirDadosBulk(pacote, conStrDw);
                await ComControlador.Log(
                    exec,
                    LogInfo.INIC_INSERT_BULK,
                    $"Finalizado BULK Insert da tabela: {NomeTab}",
                    conStrDw,
                    ConstInfo.SUCESSO,
                    idTabela
                );
            }


        }
        catch (SqlException ex)
        {
            await ComControlador.Log(
                exec,
                LogInfo.ERRO_SQL,
                $"Erro SQL: {ex} na tabela {NomeTab} ao tentar executar inserção de dados",
                conStrDw,
                ConstInfo.FALHA,
                idTabela
            );
        }
        finally
        {
            if (reader!= null &&!reader.IsClosed)
            {
                reader.Close();
            }
        }

        await ComControlador.Log(
            exec,
            LogInfo.INIC_SQL,
            $"Deletando tabela temporaria: ##T_{NomeTab}_DW_SEL...",
            conStrDw,
            ConstInfo.SUCESSO,
            idTabela
        );

        pacote.Dispose();
        reader.Dispose();

        deletarTabelaTemp.ExecuteNonQuery();
        connection.Close();
        connection.Dispose();
    }
}
