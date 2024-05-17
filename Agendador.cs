using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;

namespace IntegraCs;

public class Agenda
{
    private static string _con;
    private List<System.Timers.Timer> timers = [];
    public Agenda()
    {
        _con = ConfigurationManager.ConnectionStrings["DataWarehouse"].ConnectionString;
    }
    private static DataTable GetAgenda()
    {
        Console.WriteLine("Resgantando agendas de execucao...");
        using SqlConnection connection = new(_con);
        connection.Open();

        using SqlCommand command = new("SELECT * FROM DW_AGENDADOR", connection);
        SqlDataAdapter adapter = new(command);
        DataTable tabela = new();
        adapter.Fill(tabela);

        Console.WriteLine("Resgatado.");
        return tabela;
    }

    private static async void OnTimedEvent(object? source, int agenda)
    {
        Console.WriteLine($"Iniciando transferencia de dados. Agenda ID : {agenda}");
        TransferenciaDados dados = new();
        await dados.Transferir(agenda);
        ((System.Timers.Timer)source).Start(); 
    }

    public void Agendador()
    {
        DataTable agenda = GetAgenda();
        Dictionary<int, TimeSpan> agendas = [];
        Console.WriteLine("Instanciando agenciador...");

        foreach(DataRow row in agenda.Rows)
        {
        Console.WriteLine($"Carregando agenda {row.Field<string>("NM_AGENDA")}...");
            agendas.Add(
                    row.Field<int>("ID_DW_AGENDADOR"),
                    TimeSpan.FromMinutes(row.Field<int>("VL_RECORRENCIA"))
                );
        }
        
        System.Timers.Timer timer = new() 
        {
            Interval = agendas.First().Value.TotalMilliseconds,
            AutoReset = false
        };

        timer.Elapsed += delegate { OnTimedEvent(timer, agendas.First().Key); };
        timer.Start();
        // foreach(KeyValuePair<int, TimeSpan> time in agendas)
        // {
        //     timer.Interval = time.Value.TotalMilliseconds,
        //     timer.AutoReset = false;
        //     timer.Elapsed += delegate { OnTimedEvent(timer, time.Key); };
        //     Console.WriteLine($"Iniciando Agenda ID : {time.Key}, com valor de : {time.Value.Minutes} min.");
        //     timer.Start();
        //     timers.Add(timer);
        // }
    }
}