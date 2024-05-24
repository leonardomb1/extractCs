using System.Configuration;
using System.Data;
using System.Reactive.Linq;
using Microsoft.Data.SqlClient;

namespace IntegraCs;

public class AgendaInfo
{
    public required Action<int> Action {get; set;}
    public required TimeSpan Tempo {get; set;}
    public required bool IsRunning {get; set;}
}

public class Agenda
{
    private string _con;
    public Agenda()
    {
        _con = ConfigurationManager.ConnectionStrings["DataWarehouse"].ConnectionString;
    }
    private DataTable GetAgenda()
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

    public void Agendador()
    {
        DataTable agenda = GetAgenda();
        Dictionary<int, AgendaInfo> agendas = [];

        Console.WriteLine("Instanciando agenciador...");
        Console.WriteLine("Pressione qualquer tecla para parar...");

        foreach(DataRow row in agenda.Rows)
        {
            Console.WriteLine($"Carregando agenda: {row.Field<string>("NM_AGENDA")}...");
            int id = row.Field<int>("ID_DW_AGENDADOR");
            agendas[id] = new AgendaInfo
            {
                Action = new Action<int>(async (_) => 
                {
                    if (agendas[id].IsRunning) return;

                    TransferenciaDados dados = new();
                    Console.WriteLine($"Executando agenda: {row.Field<string>("NM_AGENDA")}...");
                    agendas[id].IsRunning = true;
                    await dados.Transferir(id);
                    agendas[id].IsRunning = false;
                    dados.Dispose();
                }),
                Tempo = TimeSpan.FromSeconds(row.Field<int>("VL_RECORRENCIA")),
                IsRunning = false
            };
        }

        List<IDisposable> subscriptions = [];

        foreach (var actionEntry in agendas)
        {
            if (!actionEntry.Value.IsRunning) 
            {
                var subscription = 
                    Observable.Interval(actionEntry.Value.Tempo)
                        .Subscribe(_ =>
                        {
                            actionEntry.Value.Action.Invoke(actionEntry.Key.GetHashCode());
                        });
                subscriptions.Add(subscription);
            }
        }

        Console.In.ReadLine();

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }
    }
}