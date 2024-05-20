using System.Configuration;
using System.Data;
using System.Reactive.Linq;
using Microsoft.Data.SqlClient;

namespace IntegraCs;

public class Agenda
{
    private static string _con = "";
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

    public void Agendador()
    {
        DataTable agenda = GetAgenda();
        Dictionary<Action<int>, TimeSpan> agendas = [];

        Console.WriteLine("Instanciando agenciador...");
        Console.WriteLine("Pressione qualquer tecla para parar...");

        foreach(DataRow row in agenda.Rows)
        {
            Console.WriteLine($"Carregando agenda: {row.Field<string>("NM_AGENDA")}...");
            agendas.Add(
                new Action<int>(async (_) => 
                {
                    Console.WriteLine($"Executando agenda: {row.Field<string>("NM_AGENDA")}...");
                    await new TransferenciaDados().Transferir(row.Field<int>("ID_DW_AGENDADOR"));
                }),
                TimeSpan.FromSeconds(row.Field<int>("VL_RECORRENCIA"))
            );
        }

        List<IDisposable> subscriptions = [];

        foreach (var actionEntry in agendas)
        {
            var subscription = Observable.Interval(actionEntry.Value)
                                        .Subscribe(_ =>
                                        {
                                            actionEntry.Key.Invoke(actionEntry.Key.GetHashCode());
                                        });
            subscriptions.Add(subscription);
        }

        Console.ReadKey();

        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }
    }
}