using System.Data;
using System.Reactive.Linq;

namespace IntegraCs;
public class Agenda
{
    public void Agendador(string orquestConStr, string dataWarehouseConStr, int packetSize)
    {
        DataTable agenda = ComControlador.BuscaAgenda(orquestConStr);
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

                    TransferenciaDados dados = new(
                        orquestConStr,
                        dataWarehouseConStr,
                        packetSize);
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