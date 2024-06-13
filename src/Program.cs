namespace IntegraCs;

public class Program
{
    private string _connectionStringOrquest;
    private string _connectionStringDW;
    private int _tamPacote;
    public Program()
    {
        var configVariables = new Dictionary<string, string>
        {
            { "PACKET_SIZE", Environment.GetEnvironmentVariable("PACKET_SIZE")?? "n/a" },
            { "DW_CONNECTIONSTRING", Environment.GetEnvironmentVariable("DW_CONNECTIONSTRING")?? "n/a" },
            { "ORQUEST_CONNECTIONSTRING", Environment.GetEnvironmentVariable("ORQUEST_CONNECTIONSTRING")?? "n/a" }
        };

        var anyConfigNotSet = configVariables.Any(variable => variable.Value == "n/a");

        if (anyConfigNotSet)
        {
            throw new Exception("Não Configurado!");
        }

        _tamPacote = int.Parse(configVariables["PACKET_SIZE"]);
        _connectionStringDW = configVariables["DW_CONNECTIONSTRING"];
        _connectionStringOrquest = configVariables["ORQUEST_CONNECTIONSTRING"];
    }
    public void Run()
    {
        Agenda agenda = new();
        agenda.Agendador(
            _connectionStringOrquest, // Orquestrador
            _connectionStringDW, // Datawarehouse
            _tamPacote
        );
    }
    public static void Main()
    {  
        Program runnable = new();
        runnable.Run();
    }
}
