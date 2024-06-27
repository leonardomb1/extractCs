namespace IntegraCs;

public class Program
{
    // private string _connectionStringOrquest;
    // private string _connectionStringDW;
    // private int _tamPacote;
    // private int _sistema;
    // public Program()
    // {
    //     var configVariables = new Dictionary<string, string>
    //     {
    //         { "PACKET_SIZE", Environment.GetEnvironmentVariable("PACKET_SIZE")?? "n/a" },
    //         { "DW_CONNECTIONSTRING", Environment.GetEnvironmentVariable("DW_CONNECTIONSTRING")?? "n/a" },
    //         { "ORQUEST_CONNECTIONSTRING", Environment.GetEnvironmentVariable("ORQUEST_CONNECTIONSTRING")?? "n/a" },
    //         { "QUERY_SYSTEMID", Environment.GetEnvironmentVariable("QUERY_SYSTEMID")?? "n/a" }
    //     };

    //     var anyConfigNotSet = configVariables.Any(variable => variable.Value == "n/a");

    //     if (anyConfigNotSet)
    //     {
    //         throw new Exception("Não Configurado!");
    //     }

    //     _tamPacote = int.Parse(configVariables["PACKET_SIZE"]);
    //     _connectionStringDW = configVariables["DW_CONNECTIONSTRING"];
    //     _connectionStringOrquest = configVariables["ORQUEST_CONNECTIONSTRING"];
    //     _sistema = int.Parse(configVariables["QUERY_SYSTEMID"]);
    // }
    public void Run()
    {
        Agenda agenda = new();
        agenda.Agendador(
            @"Data Source=KAERIPINDSQLSRV\DW;Database=DWController;User Id=integrador;Password=pALeN2BzbrgQ3Z5n;TrustServerCertificate=True;Encrypt=False;Packet Size=32767;Pooling=True;Max Pool Size=500;MultipleActiveResultSets=True;Connect Timeout=500;", // Orquestrador
            @"Data Source=KAERIPINDSQLSRV\DW;Database=DWExtract;User Id=integrador;Password=pALeN2BzbrgQ3Z5n;TrustServerCertificate=True;Encrypt=False;Packet Size=32767;Pooling=True;Max Pool Size=500;MultipleActiveResultSets=True;Connect Timeout=500;", // Datawarehouse
            103000, // Tamanho do pacote
            3 // Sistema de extração
        );         
    }
    public static void Main()
    {  
        Program runnable = new();
        runnable.Run();
    }
}
