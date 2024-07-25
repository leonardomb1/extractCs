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
        HttpHost host = new(_connectionStringOrquest, _connectionStringDW, _tamPacote);
        host.Run();
    }
    public static void Main()
    {  
        var program = new Program();
        program.Run();
    }
}
