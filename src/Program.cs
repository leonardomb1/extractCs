namespace IntegraCs;

public class Program
{
    private string _connectionStringDW;
    private int _tamPacote;
    public Program()
    {
        var configPacote = Environment.GetEnvironmentVariable("PACKET_SIZE") ?? "n/a";
        var configDW = Environment.GetEnvironmentVariable("DW_CONNECTIONSTRING") ?? "n/a"; 
        if(configPacote == "n/a" || configDW == "n/a") 
        {
            Console.WriteLine("Não Configurado."); 
            throw new Exception("Não Configurado!");
        }
        _tamPacote = int.Parse(configPacote); 
        _connectionStringDW = configDW; 
    }
    public void Run()
    {
        Agenda agenda = new();
        agenda.Agendador(_connectionStringDW, _tamPacote);
    }
    public static void Main()
    {  
        Program runnable = new();
        runnable.Run();
    }
}
