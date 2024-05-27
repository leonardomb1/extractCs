namespace IntegraCs;

public class Program
{
    public static string _connectionStringDW = "";
    public static int _tamPacote = 0;
    public Program()
    {
        var configPacote = Environment.GetEnvironmentVariable("PACKET_SIZE") ?? "n/a";
        var configDW = Environment.GetEnvironmentVariable("DW_CONNECTIONSTRING") ?? "n/a"; 
        if(configPacote == "n/a" || configDW == "n/a") 
        {
            Console.WriteLine("Não Configurado."); 
            throw new Exception("Não Configurado!");
        }
        _tamPacote = int.Parse(configDW); 
        _connectionStringDW = configDW; 
    }
    public static void Main()
    {       
        Agenda agenda = new();
        agenda.Agendador();
    }
}
