namespace IntegraCs;
public class ConsultaInfo
{
    public string ConsultaTipo { get; set; }
    public int SistemaTipo { get; set; }
    public string Consulta { get; set; }

    public ConsultaInfo(int systemType, string queryType, string query)
    {
        ConsultaTipo = queryType;
        SistemaTipo = systemType;
        Consulta = query;
    }
}