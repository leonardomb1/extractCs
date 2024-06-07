namespace IntegraCs;
public class AgendaInfo
{
    public required Action<int> Action {get; set;}
    public required TimeSpan Tempo {get; set;}
    public required bool IsRunning {get; set;}
}