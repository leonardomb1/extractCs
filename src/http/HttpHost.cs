using System.Net;
using System.Text;

namespace IntegraCs;

public class HttpHost
{
    private HttpListener _listener;
    private const int PORT = 8080;
    private const string IP = "*";
    private string _connectionStringDW;
    private string _connectionStringOrquest;
    private int _packetSize;
    public HttpHost(string orquestConStr, string dataWarehouseConStr, int packetSize) 
    {
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://{IP}:{PORT}/api/execute/");

        _connectionStringDW = dataWarehouseConStr;
        _connectionStringOrquest = orquestConStr;
        _packetSize = packetSize;
    }

    public void Run()
    {
        _listener.Start();
        Console.WriteLine($"Escutando em {IP}:{PORT}");
        while (true)
        {
            HttpListenerContext context = _listener.GetContext();
            HttpListenerRequest req = context.Request;
            HttpListenerResponse res = context.Response;
         
            string agenda = req.Headers.GetValues("agenda")!.FirstOrDefault() ?? "n/a";
            string sistema = req.Headers.GetValues("sistema")!.FirstOrDefault() ?? "n/a";
            
            int idAgenda = int.Parse(agenda);
            int idSistema = int.Parse(sistema);

            if (sistema == "n/a" || agenda == "n/a") RespGetById(res, true, "Not Found", 404);

            switch (req.HttpMethod)
            {       
                case "GET":
                    try
                    {
                        Task runner = Task.Run(async () => {
                            TransferenciaDados dados = new
                            (   
                                _connectionStringOrquest,
                                _connectionStringDW,
                                _packetSize,
                                idAgenda,
                                idSistema
                            );

                            await dados.Transferir(idAgenda);
                            dados.Dispose();
                        });
                        RespGetById(res, false, "OK", 200);
                    }
                    catch (Exception)
                    {
                        RespGetById(res, true, "Internal Server Error", 500);
                    }
                    break;
                default:
                    RespGetById(res, true, "Not Found", 404);
                       break;
            }
        }
    }
    
    private static void RespGetById(HttpListenerResponse res, bool err, string status, int code)
    {
        byte[] buffer;

        buffer = Encoding.UTF8.GetBytes(
            $"{{ \"status\": \"{status}\", \"error\": {err}}}"
        );
        res.ContentLength64 = buffer.Length;
        res.StatusCode = code;

        Stream output = res.OutputStream;

        output.Write(buffer, 0, buffer.Length);
        output.Close();
    }

}