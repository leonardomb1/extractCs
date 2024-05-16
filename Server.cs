using System.Net;
using System.Text;

namespace IntegraCs;
public class Servidor
{
    private static HttpListener? listener;
    private static string url = "http://localhost:14000/";
    private const int SUCESSO = 0;
    private static int pageViews = 0;
    private static int requestCount = 0;
    private static int sucessCount = 0;
    private static string pageData = 
        "<!DOCTYPE>" +
            "<html>" +
            "  <head>" +
            "    <title>Extrator de dados</title>" +
            "  </head>" +
            "  <body>" +
            "    <p>Page Views: {0}</p>" +
            "    <form method=\"post\" action=\"shutdown\">" +
            "      <input type=\"submit\" value=\"Desligar\" {1}>" +
            "    </form>" +
            "    <form method=\"post\" action=\"extract\">" +
            "      <input type=\"submit\" value=\"Extrair\" {2}>" +
            "    </form>" +
            "    <p>{3}</p>" +
            "  </body>" +
            "</html>";


    private static async Task HandleIncomingConnections(TransferenciaDados puxarProtheus)
    {
        bool runServer = true;
        bool extracting = false;

        while (runServer)
        {
            HttpListenerContext ctx = await listener.GetContextAsync();

            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            Console.WriteLine($"Request #: {++requestCount}");
            Console.WriteLine(req.Url?.ToString());
            Console.WriteLine(req.HttpMethod);
            Console.WriteLine(req.UserHostName);
            Console.WriteLine(req.UserAgent);
            Console.WriteLine();

            if ((req.HttpMethod == "POST") && (req?.Url?.AbsolutePath == "/shutdown"))
            {
                Console.WriteLine("Shutdown requested");
                runServer = false;
            }

            if (req.Url?.AbsolutePath != "/favicon.ico")
                pageViews += 1;

            if ((req.HttpMethod == "POST") && (req?.Url?.AbsolutePath == "/extract"))
            {
                Console.WriteLine("Recebido pedido de execucao...");
                puxarProtheus.logging = "";
                int exec = 5;
                try
                {
                    exec = puxarProtheus.Transferir().Result;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro de execução na transferência de dados: {ex.Message}, {ex.InnerException}");
                    File.AppendAllText(@$".\log\log_{pageViews}.txt", "\nExecução: " + DateTime.Now.ToString() + "\n" + puxarProtheus.logging + "\nErro:\n" + ex.ToString());
                }

                if (exec == SUCESSO)
                {
                    Console.WriteLine("Executado com sucesso.");
                    File.AppendAllText(@$".\log\log_{pageViews}.txt", "\nExecução: " + DateTime.Now.ToString() + "\n" + puxarProtheus.logging);
                    sucessCount++;
                }
            }

            string disableSubmit = !runServer ? "disabled" : "";
            string extractSubmit = !extracting ? "" : "Extraindo";
            byte[] data = Encoding.UTF8.GetBytes(string.Format(pageData, pageViews, disableSubmit, extractSubmit, puxarProtheus.logging));
            resp.ContentType = "text/html";
            resp.ContentEncoding = Encoding.UTF8;
            resp.ContentLength64 = data.LongLength;

            await resp.OutputStream.WriteAsync(data);
            resp.Close();
        }
    }

    public void Servico()
    {
        listener = new HttpListener();
        listener.Prefixes.Add(url);
        TransferenciaDados puxarProtheus = new();
        listener.Start();
        Console.WriteLine($"Escutando em {url}");

        Task listenTask = HandleIncomingConnections(puxarProtheus);
        listenTask.GetAwaiter().GetResult();

        listener.Close();
    }
}