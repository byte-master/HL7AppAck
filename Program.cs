using System.Configuration;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Azure.Storage.Queues; 
using QueueMessage = Azure.Storage.Queues.Models.QueueMessage;




string QueueName = ConfigurationManager.AppSettings["QueueName"];
ProcessMessages(QueueName);

void log(string txt)
{
    StreamWriter sw;
    string path = "LogOutput.txt";
    // This text is added only once to the file.
    if (!File.Exists(path))
    {
        // Create a file to write to.
        sw = File.CreateText(path);
    }
    else
    {
        sw = File.AppendText(path);
    }

    sw.WriteLine(DateTime.Now.ToString()+": "+ txt);
    sw.Close();
}

    void ProcessMessages(string queueName)
{
    
    // Get the connection string from app settings
    string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

    // Instantiate a QueueClient which will be used to manipulate the queue
    QueueClient queueClient = new QueueClient(connectionString, queueName);
    log("Connected to storage");
    if (queueClient.Exists())
    {
        log("Queue Exsists");
        QueueMessage[] retrievedMessage = queueClient.ReceiveMessages();
        while (retrievedMessage.Length>0)
        {
            foreach (QueueMessage m in retrievedMessage)
            {
                bool ret = false;
                //var m = queueClient.ReceiveMessage();
                string tmp = m.Body.ToString();
                var t = Convert.FromBase64String(tmp);
                string str = Encoding.Default.GetString(t);
                log("Retrieved Message: "+str);

                if (SendAppAck(str))
                {
                    log("Message AppAck sent");
                    queueClient.DeleteMessage(retrievedMessage[0].MessageId, retrievedMessage[0].PopReceipt); Console.WriteLine(str);
                    log(str + " removed from queue");
                }
                else
                {
                    log("Message AppAck failed to send");
                    Console.WriteLine(str + " Send failed");
                }
                

            }
            retrievedMessage = queueClient.ReceiveMessages();
        }
    }

    bool SendAppAck(string message)
    {
        string url = ConfigurationManager.AppSettings["MPIEndPoint"];
        string port = ConfigurationManager.AppSettings["MPIPort"];
        int returncode = 0;
        bool ret = false;
        try
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(url + ":" + port);
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("data", message)
                });
                var result = client.PostAsync("", content).Result;
                returncode = ((int)result.StatusCode);
            }
            ret = (returncode == 200);
        }
        catch { }
        return ret;
    }
}
