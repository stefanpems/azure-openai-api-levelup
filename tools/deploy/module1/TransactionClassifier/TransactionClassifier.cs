// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.IO;
using CsvHelper;
using System.Linq;
using Azure.AI.OpenAI;
using Azure;
using static System.Environment;

namespace TransactionClassification
{
    public static class TransactionClassifier
    {

        private const string prompt = @"Sei un esperto di Microsoft Defender for Cloud Apps.
            Devi rispondere alla domanda di un cliente che chiede se il prodotto offre determinate capacità.
            Laddove la risposta non fosse completamente positiva, proponi soluzioni con l'integrazione di altri prodotti Microsoft. 
            Tieni un tono ufficiale ed impersonale, come per rispondere ad una domanda di una RFI.
            Scrivi due righe per giustificare la risposta. 
            Dove possibile, aggiungi link alla documentazione del prodotto in cui trovare dettagli su quanto affermi.

            Domanda del cliente: CUSTOMER_QUESTION

            La risposta è: ";

        [FunctionName("TransactionClassifier")]
        public static void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation("-----------------------------\n\n  Start execution - Run!\n-----------------------------");
            
            // Get the blob information from the event grid event.
            var data = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();
            // retrieve continer name from URL field ("https://egblobstore.blob.core.windows.net/{containername}/blobname.jpg") of the event grid event
            log.LogInformation("-----------------------------\n\n  Run - Debug #1\n-----------------------------");
            var containerName = data.Url.Split('/')[3];
            log.LogInformation("-----------------------------\n\n  Run - Debug #2: " + containerName + "\n-----------------------------");
            // retrieve blob name from URL field ("https://egblobstore.blob.core.windows.net/containername/{blobname}.jpg") of the event grid event
            var blobName = data.Url.Split('/')[4]; 
            log.LogInformation("-----------------------------\n\n  Run - Debug #3: " + blobName + "\n-----------------------------");
            
            var connectionString = GetEnvironmentVariable("STORAGE_ACCOUNT_CONNECTION_STRING");
            log.LogInformation("-----------------------------\n\n  Run - Debug #4: " + connectionString + "\n-----------------------------");
            
                
            // Retrieve the blob from the storage account.
            var blobClient = new Azure.Storage.Blobs.BlobClient(connectionString, containerName, blobName);
            var blobContent = blobClient.DownloadContent();
            log.LogInformation("-----------------------------\n\n  Run - Debug #5\n-----------------------------");
            
            // convert from system.binary to system.io.stream
            var stream = new MemoryStream(blobContent.Value.Content.ToArray());
            log.LogInformation("-----------------------------\n\n  Run - Debug #6\n-----------------------------");
            

            // set up connection to Azure OpenAI API client
            string endpoint = GetEnvironmentVariable("OPENAI_API_BASE");
            string key = GetEnvironmentVariable("OPENAI_API_KEY");           
            OpenAIClient client = new OpenAIClient(new Uri(endpoint), new AzureKeyCredential(key));

            // read content of uploaded csv file
            log.LogInformation("-----------------------------\n\n  Run - About to get records from CSV!\n-----------------------------");
            using var reader = new StreamReader(stream);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<dynamic>().ToList();

            string prompt = TransactionClassifier.prompt;

            // for each csv record, set value in the column named "classification" to response from Azure OpenAI API Completion API
            // for each csv record, set value in the column named "Answer" to response from Azure OpenAI API Completion API
            log.LogInformation("-----------------------------\n\n  Run - About to loop the input rows!\n-----------------------------");
            int rn=0;
            foreach (var record in records)
            {
                /*
                string classification = ClassifyTransaction(record, prompt, client);
                
                record.classification = classification;
                */
                rn++;
                log.LogInformation("-----------------------------\n\n  Run - row: " +rn.ToString() + " \n-----------------------------");

                string answer = ClassifyTransaction(record, prompt, client, log);
                
                record.Answer = answer;
            }
            
            // create a new file to store updated csv file
            log.LogInformation("-----------------------------\n\n  Run - About to write the outputs!\n-----------------------------");
            using (var writer = new StringWriter())
            {
                using (var acsv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    acsv.WriteRecords(records);

                    String str1 = writer.ToString();
                }

                // convert StringWriter to a Stream so it could be uploaded via BlobClient
                log.LogInformation("-----------------------------\n\n  Run - About to upload the output file!\n-----------------------------");
                var stream2 = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(writer.ToString()));
                var outputBlobClient = new Azure.Storage.Blobs.BlobClient(connectionString, "output", blobName);
                outputBlobClient.Upload(stream2);
            }
            

            log.LogInformation("-----------------------------\n\n  Run - END!!!!!\n-----------------------------");
                
        }
                
        private static string ClassifyTransaction(dynamic transaction, string prompt, OpenAIClient client, ILogger log)
        {
            /*
            prompt = prompt.Replace("SUPPLIER_NAME", transaction.Supplier);
            prompt = prompt.Replace("DESCRIPTION_TEXT", transaction.Description);
            prompt = prompt.Replace("TRANSACTION_VALUE", transaction.Transaction_value);
            */
            
            prompt = prompt.Replace("CUSTOMER_QUESTION", transaction.Question);

            log.LogInformation("---------------------------------------\n\n  ClassifyTransaction - Prompt:\n---------------------------------------");
            log.LogInformation(prompt);
            log.LogInformation("---------------------------------------");
            
            string engine = GetEnvironmentVariable("OPENAI_API_MODEL");

            // call the Azure OpenAI API Completion API
            string completion = string.Empty;
            try{
                Response<Completions> completionsResponse = client.GetCompletions(engine, prompt);
                completion = completionsResponse.Value.Choices[0].Text;
                log.LogInformation("-------------------------------------------------\n\n  ClassifyTransaction - Completion:\n-------------------------------------------------");
                log.LogInformation(completion);
                log.LogInformation("-------------------------------------------------");
            }
            catch(Exception ex){
                log.LogError("-----------------------------\n\n  ClassifyTransaction - Error! \n" + ex.ToString() + "\n-----------------------------");
                completion = string.Concat("ERROR: ", ex.Message);
                log.LogInformation("-----------------------------");
                //do not throw(ex);
            }
            
            return completion;
        }

    }

}
