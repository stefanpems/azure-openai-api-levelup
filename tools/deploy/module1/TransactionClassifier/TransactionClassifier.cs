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

        private const string promptSA = @"Sei un esperto di Microsoft Defender for Cloud Apps.
            Devi rispondere alla domanda di un cliente che chiede se il prodotto offre determinate capacità.

            La domanda del cliente fa riferimento a questo ambito funzionale: QUESTION_AREA. 

            Rispondi solo con una di queste 3 parole: Sì, No, Parzialmente. 

            Domanda del cliente: CUSTOMER_QUESTION 

            La risposta è: ";

        private const string promptLA = @"Sei un esperto di Microsoft Defender for Cloud Apps.
            Un cliente che chiede se il prodotto offre determinate capacità.
            Rispondi in due righe, dando un minimo di spiegazioni.
            Tieni un tono ufficiale ed impersonale.
            
            La domanda del cliente fa riferimento a questo ambito funzionale: QUESTION_AREA. 
             
            Domanda del cliente: CUSTOMER_QUESTION 

            La risposta è: ";

        private const string promptRL = @"Sei un esperto di Microsoft Defender for Cloud Apps.
            Un cliente che chiede se il prodotto offre determinate capacità.
            Segnala la URL della eventuale documentazione ufficiale Microsoft dove il cliente può trovare la risposta.
            Deve essere una URL realmente esistente. Se non esiste scrivi 'N.A.'. 

            La domanda del cliente fa riferimento a questo ambito funzionale: QUESTION_AREA. 

            Domanda del cliente: CUSTOMER_QUESTION 

            La URL dove trovare la risposta è: ";

        [FunctionName("TransactionClassifier")]
        public static void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation("-----------------------------\n\n  Start execution - Run!\n-----------------------------");
            
            // Get the blob information from the event grid event.
            var data = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();
            // retrieve continer name from URL field ("https://egblobstore.blob.core.windows.net/{containername}/blobname.jpg") of the event grid event
            log.LogInformation("----\n\n  Run - Debug #1\n-----------------------------");
            var containerName = data.Url.Split('/')[3];
            log.LogInformation("----\n\n  Run - Debug #2: " + containerName + "\n-----------------------------");
            // retrieve blob name from URL field ("https://egblobstore.blob.core.windows.net/containername/{blobname}.jpg") of the event grid event
            var blobName = data.Url.Split('/')[4]; 
            log.LogInformation("----\n\n  Run - Debug #3: " + blobName + "\n-----------------------------");
            
            var connectionString = GetEnvironmentVariable("STORAGE_ACCOUNT_CONNECTION_STRING");
            log.LogInformation("----\n\n  Run - Debug #4: " + connectionString + "\n-----------------------------");
            
                
            // Retrieve the blob from the storage account and read its content.
            var blobClient = new Azure.Storage.Blobs.BlobClient(connectionString, containerName, blobName);
            var blobContent = blobClient.DownloadContent();
            log.LogInformation("----\n\n  Run - Debug #5\n-----------------------------");

             // delete the processed CSV
            log.LogInformation("-----------------------------\n\n  Run - About to delete the processed input!\n-----------------------------");
            try{
                blobClient.DeleteIfExists();
            }
            catch (Exception ex){
                log.LogWarning("-----------------------------\n\n  Run - Cannot delete input blob: \n"+ ex.ToString() + "-----------------------------");
            
                //Do not throw
            }
            
            // convert from system.binary to system.io.streamIn
            var streamIn = new MemoryStream(blobContent.Value.Content.ToArray());
            log.LogInformation("----\n\n  Run - Debug #6\n-----------------------------");
            

            // set up connection to Azure OpenAI API client
            string endpoint = GetEnvironmentVariable("OPENAI_API_BASE");
            string key = GetEnvironmentVariable("OPENAI_API_KEY");           
            OpenAIClient client = new OpenAIClient(new Uri(endpoint), new AzureKeyCredential(key));

            // read content of uploaded csv file
            log.LogInformation("-----------------------------\n\n  Run - About to get records from CSV!\n-----------------------------");
            using var reader = new StreamReader(streamIn);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var records = csv.GetRecords<dynamic>().ToList();

            string promptSA = TransactionClassifier.promptSA;
            string promptLA = TransactionClassifier.promptLA;
            string promptRL = TransactionClassifier.promptRL;


            // for each csv record, set value in the columns named "ShortAnswer", "LongAnswer" and "ReferenceLink" with the responses from Azure OpenAI API Completion API
            log.LogInformation("-----------------------------\n\n  Run - About to loop the input rows!\n-----------------------------");
            int rn=0;
            foreach (var record in records)
            {
                rn++;
                log.LogInformation("-----------------------------\n\n  Run - row: " +rn.ToString() + " \n-----------------------------");

                string shortAnswer = QueryAOAI(record, promptSA, client, log);
                record.ShortAnswer = shortAnswer;
                
                string longAnswer = QueryAOAI(record, promptLA, client, log);
                record.LongAnswer = longAnswer;

                string referenceLink = QueryAOAI(record, promptRL, client, log);
                record.ReferenceLink = referenceLink;
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
                var streamOut = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(writer.ToString()));
                var outBlobName = blobName.Replace("Q-","A-");
                var outputBlobClient = new Azure.Storage.Blobs.BlobClient(connectionString, "output", outBlobName);
                outputBlobClient.Upload(streamOut);
            }
            
            log.LogInformation("-----------------------------\n\n  Run - END!!!!!\n-----------------------------");
                
        }
                
        private static string QueryAOAI(dynamic transaction, string prompt, OpenAIClient client, ILogger log)
        {
            prompt = prompt.Replace("QUESTION_AREA", transaction.Area);

            prompt = prompt.Replace("CUSTOMER_QUESTION", transaction.Question);

            log.LogInformation("---------------------------------------\n\n  QueryAOAI - Prompt:\n---------------------------------------");
            log.LogInformation(prompt);
            log.LogInformation("---------------------------------------");
            
            string engine = GetEnvironmentVariable("OPENAI_API_MODEL");

            // call the Azure OpenAI API Completion API
            string completion = string.Empty;
            int maxRetryNum = 3;
            for(int i = 0; i < maxRetryNum; i++){
                log.LogInformation("-- Tentaive #" + i.ToString());
                try{
                    Response<Completions> completionsResponse = client.GetCompletions(engine, prompt);
                    completion = completionsResponse.Value.Choices[0].Text;
                }
                catch(Exception ex){
                    if(ex.Message.Contains("Please retry after 60 seconds")){
                        //Sleep
                        log.LogInformation("-- Waiting 70 seconds");
                        System.Threading.Thread.Sleep(70000);

                        //Do not throw
                    }
                    else
                    {
                        log.LogError("-----------------------------\n\n  QueryAOAI - Error! \n" + ex.ToString() + "\n-----------------------------");
                        completion = string.Concat("ERROR: ", ex.Message.Substring(0,100).Trim());
                        log.LogError("-----------------------------");
                        //do not throw(ex);
                    }
                }
                if(completion != string.Empty){
                    log.LogInformation("-- Tentaive #" + i.ToString() + ". Break!");
                
                    break;
                }
            }
            
            log.LogInformation("-------------------------------------------------\n\n  QueryAOAI - Completion:\n-------------------------------------------------");
            log.LogInformation(completion);
            log.LogInformation("-------------------------------------------------");   

            return completion;
        }

    }

}
