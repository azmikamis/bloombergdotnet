using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

//using BEmu;
using Bloomberglp.Blpapi;

namespace BloombergDotNet
{
    public class MainWindowViewModel
    {
        private DateTime getPreviousTradingDate()
        {
            DateTime tradedOn = DateTime.Now;
            tradedOn = tradedOn.AddDays(-1);

            if (tradedOn.DayOfWeek == DayOfWeek.Sunday)
            {
                tradedOn = tradedOn.AddDays(-2);
            }
            else if (tradedOn.DayOfWeek == DayOfWeek.Saturday)
            {
                tradedOn = tradedOn.AddDays(-1);
            }
            return tradedOn;
        }

        public void Test()
        {
            string serverHost = "localhost";
            int serverPort = 8194;

            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.ServerHost = serverHost;
            sessionOptions.ServerPort = serverPort;
            
            Debug.WriteLine("Connecting to " + serverHost + ":" + serverPort);
            Session session = new Session(sessionOptions);
            bool sessionStarted = session.Start();
            if (!sessionStarted)
            {
                System.Console.Error.WriteLine("Failed to start session.");
                return;
            }
            if (!session.OpenService("//blp/refdata"))
            {
                System.Console.Error.WriteLine("Failed to open //blp/refdata");
                return;
            }
            Service refDataService = session.GetService("//blp/refdata");
            Request request = refDataService.CreateRequest("IntradayBarRequest");
            request.Set("security", "IBM US Equity");
            request.Set("eventType", "TRADE");
            request.Set("interval", 60);	// bar interval in minutes
            DateTime tradedOn = getPreviousTradingDate();
            request.Set("startDateTime", new Datetime(tradedOn.Year,
                                                      tradedOn.Month,
                                                      tradedOn.Day,
                                                      13, 30, 0, 0));
            request.Set("endDateTime", new Datetime(tradedOn.Year,
                                                    tradedOn.Month,
                                                    tradedOn.Day,
                                                    21, 30, 0, 0));
            Debug.WriteLine("Sending Request: " + request);
            session.SendRequest(request, null);

            while (true)
            {
                Event eventObj = session.NextEvent();
                foreach (Message msg in eventObj.GetMessages())
                {
                    Debug.WriteLine(msg.ToString());
                }
                if (eventObj.Type == Event.EventType.RESPONSE)
                {
                    break;
                }
            }
        }
    }
}
