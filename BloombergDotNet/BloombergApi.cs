using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Bloomberglp.Blpapi;

namespace BloombergConsole
{
    class BloombergApi
    {
        #region Members / Contructors

        private Session _session = null;
        private Service _refDataService = null;

        private readonly string _serverHost;
        private readonly int _serverPort;

        public BloombergApi(string serverHost = "localhost", int serverPort = 8194)
        {
            _serverHost = serverHost;
            _serverPort = serverPort;
        }

        /// <summary>
        /// Initialise the Session and the Service
        /// </summary>
        internal void InitialiseSessionAndService()
        {
            if (_session == null)
            {
                var sessionOptions = new SessionOptions
                {
                    ServerHost = _serverHost,
                    ServerPort = _serverPort
                };

                //Console.WriteLine("Connecting to {0}:{1}", sessionOptions.ServerHost, sessionOptions.ServerPort);

                _session = new Session(sessionOptions);

                if (!_session.Start())
                    throw new Exception("Failed to connect!");

                if (!_session.OpenService("//blp/refdata"))
                {
                    _session.Stop();
                    _session = null;

                    throw new Exception("Failed to open //blp/refdata");
                }

                _refDataService = _session.GetService("//blp/refdata");
            }
        }

        /// <summary>
        /// Dispose the Session and the Service
        /// </summary>
        internal void DisposeSessionAndService()
        {
            _refDataService = null;

            //Stop the session
            if (_session != null)
            {
                _session.Stop();
                _session = null;
            }
        }

        #endregion

        #region Methods

        internal Dictionary<string,    //Security
            Dictionary<string, object> //Fields and values
            > GetSecuritiesFields(string[] securities, string[] fields)
        {
            var securitiesFields = new Dictionary<string, Dictionary<string, object>>();

            //Create request
            var referenceDataRequest = _refDataService.CreateRequest("ReferenceDataRequest");

            //Securities
            var securitiesElement = referenceDataRequest.GetElement("securities");
            foreach (var security in securities)
                securitiesElement.AppendValue(security);

            //Fields
            var fieldsElement = referenceDataRequest.GetElement("fields");
            foreach (var field in fields)
                fieldsElement.AppendValue(field);

            //   Send off request
            _session.SendRequest(referenceDataRequest, null);

            //   Start with our flag set to False for not done
            var done = false;

            //   Continue as long as we are not done
            while (!done)
            {
                //   Retrieve next event from the server
                var eventObj = _session.NextEvent();

                //   As long as we have a partial or final response, start to process data
                if (eventObj.Type == Event.EventType.RESPONSE ||
                    eventObj.Type == Event.EventType.PARTIAL_RESPONSE)
                {
                    //  Loop through messages
                    foreach (Message msg in eventObj)
                    {
                        //   Error handler in case of problem which throws meaningful exception
                        if (msg.AsElement.HasElement("responseError"))
                            throw new Exception("Response error:  " + msg.GetElement("responseError").GetElement("message"));

                        //   Extract the securityData top layer and the field data
                        //   History comes back on a single security basis so no looping there
                        var securityDataArray = msg.GetElement("securityData");

                        //   Loop through each security
                        for (var i = 0; i < securityDataArray.NumValues; i++)
                        {
                            //   First take out the security object...
                            var security = securityDataArray.GetValueAsElement(i);

                            var securityName = security.GetElementAsString("security");

                            //   ... then extract the fieldData object
                            var fieldData = security.GetElement("fieldData");

                            //If we need to add a new security to the securitiesFields dictionary then do so
                            Dictionary<string, object> results = null;
                            if (!securitiesFields.ContainsKey(securityName))
                                securitiesFields.Add(securityName, new Dictionary<string, object>());

                            //Get the fieldsByDate dictionary from the securitiesFields dictionary
                            results = securitiesFields[securityName];

                            //Extract results and store in results dictionary
                            foreach (var dataElement in fieldData.Elements)
                            {
                                var dataElementName = dataElement.Name.ToString();

                                //Not using this at present - just demonstrating that we can
                                switch (dataElement.Datatype)
                                {
                                    //Special handling to co-erce bloomberg datetimes back to standard .NET datetimes
                                    case Schema.Datatype.DATE:
                                        results.Add(dataElementName, dataElement.GetValueAsDate().ToSystemDateTime());
                                        break;
                                    case Schema.Datatype.DATETIME:
                                        results.Add(dataElementName, dataElement.GetValueAsDatetime().ToSystemDateTime());
                                        break;
                                    case Schema.Datatype.TIME:
                                        results.Add(dataElementName, dataElement.GetValueAsDatetime().ToSystemDateTime());
                                        break;

                                    //Standard handling
                                    default:
                                        results.Add(dataElementName, dataElement.GetValue());
                                        break;
                                }
                            }
                        }
                    }

                    //   Once we have a response we are done
                    if (eventObj.Type == Event.EventType.RESPONSE) done = true;
                }
            }

            return securitiesFields;
        }

        internal Dictionary<string,     //Security
            Dictionary<DateTime,        //DateTime of security
            Dictionary<string, object>> //Fields and values
            > GetSecuritiesFieldsByDate(string[] securities, string[] fields, DateTime startDate, DateTime endDate)
        {
            var securitiesFieldsByDate = new Dictionary<string, Dictionary<DateTime, Dictionary<string, object>>>();

            //Create request
            var historyDataRequest = _refDataService.CreateRequest("HistoricalDataRequest");

            //Securities
            var securitiesElement = historyDataRequest.GetElement("securities");
            foreach (var security in securities)
                securitiesElement.AppendValue(security);

            //Fields
            var fieldsElement = historyDataRequest.GetElement("fields");
            foreach (var field in fields)
                fieldsElement.AppendValue(field);

            //   Set the start date and end date as YYYYMMDD strings
            historyDataRequest.Set("startDate", startDate.ToString("yyyyMMdd"));
            historyDataRequest.Set("endDate", endDate.ToString("yyyyMMdd"));

            //   Send off request
            _session.SendRequest(historyDataRequest, null);

            //   Start with our flag set to False for not done
            var done = false;

            //   Continue as long as we are not done
            while (!done)
            {
                //   Retrieve next event from the server
                var eventObj = _session.NextEvent();

                //   As long as we have a partial or final response, start to process data
                if (eventObj.Type == Event.EventType.RESPONSE ||
                    eventObj.Type == Event.EventType.PARTIAL_RESPONSE)
                {
                    //  Loop through messages
                    foreach (Message msg in eventObj)
                    {
                        //   Error handler in case of problem which throws meaningful exception
                        if (msg.AsElement.HasElement("responseError"))
                            throw new Exception("Response error:  " + msg.GetElement("responseError").GetElement("message"));

                        //   Extract the securityData top layer and the field data
                        //   History comes back on a single security basis so no looping there
                        var security = msg.GetElement("securityData");
                        var securityName = security.GetElementAsString("security");
                        var fieldData = security.GetElement("fieldData");

                        //   Extract the data for each requested field
                        for (var i = 0; i < fieldData.NumValues; i++)
                        {
                            var data = fieldData.GetValueAsElement(i);

                            //   First get the date - this is our key
                            var date = data.GetElementAsDate("date").ToSystemDateTime();

                            //If we need to add a new security to the securitiesFieldsByDate dictionary then do so
                            Dictionary<DateTime, Dictionary<string, object>> fieldsByDate = null;
                            if (!securitiesFieldsByDate.ContainsKey(securityName))
                                securitiesFieldsByDate.Add(securityName, new Dictionary<DateTime, Dictionary<string, object>>());

                            //Get the fieldsByDate dictionary from the securitiesFieldsByDate dictionary
                            fieldsByDate = securitiesFieldsByDate[securityName];

                            //Extract results and store in results dictionary
                            var results = new Dictionary<string, object>();
                            foreach (var dataElement in data.Elements)
                            {
                                var dataElementName = dataElement.Name.ToString();

                                //Not using this at present - just demonstrating that we can
                                switch (dataElement.Datatype)
                                {
                                    //Special handling to co-erce bloomberg datetimes back to standard .NET datetimes
                                    case Schema.Datatype.DATE:
                                        results.Add(dataElementName, dataElement.GetValueAsDate().ToSystemDateTime());
                                        break;
                                    case Schema.Datatype.DATETIME:
                                        results.Add(dataElementName, dataElement.GetValueAsDatetime().ToSystemDateTime());
                                        break;
                                    case Schema.Datatype.TIME:
                                        results.Add(dataElementName, dataElement.GetValueAsDatetime().ToSystemDateTime());
                                        break;

                                    //Standard handling
                                    default:
                                        results.Add(dataElementName, dataElement.GetValue());
                                        break;
                                }
                            }

                            //Save results dictionary to fieldsByDate dictionary
                            fieldsByDate.Add(date, results);
                        }
                    }

                    //   Once we have a response we are done
                    if (eventObj.Type == Event.EventType.RESPONSE) done = true;
                }
            }

            return securitiesFieldsByDate;
        }

        #endregion
    }
}