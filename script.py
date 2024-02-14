import gspread
import json
import requests
import pandas as pd
import urllib

from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def getEventStats(event):
#this function takes an event short name, reads the html for its webpage
#formats the data and returns the detail and summary as a dataframe

    try:
        #get html
        html = requests.get(f'https://www.parkrun.org.uk/{event}/results/eventhistory/', headers=headers).content
        
        try:
            #read html to a list of dataframes
            df_list = pd.read_html(html)
        except Exception as e:
            print (f'something went wrong: {event}')
            print (e)
            return None,None

        #pull out and rename the cols we are interested in
        data = df_list[0][['Event ##', 'Date', 'Date/First Finishers', 'Finishers']]
        data = data.rename(columns={"Event ##":"eventNum","Date/First Finishers":"finishers","Finishers":"volunteers"})

        #data=data.query('eventNum<=63').query('eventNum>60')

        #get the Date column
        regexData = data['Date']
        #print(regexData)
        #print(data[['Date']].to_string())


        #extract data from the Date column

        #get the group of 2 digits slash 2 digits slash 4 digits, following this group are some chars followed by 'M)'
        data[["date"]]  = regexData.str.extract('(\d{2}/\d{2}/\d{4}).+M\)')
        #after the first closed bracket then a space, get a group made up of any number of digits, a colon, any number of digits, an 
        #optional colon and an optional 2 digits. This group is following by a string of charecters a closed bracket then more chars
        data[["maleFirstFinisherTime"]]  = regexData.str.extract('.*\) ([0-9]*:[0-9]*:?[0-9]{2}?).*\).*')
        #get all chars that follow "F) "
        data[["femaleFirstFinisherTime"]]  = regexData.str.extract('F\) (.*)')

        #clean up data
        data['finishers'] = data['finishers'].str.replace('finishers','')
        data['finishers'] = data['finishers'].str.replace('finisher','')    
        data['volunteers'] = data['volunteers'].str.replace('volunteers','')
        data['volunteers'] = data['volunteers'].str.replace('volunteer','')
        data['volunteers'] = data['volunteers'].str.replace('Unknown','')

        #drop the Date column
        data = data.drop(['Date'], axis=1)

        #date col to datetime
        data['date'] = pd.to_datetime(data['date'],format = '%d/%m/%Y' )


        #first finisher times to datetime
        #if the time is more than 1 hour need to prefix a zero
        #if less than an hour 2 zeros
        #if NULL do nothing

        data['maleFirstFinisherTime'] =pd.to_timedelta(data.apply(lambda row: (
                                    '0'+ row.maleFirstFinisherTime if row.maleFirstFinisherTime[1] == ':' else  
                                    '00:'+ row.maleFirstFinisherTime) if not pd.isnull(row.maleFirstFinisherTime) else 
                                    None, axis=1))
 
        data['femaleFirstFinisherTime'] =pd.to_timedelta(data.apply(lambda row: (
                                    '0'+ row.femaleFirstFinisherTime if row.femaleFirstFinisherTime[1] == ':' else  
                                    '00:'+ row.femaleFirstFinisherTime) if not pd.isnull(row.femaleFirstFinisherTime) else 
                                    None, axis=1))
        

        #more casting
        data['finishers'] = pd.to_numeric(data['finishers'] )
        data['volunteers'] = pd.to_numeric(data['volunteers'],errors = 'coerce' ).astype('Int64')

        #add the name of the event to the dataframe
        data.insert(0,'event',f'{event}')
        
        
        #get the fastest male and females times for the event, insert the event name, and rename the columns
        summary = data.agg({'maleFirstFinisherTime':'min','femaleFirstFinisherTime':'min'}).to_frame().transpose()
        summary.insert(0,'event',f'{event}')
        summary = summary.rename(columns={"finishers":"avgFinishers","maleFirstFinisherTime":"fastestMaleFinisher","femaleFirstFinisherTime":"fastestFemaleFinisher"})

        #get the average attendance for last 5 events and add it to the summary dataframe
        lastFiveEventsAvg = data.head(5).agg({'finishers': 'mean'}).values[0]
        summary.insert(1,'lastFiveEventsAvgAttendance',lastFiveEventsAvg)

        #get the dates the records were set
        maleRecordDate = data.query(f'maleFirstFinisherTime == "{summary[["fastestMaleFinisher"]].values[0][0]}"').sort_values(by=['date'],ascending=True).head(1)['date'].values[0]
        femaleRecordDate = data.query(f'femaleFirstFinisherTime == "{summary[["fastestFemaleFinisher"]].values[0][0]}"').sort_values(by=['date'],ascending=True).head(1)['date'].values[0]

        #add the record dates to the summary dataframe
        summary['maleRecordDate'] = maleRecordDate
        summary['femaleRecordDate'] = femaleRecordDate

        return data,summary

        #print(data.keys()) #show col names
        #print (data.dtypes) #show data types of cols
    
    except Exception as e:
        print(event)
        print (e)

#needed to website to return data
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

jsonPath = 'C://Users//james lester//Dropbox//temp//events.json' #WE WILL DUMP THE JSON FILE HERE
urllib.request.urlretrieve('https://images.parkrun.com/events.json',jsonPath) #download the file

#load the events data to a variable
with open(jsonPath) as f:
    eventsData = json.load(f)

#get the dataframe we are interested in
normalisedEventsData = pd.json_normalize(eventsData['events'],record_path = 'features')

ukAdultEvents=normalisedEventsData[(normalisedEventsData["properties.countrycode"]==97) & (normalisedEventsData["properties.seriesid"]==1) ][["properties.eventname"]] 
#filtering dataframe rows and specify cols
#countrycode 97 = uk, seriesid =1 - non junior parkrun

ukAdultEvents=ukAdultEvents.rename(columns={'properties.eventname':'eventName'}) #rename column

#send the event list to a list
ukAdultEvents = ukAdultEvents['eventName'].to_list()
#ukAdultEvents = ['wolverhampton']

#store the output from getEventStats functions for each event 
summaryOutput = []

#run the function for each event and add to result list
for event in ukAdultEvents[0:999]:
    data,summary  = getEventStats(event)
    summaryOutput.append(summary)

#stick all the result sets together, reset the index
summarydf = pd.concat(summaryOutput)
summarydf = summarydf.reset_index(drop=True)

print (summarydf.to_string(index=False))


#usual set up before accessing sheets, make sure your credentials file is set up
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("x.json", scope)
client = gspread.authorize(creds)


sheet = client.open("parkrun records").worksheet('Sheet1') # Open the spreadhseet

#cast dataframe columns
summarydf['fastestMaleFinisher'] = summarydf['fastestMaleFinisher'].astype(str)
summarydf['fastestFemaleFinisher'] = summarydf['fastestFemaleFinisher'].astype(str)
summarydf['maleRecordDate'] = summarydf['maleRecordDate'].astype(str)
summarydf['femaleRecordDate'] = summarydf['femaleRecordDate'].astype(str)

#tidy up data
summarydf['fastestMaleFinisher'] = summarydf['fastestMaleFinisher'].str.replace('0 days 00:','')
summarydf['fastestFemaleFinisher'] = summarydf['fastestFemaleFinisher'].str.replace('0 days 00:','')


sheet.clear() #delete all data
sheet.update([summarydf.columns.values.tolist()] + summarydf.values.tolist()) #past the dataframe

#bold, black background, white text for headers
sheet.format('A1:F1',   {
                        'textFormat':   {
                                        'bold': True,                        
                                        "foregroundColor": {
                                                            "red": 1.0,
                                                            "green": 1.0,
                                                            "blue": 1.0
                                                            }
                                        }, 
                        "backgroundColor": {
                                            "red": 0.0,
                                            "green": 0.0,
                                            "blue": 0.0}
                        }
            )


#rename sheet headers
sheet.update_cell(1,1, 'Event')
sheet.update_cell(1,2, 'Avg Attendance (last 5 events)')
sheet.update_cell(1,3, 'Fastest Male Finisher')
sheet.update_cell(1,4, 'Fastest Female Finisher')
sheet.update_cell(1,5, 'Male Record Date')
sheet.update_cell(1,6, 'Female Record Date')

#add a note to say when we last updated
sheet.update_cell(1,8, 'Last Updated:' + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
sheet.update_cell(2,8, 'Updated Weekly')

#autofit columns
sheet.columns_auto_resize(0, 6)                    