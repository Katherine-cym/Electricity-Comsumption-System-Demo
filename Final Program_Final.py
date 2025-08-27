from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, redirect
import csv
import pandas as pd
import os
import time
from datetime import datetime, timedelta
import schedule
import json
from shiny import App, ui, render, reactive
from shinywidgets import render_widget, output_widget
import nest_asyncio
import plotly.graph_objects as go
from faicons import icon_svg
from multiprocessing import Value, Manager
import threading

#%%
# Web Page

app = Flask(__name__)

class User:
    def __init__(self, name, houseid):
        self.name = name
        self.houseid = houseid

class UserManage:
    def __init__(self, f='account.csv'):
        self.f = f
        if not os.path.exists(self.f):
            with open("account.csv", "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(['name', 'houseid'])

    def register(self, user):
        with open(self.f, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == user.name and row[1] == user.houseid:
                    return f'''
                    <html>
                    </body>
                    <h3>
                    {user.name} with HouseID {user.houseid} already has an account.
                    </h3>
                    <form action="/" method="post">
                        <p><input type="submit" value="Return to Main"></p>
                    </form>
                    </body>
                    </html>'''

        with open(self.f, 'a', newline="") as f:
            w = csv.writer(f)
            w.writerow([user.name, user.houseid])
        return f'''
                <html>
                <body>
                <h3>
                Register successfully for {user.name} with HouseID {user.houseid}
                </h3>
                <form action="/" method="post">
                    <p><input type="submit" value="Return to Main"></p>
                </form>
                </body>
                </html>
                '''

    def login(self, name, houseid):
        # Modify the code to let it return houseid instead
        # houseid will be passed as a query arguments in the url
        # Which will be used as the filtering criteria of the dashboard
        with open("account.csv", 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                if row[0] == name and row[1] == houseid:
                    return houseid
        return None
    
def run_flask():
    print("Starting Flask API")
    app.run(port=1234, debug=False, use_reloader=False)

user_manage = UserManage()

@app.route('/', methods=['GET','POST'])
def main():

    if redirect_page.value:
        return redirect('/maintenance')  
    
    '''
    to refresh the page every 60s. If re-direct is True, 
    the page will be re-drect to /maintenance.
    '''
        
    return """
        
    <html>
    <body>
    <div id="rightdiv">
    <h2>Welcome, please select options!</h2>
    <form action="/register" method="post">
    <p><input type="submit" value="Registration"></p>
    </form>
    <form action="/login" method="post">
    <p><input type="submit" value="Login"></p>
    </form>
    <script>
        setInterval(function(){
            location.reload();
        }, 60000); // Refresh every 1 minute
    </script>
    </div>
    </body>
    </html>
        
    """
        
@app.route('/maintenance', methods=['GET','POST'])
def maintenance():
    return """
    
    <html>
    <body>
    <div id="rightdiv">
    <h2>Site unavailable, please come back later!</h2>
    </div>
    </body>
    </html>
    
    """

@app.route('/register', methods=['GET', 'POST'])
def register():

    if redirect_page.value:
        return redirect('/maintenance')  
    
    if request.method == 'POST' and request.form.get('name') != None and request.form.get('houseid') != None:
        name = request.form.get('name')
        houseid = request.form.get('houseid')
        user = User(name, houseid)
        return user_manage.register(user)

    '''
    to refresh the page every 60s. If re-direct is True, 
    the page will be re-drect to /maintenance.
    '''

    return """
    <html>
    <div id="rightdiv">
    <form action="register" method="post">
    <h3>Create your account</h3>
        <label for="name">
        <strong>Name</strong>
        </label>
        <input type="text" id="name" placeholder="Please enter your account name" name="name" required style="width: 30ch;">
        <br><br>
        
        <label for="houseid">
        <strong>HouseID</strong>
        </label>
        <input type="text" id="houseid" placeholder="Please enter your houseid" name="houseid" required style="width: 30ch;">
        <br><br>
        <input type="submit" value="Create Account"/>
    </form>
    <script>
        setInterval(function(){
            location.reload();
        }, 60000); // Refresh every 1 minute
    </script>
    </div>
    </html>
    """

@app.route('/login', methods=['GET', 'POST'])
def login():

    if redirect_page.value:
        return redirect('/maintenance')  
    
    #with app.app_context():  # Provide Flask request context
    if request.method == 'POST' and request.form.get('name') != None and request.form.get('houseid') != None:
        name = request.form.get('name')
        houseid = request.form.get('houseid')
        user_id = user_manage.login(name, houseid)
        if user_id:
            # If successfully login, autodirect to the dashboard page
            return redirect(f'http://localhost:3838?houseid={user_id}')
        else:
                return 'Invalid name or HouseID. Please check again' 
            # is there a need to reroute back to '/login'??

    '''
    to refresh the page every 60s. If re-direct is True, 
    the page will be re-drect to /maintenance.
   
    '''

    return """
    <html>
    <div id="rightdiv">
    <form action="login" method="post">
    <h3>Login to your account</h3>
        <label for='name'>
        <strong>Name</strong>
        </label>
        <input type="text" id="name" placeholder="Enter your account name" name="name" required style="width: 30ch;">
        <br><br>
        
        <label for="houseid">
        <strong>HouseID</strong>
        </label>
        <input type="text" id="houseid" placeholder="Enter your houseid" name="houseid" required style="width: 30ch;">
        <br><br>
        <input type="submit" value="Login"/>
    </form>
    <script>
        setInterval(function(){
            location.reload();
        }, 60000); // Refresh every 1 minute
    </script>
    </div>
    </html>
    """
    
#%%

# Shiny App

app_ui = ui.page_sidebar(
    ui.sidebar(ui.input_selectize('period', 'Period', choices=['Daily', 'Weekly', 'Monthly']),
               title='Filters'),
    ui.layout_column_wrap(
        ui.value_box('Average Usage Per Day', ui.output_text('daily_usage'), showcase=icon_svg('bolt')),
        ui.value_box('Average Usage Per Week', ui.output_text('weekly_usage'), showcase=icon_svg('plug')),
        ui.value_box('Average Usage Per Month', ui.output_text('monthly_usage'), showcase=icon_svg('lightbulb')),
        fill=False
    ),
    ui.layout_columns(
        ui.card(ui.card_header('Electricity Usage'), output_widget('bar_and_scatter'), full_screen=True),
        ui.card(ui.card_header('Daily Usage'), ui.output_data_frame('summary_statistics'), full_screen=True)
    ),
    title=ui.HTML('<h3><b>Your Electricity Usage Dashboard</b></h3>'),
    fillable=True
)

def server(input, output, session):
    dic_period = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}

    # Load JSON data only once to avoid repeated file reads
    with open('daily_consumption_house.json', 'r') as j:
        usage = json.load(j)

    # Reactive function to extract user ID
    @reactive.calc
    def user_id():
        return session.input[".clientdata_url_search"]()[9:]

    # Reactive function to get the selected period
    @reactive.calc
    def selected_period():
        return dic_period[input.period()]

    # Reactive function to prepare the data
    @reactive.calc
    def get_data():
        uid = user_id()
        if uid not in usage:
            return None  # Handle invalid user ID

        # Convert user data to DataFrame
        df = pd.DataFrame([data[:2] for data in usage[uid]], columns=['Date', 'Usage'])
        df['Date'] = pd.to_datetime(df['Date'])

        # Convert all users' data to DataFrame
        dfs = [pd.DataFrame([row[:2] for row in record], columns=['Date', 'Usage']) for record in usage.values()]
        df_all = pd.concat(dfs)
        df_all['Date'] = pd.to_datetime(df_all['Date'])

        house_count = len(df_all)/len(df)

        return df, df_all, house_count

    @render.text
    def daily_usage():
        data = get_data()
        if data is None:
            return "No data available"
        df, _, _ = data
        avg_usg = df.groupby(pd.Grouper(key='Date', freq='D')).sum().mean().values[0]
        return round(avg_usg, 2)

    @render.text
    def weekly_usage():
        data = get_data()
        if data is None:
            return "No data available"
        df, _, _ = data
        avg_usg = df.groupby(pd.Grouper(key='Date', freq='W')).sum().mean().values[0]
        return round(avg_usg, 2)

    @render.text
    def monthly_usage():
        data = get_data()
        if data is None:
            return "No data available"
        df, _, _ = data
        avg_usg = df.groupby(pd.Grouper(key='Date', freq='ME')).sum().mean().values[0]
        return round(avg_usg, 2)

    @render_widget
    def bar_and_scatter():
        data = get_data()
        if data is None:
            return go.Figure()  # Return an empty figure if no data

        df, df_all, house_count = data
        period = selected_period()
        grouper = pd.Grouper(key='Date', freq=period)

        fig = go.Figure(layout=dict(legend=dict(groupclick='toggleitem', x=0.5, y=1.15, orientation='h')))

        fig.add_trace(go.Bar(
            x=df.groupby(grouper).sum().reset_index()['Date'],
            y=df.groupby(grouper).sum().reset_index()['Usage'],
            name="Total Usage",
            marker_color="#4e7496",
        ))

        fig.add_trace(go.Scatter(
            x= df_all.groupby(grouper).sum().div(house_count).reset_index()['Date'],
            y= df_all.groupby(grouper).sum().div(house_count).reset_index()['Usage'],
            mode="markers",
            name="Average usage for all users",
            marker=dict(color="#ffc900", size=8),
            marker_symbol='diamond',
        ))

        fig.update_layout(
            xaxis=dict(
                rangeslider=dict(visible=True),
                type="date"
        ))

        return fig

    @render.data_frame
    def summary_statistics():
        data = get_data()
        if data is None:
            return pd.DataFrame()  # Return an empty DataFrame if no data
        df, _, _ = data
        df_summary = df.copy()
        df_summary['Date'] = df_summary['Date'].dt.strftime('%Y-%m-%d')
        return df_summary

shiny_app = App(app_ui, server)
#nest_asyncio.apply()
def run_shiny():
    print("Starting shiny")
    shiny_app.run(host='0.0.0.0', port=3838) 

#%%
# Meter Reading
class MeterReadingsMgr:
    def __init__(self, csv_filename):
        self.csv_filename = csv_filename # elec usage file to be loaded 
        self.house_meter_pair = {} # hold house-meter pair for search
        self.meter_house_pair = {} # invert the meter-house pair to support use in calculation module
        self.meter_readings = {} # hold meter-usage readings for backend work
        self.meter_timestamp_readings = {} # timestamped usage for audit log
        self.daily_meter_readings = {} # daily in-memnory for calculation of daily usage        
        self.load_data() # to load pre-generated meters readings
        
    # load pre-generated meters readings    
    def load_data(self):
        with open(self.csv_filename, mode = "r") as file:
            data = csv.reader(file)
            next(data)
            
            for row in data:
                house_id, meter_id, reading = row[0], row[1], float(row[2])
                
                if house_id not in self.house_meter_pair:
                    self.house_meter_pair[house_id] = meter_id
                    self.meter_house_pair[meter_id] = house_id
                    self.meter_readings[meter_id] = []
                
                self.meter_readings[meter_id].append(reading)

    # extract meters readings every 30min to simulate real-life scenario 
    def extract_meter_readings(self):
        while True:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # set timestamp to current time in YYYY-MM-DD HH:MM:SS format
            
            readings_empty = True
            
            for meter_id, readings in self.meter_readings.items():
                if readings:
                    readings_empty = False
                    key = f"{meter_id}-{timestamp}"
                    value = readings.pop(0)
                    
                    if key not in self.meter_timestamp_readings:
                        self.meter_timestamp_readings[key] = []
                        
                    self.meter_timestamp_readings[key].append(value)
                    #print (self.meter_timestamp_readings)
                    #time.sleep(1)
                    
                    if meter_id not in self.daily_meter_readings:
                        self.daily_meter_readings[meter_id] = []
                    
                    self.daily_meter_readings[meter_id].append(value)            
                    print (self.daily_meter_readings)

            if readings_empty:
                break # exit loop if all meters has no reading

            time.sleep(1800) # time in second. wait for 30min before next extraction

    def save_daily_readings(self):

        file_timestamp = datetime.now().strftime("%Y-%m-%d") # create today date timestamp for the file to be saved
        output_list_csv = f"daily_meter_readings_log{file_timestamp}.csv"
        output_timestamp_csv = f"daily_timestamp_readings_log{file_timestamp}.csv"

        with open(output_list_csv, mode = "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["MeterID", "Readings"])
            
            for key, value in self.daily_meter_readings.items():
                writer.writerow([key,value])
                                    
        with open(output_timestamp_csv, mode = "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["MeterID_Timestamp", "Readings"])
            
            for key, value in self.meter_timestamp_readings.items():
                writer.writerow([key,value])            
    
    # clear the memory of the dict
    def empty_dicts(self):
        self.daily_meter_readings ={}
        self.meter_timestamp_readings = {}
        
#%%
# Daily aggregation & saving to file

class DailyConsump:
    def __init__(self,csv_filename):
        self.csv_filename = csv_filename # elec usage file to be loaded 
        self.daily_consumption = {} 
        self.daily_consumption_house = {}
        meter_readings_mgr = MeterReadingsMgr(csv_filename) # import the MeterReadingsMgr
        self.meter_house_pair = meter_readings_mgr.meter_house_pair # MeterReadingsMgr meter-house-pair
        
    def calculation(self,daily_meter_readings):

        # define the files name for consumption        

        consumption_json = "daily_consumption.json"
        consumption_house_json = "daily_consumption_house.json"

        '''  
         
        Daily_consumption.json includes timestamp. This will be save as log for audit purpose. 
        Daily_consumption_house.json is a nested dic of each house_id: [Date, Daily usage, last meter reading].
         
        The design of log for this electricity monitoring system is assumed hourly granularity is not important for home-owner.
        Daily consumption is determined by "Today last meter reading of the daily" - "Yesterday last meter reading".
        During unplanned power failure, all extracted 30min readings will be lost. However, this will not affect daily consumption computation.
        Once the system restarts, it will continue to extract 30min readings from all meters. At the end of the day, it will compute daily consumption
        using the above formula. 
        
        The "daily_consumption_house.json" can also be fed into the billing systems to extract daily usage to compute monthly bill.
        Monthly bill is determined by multipling monthly usage with the monthly prevailing electricity price rate.
        
        
        '''

        
        # if no file exists, start an empty dictionary
        if not os.path.exists(consumption_json):
            with open(consumption_json, "w") as file:
                json.dump({}, file)
        
        if not os.path.exists(consumption_house_json):
            with open(consumption_house_json, "w") as file:
                json.dump({}, file)
        
        # Load the existing data
        with open(consumption_json, "r") as file:
            self.daily_consumption = json.load(file)
            
        with open(consumption_house_json, "r") as file:
            self. daily_consumption_house = json.load(file)           
        
        for meter_id, readings in daily_meter_readings.items(): 
            if readings:
                if meter_id not in self.daily_consumption.keys():
                    self.daily_consumption[meter_id] = []
                    today_last_reading = readings[-1]
                    yesterday_last_reading = 0
                else:
                    today_last_reading = readings[-1]
                    yesterday_last_reading = self.daily_consumption[meter_id][-1][-1] if self.daily_consumption[meter_id] else 0
                    print(f'>>>{yesterday_last_reading}')
        
                print(f'--------{today_last_reading}--->{yesterday_last_reading}')
                daily_usage = round(today_last_reading - yesterday_last_reading, 2)
                date = datetime.now().strftime("%Y-%m-%d")
                self.daily_consumption[meter_id].append([date, daily_usage, today_last_reading])
        
            house_id = self.meter_house_pair.get(meter_id) # match house_id of the meter_id
            if meter_id in self.daily_consumption:
                self.daily_consumption_house[house_id] = self.daily_consumption[meter_id]
            else:
                print(f"Warning: No house_id found for meter_id {meter_id}")

    def save_consumption(self):
                  
        consumption_json = "daily_consumption.json"
        with open(consumption_json, "w") as file:
            json.dump(self.daily_consumption, file, indent = 4)
            print("Data saved:", self.daily_consumption) # show file saved
            
        consumption_house_json = "daily_consumption_house.json"
        with open(consumption_house_json, "w") as file:
            json.dump(self.daily_consumption_house, file, indent = 4)
            print("Data saved:", self.daily_consumption_house) # show file saved                                  
            
class Scheduler:
    def __init__(self, meter_readings_manager, backup_time, daily_consumption, server_uptime, redirect_page, empty_dicts):
        self.meter_readings_manager = meter_readings_manager
        self.backup_time = backup_time
        self.daily_consumption = daily_consumption
        self.server_uptime = server_uptime
        self.redirect_page = redirect_page 
        self.empty_dicts = empty_dicts
        
    def scheduled_tasks(self):
        # print for debugging
        print(f"Scheduled Debug - Running - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}") 
        
        # ensure that each part of the thread is completed before next
        with ThreadPoolExecutor(max_workers=5) as thread_executor:
            future1 = thread_executor.submit(self.meter_readings_manager.save_daily_readings)
            future1.result()
            
            future2 = thread_executor.submit(self.daily_consumption.calculation, self.meter_readings_manager.daily_meter_readings)
            future2.result()    
            
            future3 = thread_executor.submit(self.daily_consumption.save_consumption)
            future3.result()    
        
        self.redirect_page.value = True
        self.empty_dicts()
        
        # print("Redirected is Activated") # for debugging use to show that 
                
    def deactivate_maintenance(self):
        print(f"Server Uptime - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}") 
        self.redirect_page.value = False

    def monitor_redirect(self):
        while True:
            time.sleep(600)  # Check every 10 min; to let admin know that 
            if redirect_page.value:
                print("Redirect triggered! Flask will now redirect all users.")

    def run_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def start_scheduler(self):
        schedule.every().day.at(self.backup_time).do(self.scheduled_tasks) # execute daily backup
        schedule.every().day.at(self.server_uptime).do(self.deactivate_maintenance) # turn off the redirect
        # schedule.every(60).seconds.do(self.scheduled_tasks) # run schedule every 60s to create mock-up
        threading.Thread(target = self.run_scheduler, daemon=True).start() # to rrun concurrently with extraction of data
#%%

if __name__ == "__main__":
    
    csv_filename = "project_meter_readings.csv"
    meter_mgr = MeterReadingsMgr(csv_filename)
    
    daily_consumption = DailyConsump(csv_filename)
    daily_backup_time = "00:00" # this is for testing// (datetime.now() + timedelta(minutes=1)).strftime("%H:%M")
    server_uptime = "01:00" # this is for testing // (datetime.now() + timedelta(minutes=3)).strftime("%H:%M")
    manager = Manager()
    redirect_page = manager.Value('b', False)
    empt_dicts = meter_mgr.empty_dicts
    scheduler = Scheduler(meter_mgr, daily_backup_time, daily_consumption, server_uptime, redirect_page,empt_dicts)

    # should be multi-processing, due to hardward limitation, flask & shiny uses multi-threading

    flask_thread=threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
        
    shiny_thread = threading.Thread(target=run_shiny, daemon=True)
    shiny_thread.start()  
    
    with ThreadPoolExecutor(max_workers=5) as thread_executor:
        thread_executor.submit(meter_mgr.extract_meter_readings)
        # thread_executor.submit(scheduler.run_scheduler)
        thread_executor.submit(scheduler.start_scheduler)
        thread_executor.submit(scheduler.monitor_redirect)



