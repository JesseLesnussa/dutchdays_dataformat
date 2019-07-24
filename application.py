from flask import Flask, request, jsonify, render_template, send_file
from sqlalchemy import create_engine
import urllib.parse
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
from sqlalchemy import *
import openpyxl

params = urllib.parse.quote("Driver={SQL Server};Server=tcp:yellowarrow.database.windows.net,1433;Database=YellowArrow;Uid=yellowarrowadmin@yellowarrow;Pwd=!InzichtIn01;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
loading = "test"
print(engine)
app=Flask(__name__)
filename = ""

@app.route("/")
def homepage():
    return render_template('index.html')

@app.route("/upload", methods=['GET', 'POST'])
def upload_file():
    render_template('loading.html')
    if request.method == 'POST':
        filename = "BuyFlex Dataformat export.xlsx" 
        render_template('loading.html')
        connection = engine.connect()
        print(request.files['medewerkers'])
        print('Verwerken van medewerkers...')
        medewerkers = request.files['medewerkers']
        medewerkers_df = pd.read_excel(medewerkers, index=False)
        medewerkers_df = medewerkers_df.iloc[1:]
        medewerkers_df = medewerkers_df.filter(['SollicitantID', 'naam', 'veld0', 'veld1', 'veld2'])
        medewerkers_df.to_sql('NoCore_Medewerkers', con=engine, if_exists='replace', index=False)
        print('Succes!')
        print(request.files['projecturen'])
        print('Verwerken van projecturen...')
        projecturen = request.files['projecturen']
        projecturen_df = pd.read_excel(projecturen)
        projecturen_df=projecturen_df.rename(columns = {'Text187':'Tarief'})
        projecturen_df=projecturen_df.rename(columns = {'Refnr Klant':'RefnrKlant'})
        projecturen_df['SollicitantID'] = projecturen_df['SollicitantID'].astype(str)
        projecturen_df.to_sql('ProjectUrenTarief', con=engine, if_exists='replace', index=False)
        print('Succes!')
        print(request.files['jobs'])
        print('Verwerken van jobs...')
        jobs = request.files['jobs']
        jobs_df = pd.read_excel(jobs, index=False)
        empty_indexes = [9,10,11,12]
        print('Uitvoeren van ETL-proces')
        jobs_df = jobs_df.drop(jobs_df.columns[empty_indexes], axis=1)
        jobs_df.columns.name = None
        jobs_df.to_sql('Temp_JobsUitPL', con=engine, if_exists='replace', index=False)
        print('Bestanden zijn ingevoerd in s..')
        connection.execute(text("EXEC spGetDataFormat").execution_options(autocommit=True))
        print('Succes!')
        connection.close()
        df =  pd.read_sql_table('vwDataFormat', con=engine);
        df.to_excel( 'uploads/' + filename , index=False, header=False )         

        return render_template('export.html', value= df.to_html(index=False, header=False), filename=filename)
    return render_template('index.html')


@app.route('/uploads/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    path = 'uploads/' + filename
    print(path)
    return send_file(path, as_attachment=True)

if __name__ == "__main__":
    app.run()