
from flask import Flask, request, jsonify, render_template, send_file
from sqlalchemy import create_engine
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
from sqlalchemy import *
import openpyxl


params = urllib.parse.quote("DRIVER={SQL Server};Server=tcp:yellowarrow.database.windows.net,1433;Database=YellowArrow;Uid=yellowarrowadmin@yellowarrow;Pwd=!InzichtIn01;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
app=Flask(__name__)

def setColumnNames(df, columnNames):
    df.columns = columnNames
    return df

@app.route("/")
def homepage():
    return render_template('index.html')

@app.route("/upload", methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        filename = "BuyFlex Dataformat export.xlsx" 
        connection = engine.connect()
        print(request.files['medewerkers'])
        print('Verwerken van medewerkers...')
        medewerkers = request.files['medewerkers']
        medewerkers_df = pd.read_excel(medewerkers, index=False)
        print(medewerkers_df)
        medewerkers_df = setColumnNames(medewerkers_df, ['SollicitantID', 'naam', 'veld0', 'veld1', 'veld2'])
        print(medewerkers_df)
        medewerkers_df.to_sql('NoCore_Medewerkers', con=engine, if_exists='replace', index=False)

        print('Succes!')
        print(request.files['projecturen'])
        print('Verwerken van projecturen...')
        projecturen = request.files['projecturen']
        projecturen_df = pd.read_excel(projecturen)
        projecturen_df = projecturen_df.iloc[1:]
        projecturen_df = setColumnNames(projecturen_df, ['SollicitantID', 'cNaam', 'JaarWeek', 'Tarief', 'Activiteitnummer', 'ActiviteitOmschr', 'RefnrKlant', 'Werkuren', 'UrenI', 'UrenII', 'UrenIII', 'UrenIV', 'UrenV', 'Verliesuren', 'Reis', 'Dagen', 'svwdagen' ])         
        print("converting string sollicitant id")
        projecturen_df['SollicitantID'] = projecturen_df['SollicitantID'].astype(str)
        print("success!")
        print(projecturen_df)
        projecturen_df.to_sql('ProjectUrenTarief', con=engine, if_exists='replace', index=False)
        
        print('Succes!')
        print(request.files['jobs'])
        print('Verwerken van jobs...')
        jobs = request.files['jobs']
        jobs_df = pd.read_excel(jobs, index=False)
        empty_indexes = [9,10,11,12]
        print('Uitvoeren van ETL-proces')
        jobs_df = jobs_df.drop(jobs_df.columns[empty_indexes], axis=1)
        jobs_df = jobs_df.iloc[8:-1]
        jobs_df = setColumnNames(jobs_df, ['ProjectId', 'MedewerkerId', 'NaamMedewerker', 'Job', 'BeginDatum', 'EindDatum', 'Tarief', 'Uurloon', 'Facturering'])
        print(jobs_df)
        jobs_df.to_sql('Temp_JobsUitPL', con=engine, if_exists='replace', index=False)
        print('Bestanden zijn ingevoerd in staging, ETL-proces wordt gestart...')
        connection.execute(text("EXEC spGetDataFormat").execution_options(autocommit=True))
        print('Succes!')
        connection.close()
        df =  pd.read_sql_table('vwDataFormat', con=engine)
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
