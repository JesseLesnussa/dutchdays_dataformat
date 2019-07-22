from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine
import urllib.parse
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
from sqlalchemy import *

params = urllib.parse.quote("Driver={SQL Server};Server=tcp:yellowarrow.database.windows.net,1433;Database=YellowArrow;Uid=yellowarrowadmin@yellowarrow;Pwd=!InzichtIn01;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
loading = "test"
print(engine)
app=Flask(__name__)

@app.route("/", methods=['GET', 'POST'])
def upload_file(loading="loading 123"):
    print("Upload alle bestanden")
    if request.method == 'POST':
        loading="true"
        print(request.files)
       
        print(request.files['medewerkers'])
        medewerkers = request.files['medewerkers']
        medewerkers_df = pd.read_excel(medewerkers, index=False)
        medewerkers_df = medewerkers_df.iloc[1:]
        medewerkers_df = medewerkers_df.filter(['SollicitantID', 'naam', 'veld0', 'veld1', 'veld2'])
        medewerkers_df.to_sql('NoCore_Medewerkers', con=engine, if_exists='replace', index=False)

        projecturen = request.files['projecturen']
        projecturen_df = pd.read_excel(projecturen)
        projecturen_df.columns = projecturen_df.iloc[0]
        projecturen_df = projecturen_df.reindex(projecturen_df.index.drop(0)).reset_index(drop=True)
        projecturen_df.columns.name = None
        projecturen_df=projecturen_df.rename(columns = {'Text187':'Tarief'})
        projecturen_df=projecturen_df.rename(columns = {'Refnr Klant':'RefnrKlant'})
        projecturen_df.to_sql('ProjectUrenTarief', con=engine, if_exists='replace', index=False)
        
        jobs = request.files['jobs']
        jobs_df = pd.read_excel(jobs, index=False)
        empty_indexes = [9,10,11,12]
        jobs_df = jobs_df.drop(jobs_df.columns[empty_indexes], axis=1)
        jobs_df.columns.name = None
        jobs_df.to_sql('Temp_JobsUitPL', con=engine, if_exists='replace', index=False)

        return jobs_df.head(10).to_html(index=False)
    return render_template('index.html')


if __name__ == "__main__":
    app.run()