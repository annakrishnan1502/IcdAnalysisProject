import os
import re
import pandas
import csv
import sys
import json

header = ['Customer','FileName','Total','IsICD9CodePresent?','IsICD10CodePresent?','Patients Count with ICD-9','Patients count with ICd-10','Patients count with Both','Patients With Any']
outputJson = {}
newDirPointer ={}
with open("config.json","r") as f:
 config = json.loads(f.read())
 outputConfig = config["OUTPUT"]
 outputFileDir = outputConfig["DIR"]+outputConfig["filename"]
 for customer in config["CUSTOMERS"]:
     for dir in config["CUSTOMERS"][customer]:
         strvalue = customer+"_"+dir
         newDirPointer[strvalue] = config["CUSTOMERS"][customer][dir]

def readCsv(path):
    dfcsv = pandas.read_csv(path)
    return dfcsv

def readJSON(path):
    dfjson = pandas.read_json(path, lines=True)
    return dfjson

with open(outputFileDir, 'a+') as output:
    writer = csv.writer(output, delimiter=',')
    writer.writerow(header)
    for customer, dir in newDirPointer.items():
        files = []
        masterPatList = []
        outputJson[customer] = {}
        for filename in os.listdir(dir):
            if re.search(r'(Patient|Diagnosis|Encounter|Medorder|Medadmin|Testorder|ClinicalTestResult|CancerAassessment).*\./*',filename,re.IGNORECASE):
             files.append(filename)
             #print(files)
        for f in files:
            showcols = []
            filePath = dir + f
            print 'reading file: ' + filePath
            sys.stdout.flush()
            ext = os.path.splitext(filePath)[-1].lower()
            if(ext == ".csv"):
                df = readCsv(filePath)
            elif(ext == ".json"):
                df = readJSON(filePath)
            else:
                print "unknown extension"
                continue
            outputJson[customer][f] = {}
            isICD9CodePresent = "Yes"
            isICD10CodePresent = "Yes"
            if ('ICD9_CODE' in df.columns):
                icd_col = 'ICD9_CODE'
            elif ('ICD9_Code' in df.columns):
                icd_col = 'ICD9_Code'
            elif ('Admitting_IC9_Code' in df.columns):
                icd_col = 'Admitting_IC9_Code'
            elif ('icd9_list' in df.columns):
                icd_col = 'icd9_list'
            elif ('Current_ICD9_List' in df.columns):
                icd_col = 'Current_ICD9_List'
            elif('DIAGNOSIS_CODE' in df.columns):
                icd_col = 'DIAGNOSIS_CODE'
            elif ('Diagnosis_Code' in df.columns):
                icd_col = 'Diagnosis_Code'
            else:
                icd_col = ''
                print "NO ICD-9 code column found"
                isICD9CodePresent = "No"

            if(isICD9CodePresent == "Yes"):
                showcols.append(icd_col)

            if ('ICD10_CODE' in df.columns):
                icd10_col = 'ICD10_CODE'
            elif ('ICD10_Code' in df.columns):
                icd10_col = 'ICD10_Code'
            elif ('Admitting_IC10_Code' in df.columns):
                icd10_col = 'Admitting_IC10_Code'
            elif ('icd10_list' in df.columns):
                icd10_col = 'icd10_list'
            elif ('Current_ICD10_List' in df.columns):
                icd10_col = 'Current_ICD10_List'
            else:
                icd10_col = ''
                print "NO ICD-10 code column found"
                isICD10CodePresent = "No"

            if(isICD10CodePresent == "Yes"):
                showcols.append(icd10_col)

            outputJson[customer][f]["total"] = len(df)
            total = len(df)
            if(str(isICD9CodePresent) == "No" and str(isICD10CodePresent) == "No"):
                writer.writerow([customer, f, total, 'No', 'No','','',''])
                continue


            if ('PAT_ID' in df.columns):
                pat_id = 'PAT_ID'
            elif ('Patient_ID' in df.columns):
                pat_id = 'Patient_ID'
            elif ('PATIENT_ID' in df.columns):
                pat_id = 'PATIENT_ID'
            elif ('PatientID' in df.columns):
                pat_id = 'PatientID'

            showcols.append(pat_id)

            df = df[showcols]
            if(len(masterPatList)>0):
                dfPatient = df[[pat_id]]
                dfPatient.set_index(pat_id, inplace=True)
                #compare the dflist with the master and remove all duplicates
                duplicates = set(masterPatList.index).intersection(dfPatient.index)
                df = df[~df[pat_id].isin(duplicates)]
                dfPatient = dfPatient.drop(duplicates, axis=0)
                masterPatList = masterPatList.merge(dfPatient, how='outer', left_index=True, right_index=True)
            else:
                masterPatList = df[[pat_id]]
                masterPatList.set_index(pat_id, inplace=True)

            if(isICD9CodePresent == "Yes"):
                dfWithICD9Code = df[df[icd_col].notnull()]

                if (any(dfWithICD9Code[pat_id].duplicated()) == True):
                    removeDupsdfWithICD9Code = dfWithICD9Code.drop_duplicates(pat_id)
                else:
                    removeDupsdfWithICD9Code = dfWithICD9Code
                outputJson[customer][f]["ICD9Count"] = len(removeDupsdfWithICD9Code)
                total_removeDupsdfWithICD9Code = len(removeDupsdfWithICD9Code)
            else:
                outputJson[customer][f]["ICD9Count"] =''
                total_removeDupsdfWithICD9Code = 0

            #Patients with ICD-10 Code

            if (isICD10CodePresent == "Yes"):
                dfWithICD10Code = df[df[icd10_col].notnull()]
                if (any(dfWithICD10Code[pat_id].duplicated()) == True):
                    removeDupsdfWithICD10Code = dfWithICD10Code.drop_duplicates(pat_id)
                else:
                    removeDupsdfWithICD10Code = dfWithICD10Code
                outputJson[customer][f]["ICD10Count"] = len(removeDupsdfWithICD10Code)
                total_removeDupsdfWithICD10Code = len(removeDupsdfWithICD10Code)
            else:
                outputJson[customer][f]["ICD10Count"] = ''
                total_removeDupsdfWithICD10Code = 0


            #Patients with both ICD-9 and ICD-10 code

            if (isICD9CodePresent == "Yes"  and isICD10CodePresent == "Yes"):
                dfWithBoth = df[df[icd10_col].notnull() & df[icd_col].notnull()]

                if (any(dfWithBoth[pat_id].duplicated()) == True):
                    removeDupsdfWithBoth = dfWithBoth.drop_duplicates(pat_id)
                else:
                    removeDupsdfWithBoth = dfWithBoth
                outputJson[customer][f]["Both"] = len(removeDupsdfWithBoth)
                total_removeDupsdfWithBoth = len(removeDupsdfWithBoth)
            else:
                outputJson[customer][f]["Both"] = ''
                total_removeDupsdfWithBoth = 0


            # Patients with any diagnosis code

            if (isICD9CodePresent == "Yes" and isICD10CodePresent == "Yes"):

                dfWithAnyDiagnosisCode = df[df[icd10_col].notnull() | df[icd_col].notnull()]
                if (any(dfWithAnyDiagnosisCode[pat_id].duplicated()) == True):
                    removeDupsdfWithAny = dfWithAnyDiagnosisCode.drop_duplicates(pat_id)
                else:
                    removeDupsdfWithAny = dfWithAnyDiagnosisCode
                outputJson[customer][f]["Any"] = len(removeDupsdfWithAny)
                total_removeDupsdfWithAny = len(removeDupsdfWithAny)
            else:
                outputJson[customer][f]["Any"] = ''
                total_removeDupsdfWithAny = 0

            writer.writerow([customer, f, total,isICD9CodePresent,isICD10CodePresent,total_removeDupsdfWithICD9Code,total_removeDupsdfWithICD10Code,total_removeDupsdfWithBoth,total_removeDupsdfWithAny])
    print(outputJson)

