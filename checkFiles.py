import os
import re
import pandas
import csv

customers = ["Aurora","Banner","DH","CHI","Miami","Providence"]
dirPointer = {"Aurora": "Folder1/","DH":"Folder2/","CHI":"FOLDER3/"}
header = ['Customer','FileName','Total','IsICD9CodePresent?','IsICD10CodePresent?','Patients Count with ICD-9','Patients count with ICd-10','Patients count with Both','Patients With Any']

outputJson = {}
with open('output1.csv', 'a+') as output:
    writer = csv.writer(output, delimiter=',')
    writer.writerow(header)
    for customer, dir in dirPointer.items():
        files = []
        outputJson[customer] = {}
        for filename in os.listdir(dir):
            if re.search(r'(Patient|Diagnosis|Encounter|Medorder|Medadmin|Testorder|ClinicalTestResult|CancerAassessment).*\.csv$',filename,re.IGNORECASE):
             files.append(filename)
             print(files)
             for f in files:
                 filePath = dir + f
                 df = pandas.read_csv(filePath)
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
                 else:
                     print "NO ICD-9 code column found"
                     isICD9CodePresent = "No"

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
                     print "NO ICD-10 code column found"
                     isICD10CodePresent = "No"

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


                 dfWithICD9Code = df.dropna(axis=0, subset=[icd_col])

                 if (any(dfWithICD9Code[pat_id].duplicated()) == True):
                     removeDupsdfWithICD9Code = dfWithICD9Code.drop_duplicates(pat_id)
                 else:
                     removeDupsdfWithICD9Code = dfWithICD9Code
                 outputJson[customer][f]["ICD9Count"] = len(dfWithICD9Code)

                 #Patients with ICD-10 Code

                 dfWithICD10Code = df[df[icd10_col].notnull()]
                 if (any(dfWithICD10Code[pat_id].duplicated()) == True):
                     removeDupsdfWithICD10Code = dfWithICD10Code.drop_duplicates(pat_id)
                 else:
                     removeDupsdfWithICD10Code = dfWithICD10Code
                 outputJson[customer][f]["ICD10Count"] = len(removeDupsdfWithICD10Code)


                 #Patients with both ICD-9 and ICD-10 code

                 dfWithBoth = df[df[icd10_col].notnull() & df[icd_col].notnull()]

                 if (any(dfWithBoth[pat_id].duplicated()) == True):
                     removeDupsdfWithBoth = dfWithBoth.drop_duplicates(pat_id)
                 else:
                     removeDupsdfWithBoth = dfWithBoth
                 outputJson[customer][f]["Both"] = len(removeDupsdfWithBoth)

                 # Patients with any diagnosis code

                 dfWithAnyDiagnosisCode = df[df[icd10_col].notnull() | df[icd_col].notnull()]
                 if (any(dfWithAnyDiagnosisCode[pat_id].duplicated()) == True):
                    removeDupsdfWithAny = dfWithAnyDiagnosisCode.drop_duplicates(pat_id)
                 else:
                     removeDupsdfWithAny = dfWithAnyDiagnosisCode
                 outputJson[customer][f]["Any"] = len(removeDupsdfWithAny)

                 writer.writerow([customer, f, total,isICD9CodePresent,isICD10CodePresent,len(removeDupsdfWithICD9Code),len(removeDupsdfWithICD10Code),len(removeDupsdfWithBoth),len(removeDupsdfWithAny)])
                 print(outputJson)

