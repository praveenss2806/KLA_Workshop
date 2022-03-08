import yaml
import time
from datetime import datetime

output_file= open('Milestone1A_Log.txt','w')

output_file.write(f"{datetime.now()};M1A_Workflow Entry \n")

#This function will execute if Excecution in sequential
def Sequential(workflow,path):
    activities = workflow['Activities']
    
    for activity in activities:
        output_file.write(f'{datetime.now()};{path}.{activity} Entry \n')
        
        if activities[activity]['Type']=='Task':
            #Function call
            func = eval(activities[activity]['Function'])
            #Function arguments
            func(activities[activity]['Inputs'],path+'.'+activity)
            
        elif activities[activity]['Type']=='Flow':
            #Recursive function call
            if activities[activity]['Execution'] == 'Sequential':
                Sequential(activities[activity],path+'.'+activity)
                
        output_file.write(f'{datetime.now()};{path}.{activity} Exit \n')

def TimeFunction(inputs,path):
    output_file.write(f"{datetime.now()};{path} Executing TimeFunction ({inputs['FunctionInput']},{inputs['ExecutionTime']}) \n")
    time.sleep(int(inputs['ExecutionTime']))

    
#To parse the YAML dataflie
with open('Milestone1\Milestone1A.yaml','r') as file:
    data=yaml.safe_load_all(file)
    mainflow=next(data)
    
workflow=mainflow['M1A_Workflow']

#To check wheter execution in sequential
if workflow['Execution']=='Sequential':
    Sequential(workflow,'M1A_Workflow')
    output_file.write(f'{datetime.now()};M1A_Workflow Exit')
    
print("done")