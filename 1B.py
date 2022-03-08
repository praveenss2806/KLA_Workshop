import yaml
import time
import threading
from datetime import datetime

output_file= open('Milestone1B_Log.txt','w')

output_file.write(f"{datetime.now()};M1B_Workflow Entry \n")

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
            elif activities[activity]['Execution'] == 'Concurrent':
                Concurrent(activities[activity],path+'.'+activity)
            output_file.write(f"{datetime.now()};{path}.{activity} Exit \n")

#This function will execute if Excecution in Concurrent
def Concurrent(workflow,path):
    activities = workflow['Activities']
    
    thread= []
    
    for task in activities:
        t = threading.Thread(target=Concurrent_Task,args=(activities,task,path))
        t.start()
        thread.append(t)
    
    for t in thread:
        t.join()

#This function will execute each thread in Concurrent Function   
def Concurrent_Task(activities,task,path):
    output_file.write(f"{datetime.now()};{path}.{task} Entry \n")
    if(activities[task]['Type'] == 'Task'):
            fun = eval(activities[task]['Function'])
            fun(activities[task]['Inputs'],path+"."+task)
    elif activities[task]['Type'] == 'Flow':
        if activities[task]['Execution'] == 'Sequential':
                Sequential(activities[task],path+"."+task)
        elif activities[task]['Execution'] == 'Concurrent':
                Concurrent(activities[task],path+"."+task)
        output_file.write(f"{datetime.now()};{path}.{task} Exit \n")
    

def TimeFunction(inputs,path):
    output_file.write(f"{datetime.now()};{path} Executing TimeFunction ({inputs['FunctionInput']},{inputs['ExecutionTime']}) \n")
    time.sleep(int(inputs['ExecutionTime']))
    output_file.write(f"{datetime.now()};{path} Exit \n")
    

    
#To parse the YAML dataflie
with open('Milestone1\Milestone1B.yaml','r') as file:
    data=yaml.safe_load_all(file)
    mainflow=next(data)
    
workflow=mainflow['M1B_Workflow']

#To check wheter execution in sequential
if workflow['Execution']=='Sequential':
    Sequential(workflow,'M1B_Workflow')
    output_file.write(f'{datetime.now()};M1B_Workflow Exit')
    
if workflow['Execution']=='Concurrent':
    Concurrent(workflow,'M1B_Workflow')
    output_file.write(f'{datetime.now()};M1B_Workflow Exit')
    
print("done")