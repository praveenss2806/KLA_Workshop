import yaml
import time
import threading
from datetime import datetime
import pandas as pd

f = open("Milestone2B_Log.txt", "w")
returnDict = {}
lock = threading.Lock()

data =""
with open('Milestone2\MileStone2B.yaml') as file:
    content = yaml.safe_load_all(file)
    data=next(content)
print(datetime.now())
f.write(f"{datetime.now()};M2B_Workflow Entry \n")
dataflow = data['M2B_Workflow']
        
def Sequential(dataflow,path):
    activities = dataflow['Activities']
    #print(dataflow)

    for task in activities:
        #print(task)
        f.write(f"{datetime.now()};{path}.{task} Entry \n")
        print(task," Entry .......")

        if activities[task]['Type'] == 'Task':
            print(activities[task],activities[task].get('Condition'))
            if activities[task].get('Condition') is None:
                condition = ""
            else:
                condition = activities[task]['Condition']
                print(condition)
            fun = eval(activities[task]['Function'])
            if activities[task]['Function'] == "DataLoad" :
                print(path+'.'+task+'.NoOfDefects')
                returnDict[path+'.'+task+'.NoOfDefects'] = fun(activities[task]['Inputs'],path+"."+task,"",condition)[1]
            else:
                fun(activities[task]['Inputs'],path+"."+task,"",condition)
        elif activities[task]['Type'] == 'Flow':
            if activities[task]['Execution'] == 'Sequential':
                Sequential(activities[task],path+"."+task)
            elif activities[task]['Execution'] == 'Concurrent':
                Concurrent(activities[task],path+"."+task)
            f.write(f"{datetime.now()};{path}.{task} Exit \n")
            
def Concurrent(dataflow,path):
    activities = dataflow['Activities']
    
    thread= []
    
    for task in activities:
        #f.write(f"{datetime.now()};{path}.{task} Entry \n")
        t = threading.Thread(target=Concurrent_Task,args=(activities,task,path))
        t.start()
        thread.append(t)
    
    for t in thread:
        t.join()
            
def Concurrent_Task(activities,task,path):
    
    f.write(f"{datetime.now()};{path}.{task} Entry \n")
    print(task+"........")
    if(activities[task]['Type'] == 'Task'):
            if activities[task].get('Condition') is None:
                condition = ""
            else:
                condition = activities[task]['Condition']
                #print(condition)
        
            fun = eval(activities[task]['Function'])
            if activities[task]['Function'] == "DataLoad" :
                returnDict[path+'.'+task+'.NoOfDefects'] = fun(activities[task]['Inputs'],path+"."+task,"",condition)[1]
            else:
                fun(activities[task]['Inputs'],path+"."+task,"",condition)
    elif activities[task]['Type'] == 'Flow':
        if activities[task]['Execution'] == 'Sequential':
                Sequential(activities[task],path+"."+task)
        elif activities[task]['Execution'] == 'Concurrent':
                Concurrent(activities[task],path+"."+task)
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
            
def TimeFunction(inputs,path,task,condition):
    lock.acquire()
    print(condition)
    #cond = ""
    if(len(condition)>0):
        i = 1
        if(condition[len(condition)-2] == '1'):
            i=2
        s=condition[2:len(condition)-4-i]
        print(s)
        print(returnDict[s])
        if(condition[len(condition)-2] == '1'):
            num = eval(condition[len(condition)-2]+condition[len(condition)-1])
        else:
            num = eval(condition[len(condition)-1])
        if(condition[len(condition)-i-2] == '>'):
            
            if returnDict[s]>num:
                print("true")
            else:
                print(path+" skipped")
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                print("false")
                lock.release()
                return
        elif condition[len(condition)-2-i] == '<':
            if(returnDict[s]<num):
                print("true")
            else:
                print(path+" skipped")
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                print("false")
                lock.release()
                return
    f.write(f"{datetime.now()};{path} Executing TimeFunction ({inputs['FunctionInput']},{inputs['ExecutionTime']}) \n")
    print("executing ......")
    time.sleep(int(inputs['ExecutionTime']))
    print("done...")
    if(len(task) == 0):
        f.write(f"{datetime.now()};{path} Exit \n")
    else:
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
    lock.release()
    

def DataLoad(inputs,path,task,condition):
    lock.acquire()
    print(condition)
    #cond=""
    if(len(condition)>0):
        i = 1
        if(condition[len(condition)-2] == '1'):
            i=2
        s=condition[2:len(condition)-4-i]
        print(s)
        print(returnDict[s])
        if(condition[len(condition)-2] == '1'):
            num = eval(condition[len(condition)-2]+condition[len(condition)-1] )
        else:
            num = eval(condition[len(condition)-1])
        if(condition[len(condition)-2-i] == '>'):
            
            if returnDict[s]>num:
                print("true")
            else:
                print(path+" skipped")
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                print("false")
                lock.release()
                return 
        elif condition[len(condition)-2-i] == '<':
            if(returnDict[s]<num):
                print("true")
            else:
                print(path+" skipped")
                f.write(f"{datetime.now()};{path} Skipped \n")
                f.write(f"{datetime.now()};{path} Exit \n")
                print("false")
                lock.release()
                return 

    print(inputs['Filename'])
    f.write(f"{datetime.now()};{path} Executing DataLoad ({inputs['Filename']}) \n")
    df = pd.read_csv(f"Milestone2\{inputs['Filename']}")
    if(len(task) == 0):
        f.write(f"{datetime.now()};{path} Exit \n")
    else:
        f.write(f"{datetime.now()};{path}.{task} Exit \n")
    print("done.....")
    lock.release()
    
    return df,len(df)
        
if dataflow['Execution'] == 'Sequential':
    Sequential(dataflow,"M2B_Workflow")
    f.write(f"{datetime.now()};M2B_Workflow Exit\n")