import numpy as np
import pandas as pd
import datetime

print("read dataset rows")
ds=pd.read_excel("/home/abdosh/Desktop/GP_FIRST_TASK_MACHINE_LEARNING/dataset.xlsx",na_filter=False)

ds=ds.to_numpy()

#ds=ds.sort_values(by="person_name")



#output dataset decleration
ds_h = []

print("remove all cells that contains names leaving only users id")
for pr in ds :
	if not pr[0][0].isalpha() :
		tmp=[pr[0],pr[1],pr[2]]
		ds_h.append(tmp)
	
	
print("sorting dataset array depends on user's id")
def sort_key(e):
	return e[0]
	
ds_h.sort(key=sort_key)


print("Collect all user events into one array")
i=-1
user_id_temp=0

dataset_output=[]

for row in ds_h :
	if row[0] == user_id_temp :
		dataset_output[i].append(row)
	else:
		user_id_temp=row[0]
		dataset_output.append([row])
		i=i+1

print("sorting users array depends on user's event_ts")

def sort_key_users(e):
	return e[1]

for i in range(0,10):

	print(dataset_output[i])

		
print(len(dataset_output))

	
	












#with pd.ExcelWriter("out_dataset.xlsx") as wr :
#	ds.to_excel(wr,"test_1000_sort",columns=['person_name'])




