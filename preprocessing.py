import numpy as np
import pandas as pd

print("read dataset rows")
ds=pd.read_excel("/home/abdosh/Desktop/GP_FIRST_TASK_MACHINE_LEARNING/dataset.xlsx",na_filter=False)

ds=ds.to_numpy()

#output dataset decleration
ds_h = []
	
print("remove all cells that contains names leaving only users id")
for pr in ds :
	if not pr[0][0].isalpha() :
		tmp=[pr[0].split('_')[2],pr[1],pr[2]]
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


ds_mfu = 'dataset_output_mfu.xlsx'
ds_nor = 'dataset_output_nor.xlsx'

def process_on_users_events(u_e_s):
	us_ev_sort_h=[]
	for ev in u_e_s :
		u_dic={}
		u_dic['user_id']=ev[0]
		date_arr=ev[1].split(' ')[0].split('-')
		u_dic['date']=ev[1].split(' ')[0]
		u_dic['year']=date_arr[0]
		u_dic['mon']=date_arr[1]
		u_dic['day']=date_arr[2]
		time_arr=ev[1].split(' ')[1].split(':')
		u_dic['hour']=time_arr[0]
		u_dic['min']=time_arr[1]
		u_dic['sec']=time_arr[2].split('.')[0]
		u_dic['lat']=ev[2][2:-2].split(', ')[0]
		u_dic['lon']=ev[2][2:-2].split(', ')[1]
		us_ev_sort_h.append(u_dic)
	us_ev_sort_h.sort(key=lambda x:x['date'])
	return us_ev_sort_h	

ds_x_mfu = pd.read_excel(ds_mfu)
ds_x_nor = pd.read_excel(ds_nor)
ds_c_mfu = ds_x_mfu
ds_c_nor = ds_x_nor


def append_users_events_to_ex_ds(evs,ds):
	for ev in evs :
		ds = pd.concat([ds, pd.DataFrame([ev])], ignore_index=True)
	
	return ds


print('formating user events and sorting it depends on the date')
for i in range(0,len(dataset_output)):
	print("Work on user no : "+str(i)+" of " +str(len(dataset_output)) + " Having Id = " + str(dataset_output[i][0][0]))
	u_events=dataset_output[i]
	u_events=process_on_users_events(u_events)
	if len(u_events) > 30 :	
		ds_c_mfu = append_users_events_to_ex_ds(u_events,ds_c_mfu)
	else :
		ds_c_nor = append_users_events_to_ex_ds(u_events,ds_c_nor)
	
		
ds_c_mfu.to_excel(ds_mfu, index=False)
ds_c_nor.to_excel(ds_nor, index=False)


