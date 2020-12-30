import traceback
a = [1,2,'3',4,5,'6']
for i in a:
    try :
        ss =  i - 4
        print(ss)
    except:
        traceback.print_exc()
        print('error')



