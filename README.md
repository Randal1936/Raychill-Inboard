# Raychill-Inboard
#### Attention:Raychill was launched on 2021.1.9, lasted for a year and it is abandoned now.
That's a bookkeeping program designed for my personal fund, Raychill Capital.
(A personal fund is, in a word, a sum of money raised among friends and classmates.)

This simple project mainly contains two parts: one for data storage in SQL, the other one for daily update and bookkeeping.
The SQL part is still in progress because I am trying to find a way that is faster than the "exercise" command in pymysql.
As for updating, credit to the tushare, an awesome finance tool. It's so efficient yet easy to use.

### data_init.py
That's the SQL part. All done, but a lot to improve.

### management.py
That is the main body of bookkeeping procedures. Through this you can see how I design my automatic accounting records, which may be helpful for you to make your own. Honestly speaking ,it is not completely automatic, you still have to put down every transaction you have made and update them manually. I will consider to automate that too, but it will take some time.

### main.py
When main.py works, the whole project works. You know what it means.



The following three scripts are supplements to the management.py. It seems much more convenient to get them separated from other codes.
### Fill_err.py
### Fill_distri.py
### Par.py
