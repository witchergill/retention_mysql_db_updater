#loading required packages
require(RMySQL)
require(tidyverse)
require(googlesheets4)
require(googledrive)

googledrive::drive_auth(email = 'ap.kheloyaar@gmail.com',cache  = '.secretsap')
googlesheets4::gs4_auth(token = googledrive::drive_token())

#reordering the colnames according to the universal headers
db.headers<-"S_No,Master,Client_Name,User_ID,Phone_No,Account_Status,AffiliateId,Affiliate,Campaign,Available_Balance,First_Deposit,First_DepositOn,Last_Deposit,Last_DepositOn,Registered_Date,Total_Deposits,Total_Withdrawal,Number_Of_Times_Deposited,Number_Of_Times_Withdraw"
#converting all the data into sql string
#using custom function
create_values <- function(row) {
  
  paste0("('", paste(row, collapse = "', '"), "')")
  
}
#loading all db login and passwords credentials
#games.db  credentials
user.games='u483816504_retention'
password.games='3txMtAZ?vJ'
database.name.games='u483816504_retention'

#bizz.db credentials
user.bizz='u483816504_retentionbiz'
password.bizz='6x=]4n]nkJ'
database.name.bizz='u483816504_retentionbiz'

#net.db credentials
user.net='u483816504_retentionnet'
password.net='dMc*jm5*8'
database.name.net='u483816504_retentionnet'

#club.db credentials
user.club='u483816504_retentionclub'
password.club='m|@ldYJ5#rW'
database.name.club='u483816504_retentionclub'

#pkr.db credentials
user.pkr='u483816504_retainpkr'
password.pkr='aNBugcT*w7*'
database.name.pkr='u483816504_retainpkr'



#checking the files if uploaded into the drive
while(T){
  
  drive.files<-drive_ls('daily_data')
  
  clients.file<-str_c("clients_",str_sub(Sys.Date(),-2,-1),"_",str_sub(tolower(months(Sys.Date())),1,3),".xlsx")
  clients.khlr.file<-str_c("clients_khlr_",str_sub(Sys.Date(),-2,-1),"_",str_sub(tolower(months(Sys.Date())),1,3),".xlsx")
  comb.file<-str_c("File",Sys.Date(),".csv")
  #LOADING REQUIRED FILE
  
  #data_cleaning in data.net data.frame
  
  
  data.net$`User ID`[grep("\\'",data.net$`User ID`)]<-str_replace_all(data.net$`User ID`[grep("\\'",data.net$`User ID`)],pattern = "\\'",replacement = "_") 
  data.net$`Client Name`[grep("\\'",data.net$`Client Name`)]<-str_replace_all(data.net$`Client Name`[grep("\\'",data.net$`Client Name`)],pattern = "\\'",replacement = "_")
  

  
  
  #updating the .net db 
  
  # Apply the function to each row of the data frame
  values <- apply(data.net, 1, create_values)
  # Combine all values into a single string
  values_string <- paste(values, collapse = ", ")
  values_string<-str_replace_all(values_string,"'NA'","NULL")
  
  #removing the table data from .net 
  assign('con',value =dbConnect(MySQL(),user=user.net,password=password.net,dbname=database.name.net,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- "delete from client_data;"
  dbSendQuery(conn = con,q)
  dbDisconnect(con)
  
  
  #updating the db with the new updated data
  assign('con',value =dbConnect(MySQL(),user=user.net,password=password.net,dbname=database.name.net,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- paste0("INSERT INTO client_data (",db.headers, ") VALUES " , values_string)
  #fetch(dbSendQuery(con,q))
  dbSendQuery(con,q)
  dbDisconnect(con)
  
  
  
  #for games
  data.games$`User ID`[grep("\\'",data.games$`User ID`)]<-str_replace_all(data.games$`User ID`[grep("\\'",data.games$`User ID`)],pattern = "\\'",replacement = "_")
  data.games$`Client Name`[grep("\\'",data.games$`Client Name`)]<-str_replace_all(data.games$`Client Name`[grep("\\'",data.games$`Client Name`)],pattern = "\\'",replacement = "_")
  
  #updating the .games db 
  
  # Apply the function to each row of the data frame
  values <- apply(data.games[,-20], 1, create_values)
  # Combine all values into a single string
  values_string <- paste(values, collapse = ", ")
  values_string<-str_replace_all(values_string,"'NA'","NULL")
  
  #removing the table data from .games 
  assign('con',value =dbConnect(MySQL(),user=user.games,password=password.games,dbname=database.name.games,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- "delete from client_data;"
  dbSendQuery(conn = con,q)
  dbDisconnect(con)
  
  
  #updating the db with the new updated data
  assign('con',value =dbConnect(MySQL(),user=user.games,password=password.games,dbname=database.name.games,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- paste0("INSERT INTO client_data (",db.headers, ") VALUES " , values_string)
  #fetch(dbSendQuery(con,q))
  dbSendQuery(con,q)
  dbDisconnect(con)
  
  
  
  #for .club
  
  data.club$UserID[grep("\\'",data.club$UserID)]<-str_replace_all(data.club$UserID[grep("\\'",data.club$UserID)],pattern = "\\'",replacement = "_")
  data.club$ClientName[grep("\\'",data.club$ClientName)]<-str_replace_all(data.club$ClientName[grep("\\'",data.club$ClientName)],pattern = "\\'",replacement = "_")
  
  #updating the .club db 
  
  # Apply the function to each row of the data frame
  values <- apply(data.club[,1:19], 1, create_values)
  # Combine all values into a single string
  values_string <- paste(values, collapse = ", ")
  values_string<-str_replace_all(values_string,"'NA'","NULL")
  
  #removing the table data from .club 
  assign('con',value =dbConnect(MySQL(),user=user.club,password=password.club,dbname=database.name.club,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- "delete from client_data;"
  dbSendQuery(conn = con,q)
  dbDisconnect(con)
  
  
  #updating the db with the new updated data
  assign('con',value =dbConnect(MySQL(),user=user.club,password=password.club,dbname=database.name.club,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- paste0("INSERT INTO client_data (",db.headers, ") VALUES " , values_string)
  #fetch(dbSendQuery(con,q))
  dbSendQuery(con,q)
  dbDisconnect(con)
  
  
  #for .biz
  
  data.biz$UserID[grep("\\'",data.biz$UserID)]<-str_replace_all(data.biz$UserID[grep("\\'",data.biz$UserID)],pattern = "\\'",replacement = "_")
  data.biz$ClientName[grep("\\'",data.biz$ClientName)]<-str_replace_all(data.biz$ClientName[grep("\\'",data.biz$ClientName)],pattern = "\\'",replacement = "_")
  
  #updating the .net db 
  
  # Apply the function to each row of the data frame
  values <- apply(data.biz[,1:19], 1, create_values)
  # Combine all values into a single string
  values_string <- paste(values, collapse = ", ")
  values_string<-str_replace_all(values_string,"'NA'","NULL")
  
  #removing the table data from .bizz 
  assign('con',value =dbConnect(MySQL(),user=user.bizz,password=password.bizz,dbname=database.name.bizz,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- "delete from client_data;"
  dbSendQuery(conn = con,q)
  dbDisconnect(con)
  
  
  #updating the db with the new updated data
  assign('con',value =dbConnect(MySQL(),user=user.bizz,password=password.bizz,dbname=database.name.bizz,host="srv1126.hstgr.io",port=3306),envir = .GlobalEnv )
  q <- paste0("INSERT INTO client_data (",db.headers, ") VALUES " , values_string)
  #fetch(dbSendQuery(con,q))
  dbSendQuery(con,q)
  dbDisconnect(con)
  
  #now this is done 
  
  
  
  
  
  
}
