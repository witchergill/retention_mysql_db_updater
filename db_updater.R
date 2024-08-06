#loading required packages
require(RMySQL)
require(tidyverse)
require(googlesheets4)
require(googledrive)
require(openxlsx2)

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


updation.date<-Sys.Date()-1
#checking the files if uploaded into the drive
while(T){
  
  if(updation.date < Sys.Date()){
    client.file.update=T
    client.khlr.file.update=T
    file.file.update=T
    updation.date=Sys.Date()
  }       #creating flag for updation dates and individual file updation flags
  
  #checking if files are uploaded or not
  drive.files<-drive_ls('daily_data')$name
  client.khlr.file<-str_c("clients_khlr_",str_sub(Sys.Date(),-2,-1),"_",str_sub(tolower(months(Sys.Date())),1,3),".xlsx")
  client.file<-str_c("clients_",str_sub(Sys.Date(),-2,-1),"_",str_sub(tolower(months(Sys.Date())),1,3),".xlsx")
  file.file<-str_c("File",Sys.Date(),".csv")
  
  #creating 3 different if conditions for all three different files 
  #condition for client.khlr file
  if(((client.khlr.file %in% drive.files) & client.khlr.file.update)){
    #downloading the file
    drive_download(client.khlr.file)
    #all the process for client.khlr.file will begin
    #pick the file with current date on it
    data.net<- read_xlsx(client.khlr.file)
    #creating backup dataset
    test.data.net<-data.net
    #replacing + and whitespace with ""
    data.net$`Phone No`<-str_replace_all(data.net$`Phone No`,"\\+","")
    data.net$`Phone No`<-str_replace_all(data.net$`Phone No`," ","")
    #data$`Phone No`<-str_extract(data$`Phone No`,"\\d+")
    
    #removing NA values from data (filtering phone numbers)
    
    #data<-data[!is.na(data$`Phone No`),]
    
    #filtering data with only having organic data i.e no campaigns is listed to it
    #data<-data[is.na(data$Campaign),]
    
    #filtering data with having only 0 ftd
    data.net<-data.net[data.net$`First Deposit`!=0,]
    
    #now updating the net db
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
    
    
    #now converting the updation to false from true
    client.khlr.file.update=F
    
  }
  
  if(((client.file %in% drive.files) & client.file.update)){
    #downloading the file
    drive_download(client.file)
    
    data.games<- read_xlsx(client.file)
    
    #creating backup dataset
    test.data<-data.games
    
    #replacing + and whitespace with ""
    
    data.games$`Phone No`<-str_replace_all(data.games$`Phone No`,"\\+","")
    data.games$`Phone No`<-str_replace_all(data.games$`Phone No`," ","")
    #data$`Phone No`<-str_extract(data$`Phone No`,"\\d+")
    
    #removing NA values from data (filtering phone numbers)
    
    #data<-data[!is.na(data$`Phone No`),]
    
    #filtering data with only having organic data i.e no campaigns is listed to it
    #data<-data[is.na(data$Campaign),]
    
    #filtering data with having only 0 ftd
    data.games<-data.games[data.games$`First Deposit`!=0,]
    
    
    #creating another backup for filtered dataset
    test.data2<-data.games
    
    #updating  games db
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
    
    client.file.update=F
    
  }
  
  if(((client.file %in% drive.files) & file.file.update)) {
    #downloading the file
    drive_download(file.file,path = 'daily_data')
    
    #creating filtered data for club sheet
    data.biz<- read_csv(file.file,skip = 1)
    
    #replacing the data franchise master
    data.biz$Master<-NA
    
    #filtering data with having non  0 ftd
    
    data.biz<-data.biz[data.biz$`FirstDeposit`!=0,]
    
    data.biz$Master<-str_split_fixed(data.biz$ClientName,"\\(",2)[,2]
    data.biz$ClientName<-str_split_fixed(data.biz$ClientName," \\(",2)[,1]
    #for(i in 1:nrow(data.biz)){
    
    #data.biz$Master[i]<-str_split_1(str_split_fixed(str_replace_all(data.biz$ClientName[i]," ","")
    #,pattern = "\\(",2)[2],pattern = "\\)")[1]
    #data.biz$ClientName[i]<-str_split_fixed(str_replace_all(data.biz$ClientName[i]," ","") # if client name is needed
    #,pattern = "\\(",2)[1]
    #}
    zz<-unique(data.biz$Master)
    zz<-zz[grep("^MW|^Win",zz)]
    data.biz<-data.biz[(data.biz$Master%in%zz),]
    data.biz<-data.biz[,c(1,23,2:22)]
    
    #removing the ) bracket from master column
    data.biz$Master<-str_remove_all(data.biz$Master,"\\)")
    #chaning the date_time format from character to time
    
    #converting time string into date format of REGISTRATIONDATE
    
    ########################################################################################
    
    ########################################################################################
    
    #replacing + and whitespace with ""
    data.biz$`PhoneNo`<-str_replace_all(data.biz$`PhoneNo`," ","")
    data.biz$`PhoneNo`<-str_replace_all(data.biz$`PhoneNo`,"\\+","")
    
    
    test.data.biz2<-data.biz
    
    #changing the registration date into date time format
    for(i in 1:nrow(data.biz)){
      data.biz$RegistrationDate[i]<-timeconverter(data.biz$RegistrationDate[i])
    }
    
    #changing the first depositon date
    for(i in 1:nrow(data.biz)){
      data.biz$FirstDepositOn[i]<-timeconverter(data.biz$FirstDepositOn[i])
    }
    
    #changing the last deposit on date
    for(i in 1:nrow(data.biz)){
      if(!is.na(data.biz$LastDepositOn[i])){
        data.biz$LastDepositOn[i]<-timeconverter(data.biz$LastDepositOn[i])
      }
    }
    
    #converting the format of all the dates from character to date
    data.biz$FirstDepositOn<-ymd_hms(data.biz$FirstDepositOn)
    data.biz$RegistrationDate<-ymd_hms(data.biz$RegistrationDate)
    data.biz$LastDepositOn<-ymd_hms(data.biz$LastDepositOn)
    
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
    
    
    #cleaning process for club
    #creating filtered data for club sheet
    data.club<- read_csv(file.file,skip = 1)
    
    #replacing the data franchise master
    data.club$Master<-NA
    
    #filtering data with having non  0 ftd
    
    data.club<-data.club[data.club$`FirstDeposit`!=0,]
    
    data.club$Master<-str_split_fixed(data.club$ClientName,"\\(",2)[,2]
    data.club$ClientName<-str_split_fixed(data.club$ClientName," \\(",2)[,1]
    #for(i in 1:nrow(data.club)){
    
    #data.club$Master[i]<-str_split_1(str_split_fixed(str_replace_all(data.club$ClientName[i]," ","")
    #,pattern = "\\(",2)[2],pattern = "\\)")[1]
    #data.club$ClientName[i]<-str_split_fixed(str_replace_all(data.club$ClientName[i]," ","") # if client name is needed
    #,pattern = "\\(",2)[1]
    #}
    zz<-unique(data.club$Master)
    zz<-zz[grep("^MW|^Win",zz)]
    data.club<-data.club[!(data.club$Master%in%zz),]
    data.club<-data.club[,c(1,23,2:22)]
    
    #removing the ) bracket from master column
    data.club$Master<-str_remove_all(data.club$Master,"\\)")
    #chaning the date_time format from character to time
    
    #converting time string into date format of REGISTRATIONDATE
    
    ########################################################################################
    
    ########################################################################################
    
    #replacing + and whitespace with ""
    data.club$`PhoneNo`<-str_replace_all(data.club$`PhoneNo`," ","")
    data.club$`PhoneNo`<-str_replace_all(data.club$`PhoneNo`,"\\+","")
    
    
    test.data.club2<-data.club
    
    #changing the registration date into date time format
    for(i in 1:nrow(data.club)){
      data.club$RegistrationDate[i]<-timeconverter(data.club$RegistrationDate[i])
    }
    
    #changing the first depositon date
    for(i in 1:nrow(data.club)){
      data.club$FirstDepositOn[i]<-timeconverter(data.club$FirstDepositOn[i])
    }
    
    #changing the last deposit on date
    for(i in 1:nrow(data.club)){
      if(!is.na(data.club$LastDepositOn[i])){
        data.club$LastDepositOn[i]<-timeconverter(data.club$LastDepositOn[i])
      }
    }
    
    #converting the format of all the dates from character to date
    data.club$FirstDepositOn<-ymd_hms(data.club$FirstDepositOn)
    data.club$RegistrationDate<-ymd_hms(data.club$RegistrationDate)
    data.club$LastDepositOn<-ymd_hms(data.club$LastDepositOn)
    
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
    
    
    file.file.update=F
  }
  
  Sys.sleep(20*60)
  
  }
