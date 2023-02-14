clear; clc; close all; format longG;% delete(timerfind);

%%
% https://www.thespreadsheetguru.com/blog/sample-data
% Company Employee Example Data

data_ = readtable('Employee Sample Data.csv');

% add employee ind to org data_
data_.ind = (1:size(data_, 1))';

% 
data_to_db_1 = table2struct(data_(1:10, :));
data_to_db_2 = table2struct(data_(5:15, :));

% 
collectname = 'employee';

%% mongo settings

mongo_setting.host_address = "127.0.0.1";
mongo_setting.port = "27017";
mongo_setting.dbname = "matlab_mongo";
mongo_setting.user_name = "";% optional
mongo_setting.password = "";% optional


%% create mongo object

% call mongo object and connect to db
db_ = MongoDB(mongo_setting);

% print matlab_mongo database collections
if isempty(db_.db_conn.CollectionNames)
    disp('this database is empty')
else
    disp(db_.db_conn.CollectionNames)
end


%% create clolection

% create collection if not exist
db_.create_col(collectname, false);

% print matlab_mongo database collections
disp(db_.db_conn.CollectionNames)


%% insert data_to_db_1 to db

% insert to db: matlab_mongo and collection: employee
db_.insert_to_col(collectname, data_to_db_1);


%% insert data_to_db_2 to db

% delete duplicate document
d_filter(1).field = 'ind';
d_filter(1).val_list = [data_to_db_2.ind];

% 
db_.del_from_col(collectname, d_filter);

% 
db_.insert_to_col(collectname, data_to_db_2);

%% drop collection

collectname_test = 'test';

% create test collection
db_.create_col(collectname_test, false);

% print matlab_mongo database collections
disp(db_.db_conn.CollectionNames)

% drop test collection
db_.drop_collection(collectname_test);

% print matlab_mongo database collections
disp(db_.db_conn.CollectionNames)


%% close mongodb connection

db_.close_db();

%% get from db

% call mongo object and connect to db
db_ = MongoDB(mongo_setting);

full_data = db_.get_from_col(collectname);

% remove mongo column _id, update_time from data
full_data = rmfield(full_data, "_id");
full_data = rmfield(full_data, "update_time");

% convert struct to table
full_data_table = struct2table(full_data);









