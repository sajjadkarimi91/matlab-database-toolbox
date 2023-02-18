clear; clc; close all; format longG;% delete(timerfind);

%% An example for working with Open-High-Low-Close prices plus Volume

% load ohlcv from mat-file
load('./sample-data/ohlcv.mat')

ohlcv = ohlcv(1:10, 1000:end, :);

% create data structure for insert to mongodb
[N, T, ~] = size(ohlcv);
data_to_db(N*T).ind_stock = [];
for n = 1:N
    for t = 1:T
        data_to_db((n-1)*T+t).ind_stock = n;
        data_to_db((n-1)*T+t).ind_date = t;

        data_to_db((n-1)*T+t).open = ohlcv(n, t, 1);
        data_to_db((n-1)*T+t).high = ohlcv(n, t, 2);
        data_to_db((n-1)*T+t).low = ohlcv(n, t, 3);
        data_to_db((n-1)*T+t).close = ohlcv(n, t, 4);
        data_to_db((n-1)*T+t).volume = ohlcv(n, t, 5);
    end
end




%% mongo settings

mongo_setting.host_address = "127.0.0.1";
mongo_setting.port = "27017";
mongo_setting.dbname = "matlab_mongo";
mongo_setting.user_name = "";% optional
mongo_setting.password = "";% optional
% 
collectname = 'ohlcv';

%% create collection delete old documents & insert new documents in fast parallel 

% call mongo object and connect to db
db_ = MongoDB(mongo_setting);


% create collection if not exist
db_.create_col(collectname, false);

% print matlab_mongo database collections
disp('list of collections in MongoDB')
disp(db_.db_conn.CollectionNames)

% delete old duplicate documents
d_filter(1).field = 'ind_stock';
d_filter(1).val_list = 1:N;
d_filter(2).field = 'ind_date';
d_filter(2).val_list = 1:T;
db_.del_from_col(collectname, d_filter); % first remove old same documents to avoid dublication problem

%Don't forget to close the collection :)
db_.close_db();

% insert to db: matlab_mongo and collection: employee
% db_.insert_to_col(collectname, data_to_db);

disp('use batching and parfor for fast inserting')
% use batching and parfor for fast inserting
num_batch = 8;
batch_size = ceil(length(data_to_db)/num_batch);
stop = length(data_to_db);

% for large documents above 10^5 parallel insert have a speed gain
parfor b= 1:num_batch
    strt = (b-1)*batch_size + 1;
    stp = b*batch_size;
    stp = min(stp, stop);

    db_ = MongoDB(mongo_setting);
    db_.insert_to_col(collectname, data_to_db(strt:stp));
    db_.close_db(); %Don't forget to close the collection :)
end


%% get, nan, vector, get time

% create filter
d_filter(1).field = 'ind_stock';
d_filter(1).val_list = 1:N;
d_filter(2).field = 'ind_date';
d_filter(2).val_list = 1:T;

% get from db
db_ = MongoDB(mongo_setting);
d_data = db_.get_from_col(collectname, d_filter);
db_.close_db();

% convert struct(table) to 3D ohlcv (N*T*5)
ohlcv_get = nan(N ,T , 5);
for row_ = 1:length(d_data)
    n = d_data(row_).ind_stock;
    t = d_data(row_).ind_date;

    ohlcv_get(n, t, 1) =  double(d_data(row_).open);
    ohlcv_get(n, t, 2) =  double(d_data(row_).high);
    ohlcv_get(n, t, 3) =  double(d_data(row_).low);
    ohlcv_get(n, t, 4) =  double(d_data(row_).close);
    ohlcv_get(n, t, 5) =  double(d_data(row_).volume);
end

% verify
isequaln(ohlcv_get, ohlcv)


%% get data for your selected feild 

disp('get data for your selected feild')

selected_fields = {'ind_stock', 'ind_date', 'high', 'low'};

% get from db
db_ = MongoDB(mongo_setting);
d_data = db_.get_from_col(collectname, d_filter, selected_fields);
db_.close_db();

% convert struct(table) to 3D ohlcv (N*T*5)
ohlcv_get = nan(N ,T , 2);
for row_ = 1:length(d_data)
    n = d_data(row_).ind_stock;
    t = d_data(row_).ind_date;

    ohlcv_get(n, t, 1) =  double(d_data(row_).high);
    ohlcv_get(n, t, 1) =  double(d_data(row_).low);
end







